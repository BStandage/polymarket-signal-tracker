"""Systematic whale selection — who qualifies for the watchlist.

Jump/Citadel-style rigor: a wallet enters the watchlist only if it passes
ALL of the following criteria. No manual selection, no overrides.

Criteria (applied strictly):

    1. Sample adequacy:       confirmed resolved bets >= MIN_RESOLVED
    2. Real capital:          lifetime realized PnL >= MIN_PNL_USDC
    3. Excess return:         overall ROI (realized) >= MIN_ROI
    4. Statistical skill:     z-score on (actual - expected) wins using
                               entry-price implied probabilities >= MIN_Z
                               OR raw win-rate >= 0.80 with n >= 30
    5. Recency activity:      last trade within LOOKBACK_RECENT_DAYS
    6. Not in decay:          rolling 30-day win-rate >= DECAY_THRESHOLD
    7. Bounded drawdown:      max (running) drawdown < 0.5 * cumulative PnL
    8. Account maturity:      account age >= MIN_ACCOUNT_AGE_DAYS

Outputs docs/data/watchlist.json — a list of qualifying wallets with full
skill metrics, sorted by z-score. This is the feed consumed by the live
signal detector and the frontend.
"""

from __future__ import annotations

import datetime as dt
import math
import sys
from dataclasses import dataclass
from typing import Any

from utils import DATA_DIR, log, read_json, write_json, safe_float


# ===========================================================================
# Criteria — tuneable but defensible defaults
# ===========================================================================

@dataclass
class SelectionConfig:
    # 1. Sample
    min_resolved: int = 20
    # Alt path: if n>=30 and win-rate >= 0.80, qualify via overwhelming record
    min_n_for_rate_path: int = 30
    rate_path_win_rate: float = 0.80

    # 2. Capital
    min_pnl_usdc: float = 10_000.0
    # 3. ROI
    min_roi: float = 0.10

    # 4. Statistical skill (primary path)
    min_z_score: float = 1.5       # ~p < 0.07 one-sided

    # 5. Recency
    lookback_recent_days: int = 14
    # 6. Decay detection
    decay_window_days: int = 30
    min_decay_win_rate: float = 0.60
    min_decay_bets: int = 3        # need at least this many recent bets to evaluate

    # 7. Drawdown
    max_drawdown_frac: float = 0.50

    # 8. Age
    min_account_age_days: int = 60

    # Ranking output
    top_n_watchlist: int = 20      # cap the watchlist size


CFG = SelectionConfig()


# ===========================================================================
# Per-wallet metrics
# ===========================================================================

def evaluate_wallet(wallet: dict, closed_outcomes: dict[str, int],
                     now_ts: float) -> dict | None:
    """Compute all selection metrics. Returns full dict (pass or fail),
    caller filters by `passed`.

    Economic + recency metrics use the FULL ledger (any resolved position).
    Statistical skill metrics use the CONFIRMED subset (markets we can
    independently verify the outcome of).
    """
    mkts = wallet.get("ledger_markets") or []
    if not mkts:
        return None

    # === Statistical skill from the confirmed subset ==================
    confirmed = [
        m for m in mkts
        if m.get("resolved") and m.get("market_id") in closed_outcomes
    ]
    expected_wins = 0.0
    variance = 0.0
    actual_wins = 0
    n_confirmed = 0
    for m in confirmed:
        usdc_in = safe_float(m.get("usdc_in"))
        shares = safe_float(m.get("shares_bought"))
        if usdc_in <= 0 or shares <= 0:
            continue
        p = usdc_in / shares
        if not (0 < p < 1):
            continue
        won = m.get("outcome_idx") == closed_outcomes[m["market_id"]]
        expected_wins += p
        variance += p * (1 - p)
        actual_wins += 1 if won else 0
        n_confirmed += 1
    z_score = (actual_wins - expected_wins) / math.sqrt(variance) if variance > 1e-9 else 0.0

    # === Economic + recency from the FULL ledger ======================
    all_resolved = [m for m in mkts if m.get("resolved")]
    pnl = sum(safe_float(m.get("pnl_usdc")) for m in all_resolved)
    capital = sum(safe_float(m.get("usdc_in")) for m in all_resolved)
    roi = pnl / capital if capital > 0 else 0.0
    n_total = len(all_resolved)
    raw_win_rate = (
        sum(1 for m in all_resolved if safe_float(m.get("pnl_usdc")) > 0) / n_total
        if n_total > 0 else 0.0
    )
    total_wins = sum(1 for m in all_resolved if safe_float(m.get("pnl_usdc")) > 0)
    total_losses = sum(1 for m in all_resolved if safe_float(m.get("pnl_usdc")) < 0)

    # Recency: last trade from ANY ledger market (open or resolved)
    all_ts = [
        m.get("last_trade_ts") for m in mkts if m.get("last_trade_ts") is not None
    ] + [
        m.get("first_entry_ts") for m in mkts if m.get("first_entry_ts") is not None
    ]
    last_trade_ts = max(all_ts) if all_ts else None
    days_since_last = ((now_ts - float(last_trade_ts)) / 86400.0) if last_trade_ts else 9999

    # Decay detection: win rate in last 30 days (full resolved set, PnL > 0 = win)
    recent = [
        m for m in all_resolved
        if m.get("last_trade_ts") is not None
        and (now_ts - float(m["last_trade_ts"])) / 86400.0 <= CFG.decay_window_days
    ]
    recent_wins = sum(1 for m in recent if safe_float(m.get("pnl_usdc")) > 0)
    recent_n = len(recent)
    recent_win_rate = (recent_wins / recent_n) if recent_n > 0 else 0.0

    # Drawdown on time-sorted cumulative PnL (all resolved)
    stream = sorted(
        [(float(m["last_trade_ts"]), safe_float(m.get("pnl_usdc")))
         for m in all_resolved if m.get("last_trade_ts") is not None]
    )
    peak = 0.0; max_dd = 0.0; running = 0.0
    for _, p_inc in stream:
        running += p_inc
        peak = max(peak, running)
        dd = peak - running
        if dd > max_dd: max_dd = dd

    # Account age (earliest entry in any market)
    first_entry = min(
        (m.get("first_entry_ts") for m in mkts if m.get("first_entry_ts") is not None),
        default=None,
    )
    account_age_days = ((now_ts - float(first_entry)) / 86400.0) if first_entry else 0.0

    # One-sided p-value on z-score
    from math import erf
    p_value = 1.0 - 0.5 * (1.0 + erf(z_score / math.sqrt(2))) if variance > 1e-9 else 0.5

    # Apply criteria
    # Skill path A: z-score on confirmed set >= threshold (need n_confirmed>=10)
    # Skill path B: raw win-rate on FULL set >= 0.80 with n_total >= 30
    skill_z_ok = (n_confirmed >= 10 and z_score >= CFG.min_z_score)
    skill_rate_ok = (n_total >= CFG.min_n_for_rate_path and raw_win_rate >= CFG.rate_path_win_rate)
    checks = {
        "sample":  n_total >= CFG.min_resolved,
        "capital": pnl >= CFG.min_pnl_usdc,
        "roi":     roi >= CFG.min_roi,
        "skill":   skill_z_ok or skill_rate_ok,
        "recent":  days_since_last <= CFG.lookback_recent_days,
        "no_decay": (recent_n < CFG.min_decay_bets) or (recent_win_rate >= CFG.min_decay_win_rate),
        "drawdown": (pnl <= 0) or (max_dd / pnl <= CFG.max_drawdown_frac),
        "age":     account_age_days >= CFG.min_account_age_days,
    }
    passed = all(checks.values())

    return {
        "address": wallet["address"],
        "passed": passed,
        "failed_checks": [k for k, v in checks.items() if not v],
        "metrics": {
            "n_total": n_total,
            "n_confirmed": n_confirmed,
            "total_wins": total_wins,
            "total_losses": total_losses,
            "raw_win_rate": round(raw_win_rate, 4),
            "confirmed_wins": actual_wins,
            "expected_wins": round(expected_wins, 2),
            "excess_wins": round(actual_wins - expected_wins, 2),
            "z_score": round(z_score, 3),
            "p_value_one_sided": round(p_value, 4),
            "pnl_usdc": round(pnl, 2),
            "capital_usdc": round(capital, 2),
            "roi": round(roi, 4),
            "days_since_last_trade": round(days_since_last, 1),
            "recent_n": recent_n,
            "recent_wins": recent_wins,
            "recent_win_rate": round(recent_win_rate, 4),
            "max_drawdown_usdc": round(max_dd, 2),
            "drawdown_ratio": round(max_dd / pnl, 4) if pnl > 0 else None,
            "account_age_days": round(account_age_days, 1),
        },
        "checks": checks,
    }


# ===========================================================================
# Main
# ===========================================================================

def run_selection() -> dict:
    raw = read_json(DATA_DIR / "whales_raw.json", default={})
    closed_payload = read_json(DATA_DIR / "closed_outcomes.json", default={})
    wallets = raw.get("wallets", []) if isinstance(raw, dict) else []
    closed = closed_payload.get("outcomes", {}) if isinstance(closed_payload, dict) else {}

    if not wallets or not closed:
        log.error("Selector: need whales_raw + closed_outcomes. wallets=%d closed=%d",
                  len(wallets), len(closed))
        return {"error": "missing data"}

    now_ts = dt.datetime.now(dt.timezone.utc).timestamp()
    log.info("Evaluating %d wallets against selection criteria", len(wallets))

    evaluated = []
    for w in wallets:
        row = evaluate_wallet(w, closed, now_ts)
        if row is not None:
            evaluated.append(row)

    passed = [r for r in evaluated if r["passed"]]
    passed.sort(key=lambda r: r["metrics"]["z_score"], reverse=True)
    watchlist = passed[: CFG.top_n_watchlist]

    # Breakdown of why most fail — for tuning visibility
    failure_counts: dict[str, int] = {}
    for r in evaluated:
        for fc in r.get("failed_checks", []):
            failure_counts[fc] = failure_counts.get(fc, 0) + 1

    log.info("Selection: %d / %d wallets passed ALL criteria",
             len(passed), len(evaluated))
    log.info("Failure reasons (top 5): %s",
             sorted(failure_counts.items(), key=lambda x: -x[1])[:5])

    output = {
        "updated_at": dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
        "config": {
            "min_resolved": CFG.min_resolved,
            "min_pnl_usdc": CFG.min_pnl_usdc,
            "min_roi": CFG.min_roi,
            "min_z_score": CFG.min_z_score,
            "rate_path": {"n": CFG.min_n_for_rate_path, "win_rate": CFG.rate_path_win_rate},
            "lookback_recent_days": CFG.lookback_recent_days,
            "min_decay_win_rate": CFG.min_decay_win_rate,
            "max_drawdown_frac": CFG.max_drawdown_frac,
            "min_account_age_days": CFG.min_account_age_days,
        },
        "summary": {
            "wallets_evaluated": len(evaluated),
            "wallets_passed": len(passed),
            "failure_counts": failure_counts,
        },
        "watchlist": watchlist,
    }
    write_json(DATA_DIR / "watchlist.json", output)
    return output


if __name__ == "__main__":
    try:
        out = run_selection()
        if "error" in out:
            sys.exit(1)
        s = out["summary"]
        print(f"\nEvaluated: {s['wallets_evaluated']} wallets")
        print(f"Passed:    {s['wallets_passed']}\n")
        print("Failure reasons:")
        for k, v in sorted(s["failure_counts"].items(), key=lambda x: -x[1]):
            print(f"  {k:10s}: {v}")
        print("\nTop watchlist:")
        for i, r in enumerate(out["watchlist"][:20], 1):
            m = r["metrics"]
            print(f"  {i:>2}. {r['address'][:12]}... "
                  f"total={m['total_wins']:3d}/{m['n_total']:3d} ({m['raw_win_rate']*100:.0f}%)  "
                  f"conf={m['confirmed_wins']:2d}/{m['n_confirmed']:2d} z={m['z_score']:+.2f} p={m['p_value_one_sided']:.3f}  "
                  f"ROI={m['roi']*100:+.0f}%  PnL=${m['pnl_usdc']:>9,.0f}  "
                  f"30d={m['recent_wins']}/{m['recent_n']}")
    except Exception as exc:
        log.exception("whale_selector crashed: %s", exc)
        sys.exit(1)
