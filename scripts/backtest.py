"""Walk-forward backtest, quant-grade.

For each (wallet × resolved-market) observation:
  - Rank wallets by realized PnL using only data before T (− rank_lag).
  - Record whether this wallet's side matched the resolution.

Strategies (four slices from permissive → strict):
  * `broad`               — all rank-eligible events
  * `hard_bets`           — entry implied prob in [0.30, 0.70]
  * `hard_conv`           — hard_bets + position ≥ $5k
  * `hard_conv_decorr`    — hard_conv + count each market once

For each strategy × K combination we compute:
  - Hit rate + Wilson 95% CI
  - Two-proportion z-test p-value vs baseline
  - Bootstrap 95% CI on net ROI (1000 resamples)
  - Sharpe-like ratio on per-event returns
  - Cost sensitivity (at 0%/1%/2%/3% fee)
  - Train/test split: rank on first 70%, evaluate on last 30%

Verdict requires ALL of:
  - observations ≥ 50
  - p-value < 0.05 (one-sided, better than baseline)
  - net ROI at 2% fee > 3%
  - out-of-sample net ROI > 0 (if ≥15 OOS observations)

Output: docs/data/backtest_report.json.
"""

from __future__ import annotations

import datetime as dt
import json
import math
import random
import statistics
import sys
from dataclasses import dataclass
from typing import Any

from utils import DATA_DIR, log, read_json, write_json, safe_float


# ===========================================================================
# Config
# ===========================================================================

@dataclass
class BacktestConfig:
    min_resolved_before_rank: int = 2
    min_age_days_at_rank: int = 7
    rank_lag_hours: float = 0.5
    top_ks: tuple[int, ...] = (10, 25, 50, 100)

    # Cost model (central) + sensitivity grid
    fee_rate: float = 0.02
    fee_sensitivity: tuple[float, ...] = (0.00, 0.01, 0.02, 0.03)
    slippage_small: float = 0.005
    slippage_mid:   float = 0.015
    slippage_large: float = 0.03

    # Strategy filters
    difficulty_min_prob: float = 0.30
    difficulty_max_prob: float = 0.70
    conviction_min_usdc: float = 5_000.0

    # Verdict thresholds
    min_sample_for_verdict: int = 50
    verdict_p_threshold: float = 0.05
    verdict_net_roi_threshold: float = 0.03

    # Stats
    bootstrap_iters: int = 1000
    train_frac: float = 0.70
    random_seed: int = 42


CFG = BacktestConfig()
STRATEGIES = ("broad", "hard_bets", "hard_conv", "hard_conv_decorr")


# ===========================================================================
# Cost + stat helpers
# ===========================================================================

def slippage_for(size_usdc: float) -> float:
    if size_usdc < 1_000:  return CFG.slippage_small
    if size_usdc < 10_000: return CFG.slippage_mid
    return CFG.slippage_large


def apply_costs(raw_pnl: float, size: float, fee: float | None = None) -> float:
    f = CFG.fee_rate if fee is None else fee
    return raw_pnl - size * f - size * slippage_for(size)


def wilson_ci(p: float, n: int, z: float = 1.96) -> tuple[float, float]:
    if n == 0:
        return (0.0, 0.0)
    den = 1 + z * z / n
    centre = (p + z * z / (2 * n)) / den
    spread = z * math.sqrt((p * (1 - p) + z * z / (4 * n)) / n) / den
    return (max(0.0, centre - spread), min(1.0, centre + spread))


def two_prop_z(p1: float, n1: int, p2: float, n2: int) -> tuple[float, float]:
    """Return (z, one-sided p-value for p1 > p2)."""
    if n1 == 0 or n2 == 0:
        return (0.0, 1.0)
    p_pool = (p1 * n1 + p2 * n2) / (n1 + n2)
    se = math.sqrt(p_pool * (1 - p_pool) * (1 / n1 + 1 / n2))
    if se == 0:
        return (0.0, 1.0)
    z = (p1 - p2) / se
    p = 1.0 - 0.5 * (1.0 + math.erf(z / math.sqrt(2)))
    return (z, p)


def bootstrap_ci(pairs: list[tuple], stat_fn, iters: int, seed: int) -> tuple[float, float, float]:
    if not pairs:
        return (0.0, 0.0, 0.0)
    rng = random.Random(seed)
    n = len(pairs)
    point = stat_fn(pairs)
    samples = []
    for _ in range(iters):
        resample = [pairs[rng.randrange(n)] for _ in range(n)]
        samples.append(stat_fn(resample))
    samples.sort()
    lo = samples[max(0, int(0.025 * iters))]
    hi = samples[min(iters - 1, int(0.975 * iters) - 1)]
    return (point, lo, hi)


def sharpe_like(returns: list[float]) -> float:
    if len(returns) < 2:
        return 0.0
    mean = statistics.mean(returns)
    sd = statistics.pstdev(returns)
    if sd == 0:
        return 0.0
    return (mean / sd) * math.sqrt(200)


# ===========================================================================
# Helpers
# ===========================================================================

def _wallet_ledger_markets(wallet: dict) -> list[dict]:
    return sorted(
        wallet.get("ledger_markets") or [],
        key=lambda m: (m.get("first_entry_ts") or 0)
    )


def implied_prob_of_side_winning(m: dict) -> float | None:
    usdc_in = safe_float(m.get("usdc_in"))
    shares = safe_float(m.get("shares_bought"))
    if usdc_in <= 0 or shares <= 0:
        return None
    return usdc_in / shares


# ===========================================================================
# Main
# ===========================================================================

def run_backtest() -> dict:
    raw = read_json(DATA_DIR / "whales_raw.json", default={})
    outcomes_payload = read_json(DATA_DIR / "closed_outcomes.json", default={})
    wallets = raw.get("wallets", []) if isinstance(raw, dict) else []
    closed_outcomes = outcomes_payload.get("outcomes", {}) if isinstance(outcomes_payload, dict) else {}

    if not wallets or not closed_outcomes:
        log.warning("Backtest: missing wallet (%d) or closed outcomes (%d)",
                    len(wallets), len(closed_outcomes))
        return _empty_report()

    log.info("Backtest: %d wallets, %d closed markets", len(wallets), len(closed_outcomes))

    ledgers_by_addr: dict[str, list[dict]] = {
        w["address"]: _wallet_ledger_markets(w) for w in wallets
    }
    wallet_prefix: dict[str, list[tuple[float, float, int]]] = {}
    for addr, ledger in ledgers_by_addr.items():
        prefix = []
        pnl_sum = 0.0
        cnt = 0
        for m in ledger:
            if not m.get("resolved") or not m.get("last_trade_ts"):
                continue
            pnl_sum += safe_float(m.get("pnl_usdc"))
            cnt += 1
            prefix.append((float(m["last_trade_ts"]), pnl_sum, cnt))
        wallet_prefix[addr] = prefix

    def pnl_at(addr: str, cutoff: float) -> tuple[float, int]:
        pref = wallet_prefix.get(addr) or []
        lo, hi = 0, len(pref)
        while lo < hi:
            mid = (lo + hi) // 2
            if pref[mid][0] < cutoff:
                lo = mid + 1
            else:
                hi = mid
        if lo == 0:
            return 0.0, 0
        _, pnl, cnt = pref[lo - 1]
        return pnl, cnt

    def account_age_days_at(addr: str, cutoff: float) -> float:
        for m in ledgers_by_addr.get(addr, []):
            ts = m.get("first_entry_ts")
            if ts is not None and ts < cutoff:
                return (cutoff - ts) / 86400.0
        return 0.0

    events: list[dict] = []
    for addr, ledger in ledgers_by_addr.items():
        for m in ledger:
            mid = m.get("market_id")
            if not mid or not m.get("resolved"):
                continue
            if mid not in closed_outcomes:
                continue
            resolve_ts = m.get("last_trade_ts") or m.get("first_entry_ts")
            if not resolve_ts:
                continue
            implied = implied_prob_of_side_winning(m)
            events.append({
                "market_id": mid,
                "resolve_ts": float(resolve_ts),
                "addr": addr,
                "outcome_idx": m.get("outcome_idx"),
                "usdc_in": safe_float(m.get("usdc_in")),
                "pnl": safe_float(m.get("pnl_usdc")),
                "correct": m.get("outcome_idx") == closed_outcomes[mid],
                "implied_prob": implied,
            })
    events.sort(key=lambda e: e["resolve_ts"])
    log.info("Backtest corpus: %d (wallet, resolved-market) observations", len(events))

    lag_s = CFG.rank_lag_hours * 3600
    full = _run_pass(events, ledgers_by_addr, pnl_at, account_age_days_at, lag_s)

    # Out-of-sample: last 30% of events
    split_i = int(len(events) * CFG.train_frac)
    oos = None
    if split_i >= 5 and (len(events) - split_i) >= 5:
        oos = _run_pass(events[split_i:], ledgers_by_addr, pnl_at, account_age_days_at, lag_s)

    strategy_summaries = []
    best_strategy = None

    for strat in STRATEGIES:
        fs = full["strategies"][strat]
        top_k_out = []
        for k in CFG.top_ks:
            s = fs["top_k"][k]
            n = s["obs"]
            if n == 0:
                top_k_out.append({"k": k, "observations": 0})
                continue

            hr = s["hits"] / n
            base_n = fs["baseline_total"]
            base_wr = (fs["baseline_hits"] / base_n) if base_n else 0.5
            wci = wilson_ci(hr, n)
            _, pval = two_prop_z(hr, n, base_wr, base_n)

            capital = sum(s["sizes"])
            gross_pnl = sum(s["pnls"])
            net_pnl = sum(apply_costs(p, siz) for p, siz in zip(s["pnls"], s["sizes"]))
            gross_roi = gross_pnl / capital if capital else 0.0
            net_roi = net_pnl / capital if capital else 0.0

            # Bootstrap CI on net ROI
            pairs = list(zip(s["pnls"], s["sizes"]))
            def _net_roi_stat(resample):
                cap = sum(siz for _, siz in resample) or 1.0
                return sum(apply_costs(p, siz) for p, siz in resample) / cap
            _, ci_lo, ci_hi = bootstrap_ci(pairs, _net_roi_stat, CFG.bootstrap_iters, CFG.random_seed + k)

            # Cost sensitivity
            cost_sens = {}
            for fee in CFG.fee_sensitivity:
                np_ = sum(apply_costs(p, siz, fee=fee) for p, siz in zip(s["pnls"], s["sizes"]))
                cost_sens[f"fee_{int(fee*100)}pct"] = round(np_ / capital, 4) if capital else 0.0

            per_event_returns = [apply_costs(p, siz) / siz for p, siz in zip(s["pnls"], s["sizes"]) if siz > 0]
            sr = sharpe_like(per_event_returns)

            oos_val = None
            if oos:
                os_s = oos["strategies"][strat]["top_k"][k]
                if os_s["obs"] > 0:
                    oos_hr = os_s["hits"] / os_s["obs"]
                    oos_cap = sum(os_s["sizes"])
                    oos_net = sum(apply_costs(p, siz) for p, siz in zip(os_s["pnls"], os_s["sizes"]))
                    oos_val = {
                        "observations": os_s["obs"],
                        "hit_rate": round(oos_hr, 4),
                        "net_roi": round(oos_net / oos_cap, 4) if oos_cap else 0.0,
                    }

            top_k_out.append({
                "k": k,
                "observations": n,
                "hit_rate": round(hr, 4),
                "hit_rate_ci95": [round(wci[0], 4), round(wci[1], 4)],
                "edge_vs_baseline_pp": round((hr - base_wr) * 100, 2),
                "p_value_one_sided": round(pval, 4),
                "gross_roi": round(gross_roi, 4),
                "net_roi_after_costs": round(net_roi, 4),
                "net_roi_ci95": [round(ci_lo, 4), round(ci_hi, 4)],
                "net_pnl_usdc": round(net_pnl, 2),
                "gross_pnl_usdc": round(gross_pnl, 2),
                "capital_usdc": round(capital, 2),
                "sharpe_annualized": round(sr, 2),
                "cost_sensitivity_net_roi": cost_sens,
                "out_of_sample": oos_val,
                "hits": s["hits"],
                "misses": n - s["hits"],
            })

        strategy_summaries.append({
            "name": strat,
            "baseline_win_rate": round(fs["baseline_hits"] / fs["baseline_total"], 4) if fs["baseline_total"] else None,
            "baseline_total": fs["baseline_total"],
            "top_k": top_k_out,
        })

        for t in top_k_out:
            if t.get("observations", 0) < CFG.min_sample_for_verdict:
                continue
            if t.get("p_value_one_sided", 1.0) > CFG.verdict_p_threshold:
                continue
            if t.get("net_roi_after_costs", 0) < CFG.verdict_net_roi_threshold:
                continue
            oos_v = t.get("out_of_sample")
            if oos_v and oos_v.get("observations", 0) >= 15 and oos_v.get("net_roi", 0) <= 0:
                continue
            if best_strategy is None or t["net_roi_after_costs"] > best_strategy["net_roi_after_costs"]:
                best_strategy = {**t, "strategy": strat}

    if best_strategy:
        oos_txt = ""
        if best_strategy.get("out_of_sample") and best_strategy["out_of_sample"].get("observations", 0):
            o = best_strategy["out_of_sample"]
            oos_txt = f" · OOS ({o['observations']} obs) hit {o['hit_rate']*100:.1f}%, ROI {o['net_roi']*100:+.1f}%"
        verdict = (
            f"TRADEABLE SIGNAL — `{best_strategy['strategy']}` top-{best_strategy['k']}: "
            f"{best_strategy['hit_rate']*100:.1f}% hit rate "
            f"(Wilson 95% {best_strategy['hit_rate_ci95'][0]*100:.0f}-{best_strategy['hit_rate_ci95'][1]*100:.0f}%), "
            f"p={best_strategy['p_value_one_sided']:.3f}, "
            f"net ROI {best_strategy['net_roi_after_costs']*100:+.1f}% "
            f"(bootstrap {best_strategy['net_roi_ci95'][0]*100:+.1f} to {best_strategy['net_roi_ci95'][1]*100:+.1f}%), "
            f"Sharpe {best_strategy['sharpe_annualized']:.2f}{oos_txt}."
        )
    else:
        candidates = []
        for s in strategy_summaries:
            for t in s["top_k"]:
                if t.get("observations", 0) >= 20:
                    candidates.append({**t, "strategy": s["name"]})
        candidates.sort(key=lambda t: t.get("net_roi_after_costs", -99), reverse=True)
        if candidates:
            top = candidates[0]
            reasons = []
            if top.get("observations", 0) < CFG.min_sample_for_verdict:
                reasons.append(f"n={top['observations']} < {CFG.min_sample_for_verdict}")
            if top.get("p_value_one_sided", 1) > CFG.verdict_p_threshold:
                reasons.append(f"p={top['p_value_one_sided']:.3f} not significant")
            if top.get("net_roi_after_costs", -99) < CFG.verdict_net_roi_threshold:
                reasons.append(f"net ROI {top['net_roi_after_costs']*100:+.1f}% < {CFG.verdict_net_roi_threshold*100:.0f}%")
            verdict = (
                f"NO TRADEABLE SIGNAL — best candidate `{top['strategy']}` top-{top['k']}: "
                f"hit {top['hit_rate']*100:.1f}% ({top['edge_vs_baseline_pp']:+.1f}pp), "
                f"net ROI {top['net_roi_after_costs']*100:+.1f}%. "
                f"Failed: {', '.join(reasons)}."
            )
        else:
            verdict = "INSUFFICIENT DATA — fewer than 20 observations under any strategy."

    report = {
        "updated_at": dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
        "config": {
            "rank_lag_hours": CFG.rank_lag_hours,
            "fee_rate": CFG.fee_rate,
            "difficulty_range": [CFG.difficulty_min_prob, CFG.difficulty_max_prob],
            "conviction_min_usdc": CFG.conviction_min_usdc,
            "top_ks": list(CFG.top_ks),
            "verdict_thresholds": {
                "min_sample": CFG.min_sample_for_verdict,
                "p_value_max": CFG.verdict_p_threshold,
                "net_roi_min": CFG.verdict_net_roi_threshold,
            },
        },
        "n_events": len(events),
        "strategies": strategy_summaries,
        "best_strategy": best_strategy,
        "verdict": verdict,
        "summary": strategy_summaries[0] if strategy_summaries else {"n_events": len(events), "top_k": []},
    }
    write_json(DATA_DIR / "backtest_report.json", report)
    log.info("Backtest verdict: %s", verdict)
    return report


def _run_pass(events, ledgers_by_addr, pnl_at, age_fn, lag_s):
    def new_stats():
        return {"obs": 0, "hits": 0, "pnls": [], "sizes": []}

    result = {
        "strategies": {
            s: {"baseline_hits": 0, "baseline_total": 0,
                "top_k": {k: new_stats() for k in CFG.top_ks}}
            for s in STRATEGIES
        }
    }
    decorr_seen: set[tuple[str, int]] = set()

    for ev in events:
        cutoff = ev["resolve_ts"] - lag_s
        ranked = []
        for addr in ledgers_by_addr:
            pnl, cnt = pnl_at(addr, cutoff)
            if cnt < CFG.min_resolved_before_rank:
                continue
            if age_fn(addr, cutoff) < CFG.min_age_days_at_rank:
                continue
            if pnl <= 0:
                continue
            ranked.append((addr, pnl))
        ranked.sort(key=lambda x: x[1], reverse=True)
        if not ranked:
            continue
        rank_of = {a: i for i, (a, _) in enumerate(ranked)}
        addr = ev["addr"]
        if addr not in rank_of:
            continue
        r = rank_of[addr]

        implied = ev.get("implied_prob")
        is_hard = implied is not None and CFG.difficulty_min_prob <= implied <= CFG.difficulty_max_prob
        is_conv = ev["usdc_in"] >= CFG.conviction_min_usdc
        key = (ev["market_id"], ev["outcome_idx"])
        is_first = key not in decorr_seen

        for strat in STRATEGIES:
            if strat == "hard_bets" and not is_hard: continue
            if strat == "hard_conv" and not (is_hard and is_conv): continue
            if strat == "hard_conv_decorr" and not (is_hard and is_conv and is_first): continue

            srec = result["strategies"][strat]
            srec["baseline_total"] += 1
            if ev["correct"]:
                srec["baseline_hits"] += 1
            for k in CFG.top_ks:
                if r < k:
                    s = srec["top_k"][k]
                    s["obs"] += 1
                    s["pnls"].append(ev["pnl"])
                    s["sizes"].append(ev["usdc_in"])
                    if ev["correct"]:
                        s["hits"] += 1

        if is_hard and is_conv and is_first:
            decorr_seen.add(key)

    return result


def _empty_report() -> dict:
    return {
        "updated_at": dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
        "n_events": 0,
        "strategies": [],
        "best_strategy": None,
        "summary": {"n_events": 0, "top_k": []},
        "verdict": "INSUFFICIENT DATA — rerun fetch_whales.py first.",
    }


if __name__ == "__main__":
    try:
        report = run_backtest()
        print(json.dumps({
            "verdict": report["verdict"],
            "n_events": report.get("n_events", 0),
            "best_strategy": report.get("best_strategy"),
            "strategies_summary": [
                {"name": s["name"], "baseline_total": s["baseline_total"],
                 "top_10": next((t for t in s["top_k"] if t["k"] == 10), None)}
                for s in report.get("strategies", [])
            ],
        }, indent=2, default=str))
    except Exception as exc:
        log.exception("Backtest crashed: %s", exc)
        sys.exit(1)
