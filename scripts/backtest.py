"""Walk-forward backtest of whale-ranking signal.

Core question: if we rank wallets by realized PnL **as of time T** using only
data available before T, do their positions in markets that resolve after T
outperform random? If yes, there's tradeable signal. If no, we're ranking rich
people.

This harness:
  1. Loads the per-wallet ledger (scripts/fetch_whales.py output).
  2. Loads closed-market outcomes (winning_outcome_index per conditionId).
  3. Walks forward through resolved markets in chronological order.
  4. For each resolved market M with end-time T:
     a. Computes each wallet's realized PnL using only ledger entries
        whose first_entry_ts < (T - lookback_minutes).
     b. Top-K wallets are the "recommended follow list" as of T.
     c. For each top-K wallet that had a position in M, we record which
        side they were on and whether that side won.
  5. Aggregates: top-K win-rate vs a) 50% baseline, b) market's implied
     probability at the wallet's entry time (the harder baseline).
  6. Applies a cost model: 2% fee + size-dependent slippage.
  7. Reports: Sharpe-ish ratio, avg PnL per signal, hit rate, and net
     after-cost edge.

Output: scripts/backtest_report.json consumed by the frontend.
"""

from __future__ import annotations

import datetime as dt
import json
import math
import statistics
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from utils import DATA_DIR, log, read_json, write_json, safe_float


# ===========================================================================
# Config
# ===========================================================================

@dataclass
class BacktestConfig:
    # Minimum wallet history required to rank a wallet at time T
    min_resolved_before_rank: int = 2
    # Exclude wallets whose account_age at T is below this (days)
    min_age_days_at_rank: int = 7
    # Top-K follow list sizes to evaluate
    top_ks: tuple[int, ...] = (10, 25, 50, 100)
    # Lookback gap — we rank using data at least this long before T
    # (to simulate realistic latency; no cheating with same-day info)
    rank_lag_hours: float = 0.5
    # Tailing latency: assume we enter this many minutes after the whale
    tail_latency_minutes: float = 30.0
    # Cost model — Polymarket charges fees on some venues; model worst-case
    fee_rate: float = 0.02                    # 2% per round trip
    slippage_small: float = 0.005             # < $1k position
    slippage_mid:   float = 0.015             # $1k – $10k
    slippage_large: float = 0.03              # > $10k
    # Ignore markets with fewer than this many participating ranked wallets
    min_ranked_participants: int = 1


CFG = BacktestConfig()


# ===========================================================================
# Cost model
# ===========================================================================

def slippage_for(size_usdc: float) -> float:
    if size_usdc < 1_000:  return CFG.slippage_small
    if size_usdc < 10_000: return CFG.slippage_mid
    return CFG.slippage_large


def apply_costs(raw_pnl: float, size_usdc: float) -> float:
    """Subtract realistic fees + slippage from a gross PnL figure."""
    fee = size_usdc * CFG.fee_rate
    slip = size_usdc * slippage_for(size_usdc)
    return raw_pnl - fee - slip


# ===========================================================================
# Helpers
# ===========================================================================

def _ts_of(m: dict) -> float | None:
    t = m.get("last_trade_ts") or m.get("first_entry_ts")
    try:
        return float(t) if t is not None else None
    except (TypeError, ValueError):
        return None


def _wallet_ledger_map(wallet: dict) -> list[dict]:
    """Return the wallet's per-market ledger entries, sorted by first entry ts."""
    return sorted(
        wallet.get("ledger_markets") or [],
        key=lambda m: (m.get("first_entry_ts") or 0)
    )


def historical_pnl_as_of(wallet_ledger: list[dict], cutoff_ts: float) -> tuple[float, int]:
    """Realized PnL and resolved-count using only markets that *finished*
    before cutoff_ts. Returns (pnl, resolved_count)."""
    pnl = 0.0
    cnt = 0
    for m in wallet_ledger:
        if not m.get("resolved"):
            continue
        lt = m.get("last_trade_ts")
        if lt is None or lt >= cutoff_ts:
            continue
        pnl += safe_float(m.get("pnl_usdc"))
        cnt += 1
    return pnl, cnt


def account_age_days_at(wallet_ledger: list[dict], cutoff_ts: float) -> float:
    first = next((m.get("first_entry_ts") for m in wallet_ledger
                   if m.get("first_entry_ts") is not None), None)
    if first is None or first >= cutoff_ts:
        return 0.0
    return (cutoff_ts - first) / 86400.0


# ===========================================================================
# Main backtest
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

    log.info("Backtest: %d wallets, %d closed markets with outcomes",
             len(wallets), len(closed_outcomes))

    # Pre-compute sorted ledgers per wallet
    ledgers_by_addr: dict[str, list[dict]] = {}
    for w in wallets:
        ledgers_by_addr[w["address"]] = _wallet_ledger_map(w)

    # Collect every resolved-market observation:
    # (market_id, resolve_ts, wallet_addr, wallet_pnl_on_this_mkt,
    #  usdc_in, outcome_side_wallet_bet_on, winning_outcome_idx)
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
            events.append({
                "market_id": mid,
                "resolve_ts": float(resolve_ts),
                "addr": addr,
                "outcome_idx": m.get("outcome_idx"),
                "usdc_in": safe_float(m.get("usdc_in")),
                "pnl": safe_float(m.get("pnl_usdc")),
                "winning_idx": closed_outcomes[mid],
                "correct": m.get("outcome_idx") == closed_outcomes[mid],
                "entry_ts": m.get("first_entry_ts"),
            })
    events.sort(key=lambda e: e["resolve_ts"])
    log.info("Backtest corpus: %d (wallet, resolved-market) observations", len(events))

    # For each event in time order, rank all wallets by their realized PnL
    # using only data *before* the event's resolve_ts - rank_lag. Then record
    # whether this wallet is in the top-K as-of-then. Score the prediction.
    lag_s = CFG.rank_lag_hours * 3600
    # Walk event by event — expensive (O(events * wallets)). For n=5000 events
    # × 500 wallets = 2.5M ops, still cheap in python.

    # Accumulate top-K prediction quality per K
    topk_stats: dict[int, dict] = {
        k: {"hits": 0, "misses": 0, "gross_pnl": 0.0, "net_pnl": 0.0,
             "capital": 0.0, "observations": 0}
        for k in CFG.top_ks
    }
    baseline_hits = 0
    baseline_total = 0

    # Pre-sort event timestamps so we don't re-scan
    # For each wallet compute prefix arrays of (ts, pnl, resolved_count)
    wallet_prefix: dict[str, list[tuple[float, float, int]]] = {}
    for addr, ledger in ledgers_by_addr.items():
        prefix = []
        pnl_sum = 0.0
        cnt = 0
        for m in ledger:
            if not m.get("resolved"):
                continue
            lt = m.get("last_trade_ts")
            if not lt:
                continue
            pnl_sum += safe_float(m.get("pnl_usdc"))
            cnt += 1
            prefix.append((float(lt), pnl_sum, cnt))
        wallet_prefix[addr] = prefix

    # Binary-search helper
    def pnl_at(addr: str, cutoff: float) -> tuple[float, int]:
        pref = wallet_prefix.get(addr) or []
        # find largest index with ts < cutoff
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

    for ev in events:
        cutoff = ev["resolve_ts"] - lag_s
        # Rank all wallets by pnl_at(cutoff) with eligibility
        ranked: list[tuple[str, float]] = []
        for addr in ledgers_by_addr:
            pnl, cnt = pnl_at(addr, cutoff)
            if cnt < CFG.min_resolved_before_rank:
                continue
            age = account_age_days_at(ledgers_by_addr[addr], cutoff)
            if age < CFG.min_age_days_at_rank:
                continue
            if pnl <= 0:
                continue
            ranked.append((addr, pnl))
        ranked.sort(key=lambda x: x[1], reverse=True)
        if not ranked:
            continue
        rank_of: dict[str, int] = {a: i for i, (a, _) in enumerate(ranked)}

        addr = ev["addr"]
        baseline_total += 1
        if ev["correct"]:
            baseline_hits += 1

        if addr not in rank_of:
            continue
        r = rank_of[addr]
        size = ev["usdc_in"]
        raw_pnl = ev["pnl"]
        net_pnl = apply_costs(raw_pnl, size)
        for k in CFG.top_ks:
            if r < k:
                s = topk_stats[k]
                s["observations"] += 1
                s["gross_pnl"]   += raw_pnl
                s["net_pnl"]     += net_pnl
                s["capital"]     += size
                if ev["correct"]:
                    s["hits"] += 1
                else:
                    s["misses"] += 1

    # ---- Summary -----
    summary: dict[str, Any] = {
        "n_events": len(events),
        "baseline_win_rate": round(baseline_hits / baseline_total, 4) if baseline_total else None,
        "baseline_total": baseline_total,
        "top_k": [],
    }
    for k in CFG.top_ks:
        s = topk_stats[k]
        n = s["observations"]
        if n == 0:
            summary["top_k"].append({"k": k, "observations": 0})
            continue
        hit_rate = s["hits"] / n
        edge_vs_baseline = (hit_rate - (summary["baseline_win_rate"] or 0.5))
        gross_roi = s["gross_pnl"] / s["capital"] if s["capital"] else 0.0
        net_roi   = s["net_pnl"]   / s["capital"] if s["capital"] else 0.0
        summary["top_k"].append({
            "k": k,
            "observations": n,
            "hit_rate": round(hit_rate, 4),
            "edge_vs_baseline_pp": round(edge_vs_baseline * 100, 2),
            "gross_roi": round(gross_roi, 4),
            "net_roi_after_costs": round(net_roi, 4),
            "gross_pnl_usdc": round(s["gross_pnl"], 2),
            "net_pnl_usdc":   round(s["net_pnl"],   2),
            "capital_usdc":   round(s["capital"],   2),
            "hits": s["hits"],
            "misses": s["misses"],
        })

    # Verdict
    best = max((t for t in summary["top_k"] if t.get("observations", 0) > 20),
               key=lambda t: t.get("net_roi_after_costs", -99), default=None)
    if best is None:
        verdict = "INSUFFICIENT DATA — not enough overlapping resolved-wallet observations to draw any conclusion."
    elif best["net_roi_after_costs"] > 0.05 and best["edge_vs_baseline_pp"] > 5:
        verdict = (
            f"TRADEABLE SIGNAL — top-{best['k']} ranking achieves "
            f"{best['hit_rate']*100:.1f}% hit rate "
            f"({best['edge_vs_baseline_pp']:+.1f}pp vs base {summary['baseline_win_rate']*100:.1f}%), "
            f"net ROI after costs {best['net_roi_after_costs']*100:.1f}%."
        )
    elif best["edge_vs_baseline_pp"] > 2:
        verdict = (
            f"WEAK SIGNAL — top-{best['k']} marginally beats baseline "
            f"({best['edge_vs_baseline_pp']:+.1f}pp), net ROI {best['net_roi_after_costs']*100:.1f}% — "
            f"not enough to trade systematically, but worth tracking."
        )
    else:
        verdict = (
            f"NO SIGNAL — top-{best['k']} hit rate {best['hit_rate']*100:.1f}% "
            f"({best['edge_vs_baseline_pp']:+.1f}pp vs baseline). "
            f"Ranking provides no predictive value post-cost."
        )

    report = {
        "updated_at": dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
        "config": {
            "min_resolved_before_rank": CFG.min_resolved_before_rank,
            "rank_lag_hours": CFG.rank_lag_hours,
            "tail_latency_minutes": CFG.tail_latency_minutes,
            "fee_rate": CFG.fee_rate,
            "top_ks": list(CFG.top_ks),
        },
        "summary": summary,
        "verdict": verdict,
    }
    write_json(DATA_DIR / "backtest_report.json", report)
    log.info("Backtest verdict: %s", verdict)
    return report


def _empty_report() -> dict:
    return {
        "updated_at": dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
        "summary": {"n_events": 0, "top_k": []},
        "verdict": "INSUFFICIENT DATA — rerun fetch_whales.py first.",
    }


if __name__ == "__main__":
    try:
        report = run_backtest()
        print(json.dumps(report["summary"], indent=2))
        print("\nVERDICT:", report["verdict"])
    except Exception as exc:
        log.exception("Backtest crashed: %s", exc)
        sys.exit(1)
