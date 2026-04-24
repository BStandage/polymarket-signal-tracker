"""Live signal detector — polls watchlist wallets for fresh entries.

Runs on a tight cron (~5 min). For each wallet currently on the watchlist,
fetches their recent /activity and surfaces any BUY trades from the last
N minutes. These are the tailable signals.

For each fresh signal, attaches:
  * The whale's watchlist metrics (lifetime record, 30-day record, PnL)
  * Current market price (to compute drift from whale's entry)
  * An ENTER / LATE / SKIP verdict based on freshness + drift + size

Output: docs/data/live_signals.json
"""

from __future__ import annotations

import datetime as dt
import json
import sys
import time
from dataclasses import dataclass
from typing import Any

from utils import ApiClient, DATA_DIR, log, read_json, write_json, safe_float


# ===========================================================================
# Config
# ===========================================================================

@dataclass
class SignalConfig:
    data_base: str = "https://data-api.polymarket.com"
    gamma_base: str = "https://gamma-api.polymarket.com"

    # Freshness windows
    signal_lookback_minutes: int = 45     # trades older than this are not emitted
    fresh_minutes: int = 10               # ENTER-verdict cutoff
    late_minutes: int = 30                # LATE-verdict cutoff

    # Filters
    min_size_usdc: float = 500.0          # drop boredom trades
    max_drift_for_enter: float = 0.03     # price drift from whale entry > 3% downgrades to WAIT
    max_drift_for_late: float = 0.07      # > 7% drift → SKIP

    # Per-wallet activity pull
    activity_limit: int = 50

    # Rate limiting
    rate_calls: int = 6
    rate_window_s: float = 1.0


CFG = SignalConfig()


# ===========================================================================
# Core logic
# ===========================================================================

def fetch_recent_trades(client: ApiClient, addr: str, since_ts: float) -> list[dict]:
    """Fetch recent /activity for a wallet, filter to BUY trades after since_ts."""
    payload = client.get("/activity", params={"user": addr, "limit": CFG.activity_limit})
    if payload is None:
        return []
    items = payload if isinstance(payload, list) else (
        payload.get("data") or payload.get("activity") or []
    )
    out = []
    for ev in items:
        if ev.get("type") != "TRADE":
            continue
        if (ev.get("side") or "").upper() != "BUY":
            continue
        ts = safe_float(ev.get("timestamp"))
        if not ts or ts < since_ts:
            continue
        out.append(ev)
    return out


def fetch_current_price(gamma: ApiClient, condition_id: str, outcome_idx: int) -> float | None:
    """Hit Gamma for the market's current price on the given outcome side."""
    payload = gamma.get(f"/markets", params={"condition_ids": condition_id, "limit": 1})
    if payload is None:
        return None
    items = payload if isinstance(payload, list) else (
        payload.get("data") or payload.get("markets") or []
    )
    if not items:
        return None
    m = items[0]
    raw_prices = m.get("outcomePrices")
    try:
        prices = json.loads(raw_prices) if isinstance(raw_prices, str) else (raw_prices or [])
    except json.JSONDecodeError:
        prices = []
    if not isinstance(prices, list) or len(prices) <= outcome_idx:
        return None
    try:
        return float(prices[outcome_idx])
    except (TypeError, ValueError):
        return None


def classify_signal(trade: dict, current_price: float | None) -> dict:
    """Produce ENTER / LATE / SKIP + reasoning."""
    now_ts = dt.datetime.now(dt.timezone.utc).timestamp()
    ts = safe_float(trade.get("timestamp"))
    age_min = (now_ts - ts) / 60.0 if ts else 9999
    entry = safe_float(trade.get("price"))
    size_usdc = safe_float(trade.get("usdcSize"))

    drift = None
    if current_price is not None and entry > 0:
        drift = abs(current_price - entry)

    reasons: list[str] = []
    verdict = "ENTER"

    if age_min > CFG.late_minutes:
        verdict = "SKIP"
        reasons.append(f"stale ({age_min:.0f}m old)")
    elif age_min > CFG.fresh_minutes:
        verdict = "LATE"
        reasons.append(f"{age_min:.0f}m late")

    if size_usdc < CFG.min_size_usdc:
        verdict = "SKIP"
        reasons.append(f"size only ${size_usdc:.0f}")

    if drift is not None:
        if drift > CFG.max_drift_for_late:
            verdict = "SKIP"
            reasons.append(f"price moved {drift*100:.1f}% since entry")
        elif drift > CFG.max_drift_for_enter and verdict == "ENTER":
            verdict = "LATE"
            reasons.append(f"price drifted {drift*100:.1f}%")

    return {
        "verdict": verdict,
        "reasons": reasons,
        "age_min": round(age_min, 1),
        "drift": round(drift, 4) if drift is not None else None,
    }


# ===========================================================================
# Main
# ===========================================================================

def run() -> dict:
    wl_payload = read_json(DATA_DIR / "watchlist.json", default={})
    watchlist = wl_payload.get("watchlist", []) if isinstance(wl_payload, dict) else []
    if not watchlist:
        log.warning("Signal detector: empty watchlist, writing empty output")
        return _write_empty()

    log.info("Signal detector: polling %d watchlisted wallets", len(watchlist))

    data_api = ApiClient(CFG.data_base, rate_limit=(CFG.rate_calls, CFG.rate_window_s))
    gamma    = ApiClient(CFG.gamma_base, rate_limit=(CFG.rate_calls, CFG.rate_window_s))

    now_ts   = dt.datetime.now(dt.timezone.utc).timestamp()
    since_ts = now_ts - CFG.signal_lookback_minutes * 60

    signals: list[dict] = []

    for entry in watchlist:
        addr = entry["address"]
        metrics = entry.get("metrics", {})
        trades = fetch_recent_trades(data_api, addr, since_ts)
        for t in trades:
            cid = t.get("conditionId")
            outcome_idx = t.get("outcomeIndex")
            try:
                outcome_idx = int(outcome_idx) if outcome_idx is not None else None
            except (TypeError, ValueError):
                outcome_idx = None

            current_price = None
            if cid and outcome_idx in (0, 1):
                current_price = fetch_current_price(gamma, cid, outcome_idx)

            classification = classify_signal(t, current_price)

            entry_price = safe_float(t.get("price"))
            size_usdc   = safe_float(t.get("usdcSize"))
            shares      = safe_float(t.get("size"))

            sig = {
                "signal_id": f"{addr[:10]}_{t.get('transactionHash','')[:10]}",
                "ts":             safe_float(t.get("timestamp")),
                "timestamp_iso":  dt.datetime.fromtimestamp(
                                    safe_float(t.get("timestamp")), dt.timezone.utc
                                  ).isoformat().replace("+00:00", "Z") if t.get("timestamp") else None,
                "market_id":      cid,
                "market_title":   t.get("title"),
                "market_slug":    t.get("eventSlug") or t.get("slug"),
                "outcome":        t.get("outcome"),
                "outcome_idx":    outcome_idx,
                "side":           (t.get("outcome") or "").upper(),
                "entry_price":    round(entry_price, 4),
                "current_price":  round(current_price, 4) if current_price is not None else None,
                "shares":         round(shares, 2),
                "size_usdc":      round(size_usdc, 2),
                "tx_hash":        t.get("transactionHash"),
                "whale": {
                    "address":        addr,
                    "pseudonym":      t.get("pseudonym"),
                    "raw_win_rate":   metrics.get("raw_win_rate"),
                    "lifetime_pnl":   metrics.get("pnl_usdc"),
                    "roi":            metrics.get("roi"),
                    "recent_wins":    metrics.get("recent_wins"),
                    "recent_n":       metrics.get("recent_n"),
                    "recent_win_rate":metrics.get("recent_win_rate"),
                    "z_score":        metrics.get("z_score"),
                    "n_total":        metrics.get("n_total"),
                    "total_wins":     metrics.get("total_wins"),
                },
                "verdict":     classification["verdict"],
                "age_min":     classification["age_min"],
                "drift":       classification["drift"],
                "skip_reasons": classification["reasons"],
            }
            signals.append(sig)

    signals.sort(key=lambda s: s["ts"] or 0, reverse=True)

    output = {
        "updated_at": dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
        "config": {
            "signal_lookback_minutes": CFG.signal_lookback_minutes,
            "fresh_minutes": CFG.fresh_minutes,
            "late_minutes":  CFG.late_minutes,
            "min_size_usdc": CFG.min_size_usdc,
        },
        "watchlist_size": len(watchlist),
        "signal_count": len(signals),
        "enter_count": sum(1 for s in signals if s["verdict"] == "ENTER"),
        "signals": signals,
    }
    write_json(DATA_DIR / "live_signals.json", output)
    log.info("Wrote %d signals (%d ENTER) from %d watchlisted wallets",
             len(signals), output["enter_count"], len(watchlist))
    return output


def _write_empty() -> dict:
    output = {
        "updated_at": dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
        "signal_count": 0, "enter_count": 0, "watchlist_size": 0,
        "signals": [],
    }
    write_json(DATA_DIR / "live_signals.json", output)
    return output


if __name__ == "__main__":
    try:
        out = run()
        print(f"\nSignals: {out['signal_count']} total, {out['enter_count']} ENTER-verdict")
        for s in out["signals"][:10]:
            w = s["whale"]
            print(f"  [{s['verdict']}] {s['age_min']:.0f}m  {s['side']:3s} @ {s['entry_price']:.3f} "
                  f"${s['size_usdc']:>7,.0f}  {s['market_title'][:50] if s['market_title'] else s['market_id'][:12]}")
            print(f"       whale {w['address'][:12]}  "
                  f"{w.get('total_wins','?')}/{w.get('n_total','?')} ({(w.get('raw_win_rate') or 0)*100:.0f}%)  "
                  f"PnL ${w.get('lifetime_pnl') or 0:,.0f}")
    except Exception as exc:
        log.exception("signal_detector crashed: %s", exc)
        sys.exit(1)
