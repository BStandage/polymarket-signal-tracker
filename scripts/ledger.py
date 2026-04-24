"""Wallet trade ledger + per-market PnL reconstruction.

Polymarket's `data-api.polymarket.com/activity` endpoint returns a stream
of TRADE (BUY/SELL) and REDEEM events per wallet, timestamped on-chain.
From this we reconstruct:

  * Per-(wallet, market, outcome) cash flow: usdc_in (buys), usdc_out_trade
    (sells), usdc_out_redeem (redemption payouts).
  * Per-(wallet, market) net P&L and resolution status.
  * Per-wallet totals: realized_pnl, resolved_markets, wins, losses,
    total_capital, overall_roi, time-windowed PnL (30d / 90d / all).

This is the honest data source for wallet skill; `/positions` only shows
a current snapshot and loses history as positions get redeemed.

Output structure (returned by build_wallet_ledger):

    {
      "address": "0x...",
      "markets": [ {market_id, outcome_idx, usdc_in, usdc_out, pnl,
                    resolved, first_entry_ts, last_exit_ts}, ...],
      "totals": {
          "realized_pnl_usdc": ...,
          "capital_deployed_usdc": ...,
          "overall_roi": ...,
          "resolved_markets": ...,
          "wins": ..., "losses": ...,
          "win_rate": ...,
          "pnl_30d": ..., "pnl_90d": ..., "pnl_365d": ...,
          "first_trade_ts": ..., "last_trade_ts": ...,
      },
    }
"""

from __future__ import annotations

import datetime as dt
from typing import Any, Iterable

from utils import ApiClient, log, safe_float


# ---------------------------------------------------------------------------
# Activity fetch
# ---------------------------------------------------------------------------

def fetch_activity(client: ApiClient, addr: str, max_events: int = 2000,
                   page_size: int = 500) -> list[dict]:
    """Fetch up to max_events of a wallet's activity from /activity."""
    events: list[dict] = []
    offset = 0
    while len(events) < max_events:
        params = {"user": addr, "limit": page_size, "offset": offset}
        payload = client.get("/activity", params=params)
        if payload is None:
            break
        items = payload if isinstance(payload, list) else (
            payload.get("data") or payload.get("activity") or []
        )
        if not items:
            break
        events.extend(items)
        if len(items) < page_size:
            break
        offset += len(items)
    return events[:max_events]


# ---------------------------------------------------------------------------
# Ledger construction
# ---------------------------------------------------------------------------

def build_wallet_ledger(addr: str, events: Iterable[dict],
                         closed_market_outcomes: dict[str, int] | None = None) -> dict:
    """Group TRADE / REDEEM events into per-(market, outcome) cashflows.

    closed_market_outcomes: optional map {conditionId: winning_outcome_index}.
    Only used to flag a market as 'resolved' even when the wallet never
    redeemed (e.g. they sold out before resolution).
    """
    closed_market_outcomes = closed_market_outcomes or {}
    now_ts = dt.datetime.now(dt.timezone.utc).timestamp()

    # Per-(conditionId, outcomeIndex) aggregate.
    # REDEEMs have outcomeIndex=999 (not tied to a side). We attribute redeems
    # to whichever outcome the wallet has net-long shares on.
    sides: dict[tuple[str, int], dict[str, Any]] = {}
    redeems_by_market: dict[str, list[dict]] = {}

    for ev in events:
        mkt_id = str(ev.get("conditionId") or "")
        if not mkt_id:
            continue
        etype = ev.get("type")
        ts = safe_float(ev.get("timestamp"))
        usdc = safe_float(ev.get("usdcSize"))
        size = safe_float(ev.get("size"))

        if etype == "REDEEM":
            redeems_by_market.setdefault(mkt_id, []).append({
                "ts": ts, "usdc": usdc, "shares": size,
            })
            continue

        if etype != "TRADE":
            continue  # skip unknown

        outcome_idx = _coerce_outcome_idx(ev.get("outcomeIndex"))
        if outcome_idx is None:
            continue
        side = (ev.get("side") or "").upper()

        key = (mkt_id, outcome_idx)
        rec = sides.setdefault(key, {
            "market_id": mkt_id,
            "outcome_idx": outcome_idx,
            "market_title": ev.get("title") or "",
            "market_slug": ev.get("eventSlug") or ev.get("slug") or "",
            "usdc_in": 0.0,          # spent buying shares
            "usdc_out_trade": 0.0,   # recouped by selling shares
            "shares_bought": 0.0,
            "shares_sold": 0.0,
            "first_entry_ts": None,
            "last_trade_ts": None,
        })
        if not rec["market_title"] and ev.get("title"):
            rec["market_title"] = ev["title"]
        if not rec["market_slug"] and (ev.get("eventSlug") or ev.get("slug")):
            rec["market_slug"] = ev.get("eventSlug") or ev.get("slug")

        if side == "BUY":
            rec["usdc_in"] += usdc
            rec["shares_bought"] += size
            if rec["first_entry_ts"] is None or ts < rec["first_entry_ts"]:
                rec["first_entry_ts"] = ts
        elif side == "SELL":
            rec["usdc_out_trade"] += usdc
            rec["shares_sold"] += size
        if rec["last_trade_ts"] is None or (ts and ts > rec["last_trade_ts"]):
            rec["last_trade_ts"] = ts

    # Attribute REDEEMs to the outcome side the wallet was net-long on.
    for mkt_id, reds in redeems_by_market.items():
        total_redeem_usdc = sum(r["usdc"] for r in reds)
        last_redeem_ts = max((r["ts"] for r in reds if r["ts"]), default=None)
        # Find the outcome side with shares still held (bought > sold).
        candidates = [
            (side, rec) for (m, side), rec in sides.items()
            if m == mkt_id and rec["shares_bought"] - rec["shares_sold"] > 0.01
        ]
        if not candidates:
            # Redemption on a side we didn't track; create a skeleton record
            # so the PnL still flows through.
            key = (mkt_id, -1)
            rec = sides.setdefault(key, {
                "market_id": mkt_id,
                "outcome_idx": -1,
                "market_title": "",
                "market_slug": "",
                "usdc_in": 0.0,
                "usdc_out_trade": 0.0,
                "shares_bought": 0.0,
                "shares_sold": 0.0,
                "first_entry_ts": None,
                "last_trade_ts": None,
            })
            rec.setdefault("usdc_out_redeem", 0.0)
            rec["usdc_out_redeem"] += total_redeem_usdc
            rec["last_trade_ts"] = last_redeem_ts
            continue
        # If multiple sides have unresolved shares, split by share count.
        total_unresolved = sum(c[1]["shares_bought"] - c[1]["shares_sold"] for c in candidates)
        for side, rec in candidates:
            share_of = (rec["shares_bought"] - rec["shares_sold"]) / total_unresolved
            rec.setdefault("usdc_out_redeem", 0.0)
            rec["usdc_out_redeem"] += total_redeem_usdc * share_of
            if last_redeem_ts and (rec["last_trade_ts"] is None or last_redeem_ts > rec["last_trade_ts"]):
                rec["last_trade_ts"] = last_redeem_ts

    # Finalize per-side metrics.
    market_records: list[dict] = []
    for (mkt_id, outcome_idx), rec in sides.items():
        redeem = rec.get("usdc_out_redeem", 0.0)
        usdc_out = rec["usdc_out_trade"] + redeem
        pnl = usdc_out - rec["usdc_in"]
        shares_held = rec["shares_bought"] - rec["shares_sold"]

        # Resolved if: explicit redemption happened, or market is closed,
        # or wallet sold out all shares and no open position remains.
        resolved = (
            redeem > 0
            or mkt_id in closed_market_outcomes
            or (shares_held <= 0.01 and rec["usdc_in"] > 0)
        )

        record = {
            "market_id": mkt_id,
            "outcome_idx": outcome_idx,
            "market_title": rec["market_title"],
            "market_slug": rec["market_slug"],
            "side": _side_label(outcome_idx),
            "usdc_in": round(rec["usdc_in"], 2),
            "usdc_out": round(usdc_out, 2),
            "usdc_out_trade": round(rec["usdc_out_trade"], 2),
            "usdc_out_redeem": round(redeem, 2),
            "shares_bought": round(rec["shares_bought"], 2),
            "shares_sold": round(rec["shares_sold"], 2),
            "shares_held": round(max(0.0, shares_held), 2),
            "pnl_usdc": round(pnl, 2),
            "roi": round(pnl / rec["usdc_in"], 4) if rec["usdc_in"] > 0 else 0.0,
            "resolved": bool(resolved),
            "first_entry_ts": rec["first_entry_ts"],
            "last_trade_ts": rec["last_trade_ts"],
        }
        market_records.append(record)

    # Aggregate totals.
    totals = _aggregate_totals(market_records, now_ts)
    return {
        "address": addr,
        "markets": market_records,
        "totals": totals,
    }


def _coerce_outcome_idx(v: Any) -> int | None:
    try:
        i = int(v)
        return i if i in (0, 1) else None
    except (TypeError, ValueError):
        return None


def _side_label(idx: int) -> str:
    return "YES" if idx == 0 else "NO" if idx == 1 else ""


def _aggregate_totals(markets: list[dict], now_ts: float) -> dict:
    resolved = [m for m in markets if m["resolved"]]
    unresolved = [m for m in markets if not m["resolved"]]

    wins = sum(1 for m in resolved if m["pnl_usdc"] > 0)
    losses = sum(1 for m in resolved if m["pnl_usdc"] < 0)
    realized_pnl = sum(m["pnl_usdc"] for m in resolved)
    open_pnl     = sum(m["pnl_usdc"] for m in unresolved)
    capital      = sum(m["usdc_in"] for m in markets)
    capital_resolved = sum(m["usdc_in"] for m in resolved)

    # Windowed realized PnL
    def pnl_since(cutoff_ts: float) -> float:
        return sum(
            m["pnl_usdc"] for m in resolved
            if (m["last_trade_ts"] or 0) >= cutoff_ts
        )

    day = 86400.0
    pnl_30d  = pnl_since(now_ts - 30 * day)
    pnl_90d  = pnl_since(now_ts - 90 * day)
    pnl_365d = pnl_since(now_ts - 365 * day)

    all_ts = [m["first_entry_ts"] for m in markets if m["first_entry_ts"]]
    first_ts = min(all_ts) if all_ts else None
    last_ts  = max(m["last_trade_ts"] for m in markets if m["last_trade_ts"]) if markets else None

    win_rate = wins / (wins + losses) if (wins + losses) > 0 else 0.0
    overall_roi = (realized_pnl / capital_resolved) if capital_resolved > 0 else 0.0
    account_age_days = ((now_ts - first_ts) / 86400) if first_ts else 0.0

    return {
        "realized_pnl_usdc": round(realized_pnl, 2),
        "open_pnl_usdc":     round(open_pnl, 2),
        "total_pnl_usdc":    round(realized_pnl + open_pnl, 2),
        "capital_deployed_usdc": round(capital, 2),
        "capital_resolved_usdc": round(capital_resolved, 2),
        "overall_roi": round(overall_roi, 4),
        "resolved_markets": len(resolved),
        "open_markets": len(unresolved),
        "wins": wins,
        "losses": losses,
        "win_rate": round(win_rate, 4),
        "pnl_30d":  round(pnl_30d, 2),
        "pnl_90d":  round(pnl_90d, 2),
        "pnl_365d": round(pnl_365d, 2),
        "first_trade_ts": first_ts,
        "last_trade_ts":  last_ts,
        "account_age_days": round(account_age_days, 1),
    }
