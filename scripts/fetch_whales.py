"""Polymarket whale tracker - data fetching pipeline.

Discovers large trades across active Polymarket markets, then builds a per-wallet
historical trade & position dataset. Outputs raw data for scoring downstream.

Outputs:
    data/markets.json      - snapshot of active markets
    data/whales_raw.json   - raw per-wallet aggregates + trades + positions
    data/last_updated.json - timestamp metadata

The pipeline is resumable: per-market trade lookups and per-wallet history
calls are cached under data/.cache so a mid-run failure can resume.
"""

from __future__ import annotations

import datetime as dt
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from tqdm import tqdm

from utils import (
    ApiClient,
    CACHE_DIR,
    DATA_DIR,
    classify_market,
    log,
    read_json,
    safe_float,
    safe_int,
    to_checksum,
    usdc_maybe,
    write_json,
)
from ledger import fetch_activity, build_wallet_ledger


# ===========================================================================
# CONFIG - tune these without touching logic
# ===========================================================================

@dataclass
class Config:
    # API endpoints
    gamma_base: str = "https://gamma-api.polymarket.com"
    clob_base: str = "https://clob.polymarket.com"
    data_base: str = "https://data-api.polymarket.com"

    # Discovery
    max_active_markets: int = 100      # markets to scan for whales
    market_page_size: int = 100
    whale_trade_min_usdc: float = 500  # configurable whale threshold
    trades_per_market_limit: int = 500

    # Wallet history
    max_wallets_to_analyze: int = 400  # hard cap on wallets queried (cost control)
    wallet_history_limit: int = 500    # per-wallet trade history pulls
    wallet_positions_limit: int = 200  # per-wallet open positions pulls
    wallet_activity_limit: int = 2000  # per-wallet full activity stream (ledger)

    # Resolved-market outcomes fetch
    fetch_closed_markets: bool = True
    closed_market_pages: int = 20       # up to this many pages of 500 each

    # Rate limiting
    rate_calls: int = 6
    rate_window_s: float = 1.0

    # Cache behavior
    use_cache: bool = True
    cache_ttl_seconds: int = 60 * 60 * 2  # 2 hours


CFG = Config()


# ===========================================================================
# Data classes
# ===========================================================================

@dataclass
class Market:
    condition_id: str
    slug: str
    title: str
    category: str
    active: bool
    closed: bool
    end_date: str | None
    created_at: str | None
    volume: float
    liquidity: float
    outcomes: list[str] = field(default_factory=list)
    raw: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "condition_id": self.condition_id,
            "slug": self.slug,
            "title": self.title,
            "category": self.category,
            "active": self.active,
            "closed": self.closed,
            "end_date": self.end_date,
            "created_at": self.created_at,
            "volume": self.volume,
            "liquidity": self.liquidity,
            "outcomes": self.outcomes,
        }


# ===========================================================================
# Clients
# ===========================================================================

gamma = ApiClient(CFG.gamma_base, rate_limit=(CFG.rate_calls, CFG.rate_window_s))
clob = ApiClient(CFG.clob_base, rate_limit=(CFG.rate_calls, CFG.rate_window_s))
data_api = ApiClient(CFG.data_base, rate_limit=(CFG.rate_calls, CFG.rate_window_s))


# ===========================================================================
# Cache helpers
# ===========================================================================

def _cache_path(key: str) -> Path:
    safe = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in key)
    return CACHE_DIR / f"{safe}.json"


def cache_get(key: str) -> Any:
    if not CFG.use_cache:
        return None
    p = _cache_path(key)
    if not p.exists():
        return None
    try:
        age = dt.datetime.now().timestamp() - p.stat().st_mtime
        if age > CFG.cache_ttl_seconds:
            return None
        return read_json(p)
    except OSError:
        return None


def cache_put(key: str, value: Any) -> None:
    if not CFG.use_cache:
        return
    try:
        write_json(_cache_path(key), value)
    except Exception as exc:  # never let cache write kill the pipeline
        log.debug("cache_put failed for %s: %s", key, exc)


# ===========================================================================
# Step 1: Fetch active markets (Gamma API)
# ===========================================================================

def fetch_active_markets() -> list[Market]:
    log.info("Fetching active markets from Gamma API")
    markets: list[Market] = []
    offset = 0
    pbar = tqdm(desc="markets", unit="mkt")
    while len(markets) < CFG.max_active_markets:
        remaining = CFG.max_active_markets - len(markets)
        limit = min(CFG.market_page_size, remaining)
        params = {
            "active": "true",
            "closed": "false",
            "limit": limit,
            "offset": offset,
        }
        payload = gamma.get("/markets", params=params)
        if payload is None:
            break
        # Gamma can return either a raw list or an object with a data/markets key
        items = _extract_list(payload, ("data", "markets"))
        if not items:
            break
        for m in items:
            market = _parse_market(m)
            if market and market.condition_id:
                markets.append(market)
        pbar.update(len(items))
        if len(items) < limit:
            break
        offset += len(items)
    pbar.close()
    log.info("Retrieved %d active markets", len(markets))
    return markets


def _extract_list(payload: Any, keys: tuple[str, ...]) -> list:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for k in keys:
            v = payload.get(k)
            if isinstance(v, list):
                return v
    return []


def _parse_market(m: dict) -> Market | None:
    condition_id = (
        m.get("conditionId")
        or m.get("condition_id")
        or m.get("id")
        or ""
    )
    if not condition_id:
        return None
    title = m.get("question") or m.get("title") or m.get("slug") or ""
    outcomes_raw = m.get("outcomes")
    outcomes: list[str] = []
    if isinstance(outcomes_raw, list):
        outcomes = [str(o) for o in outcomes_raw]
    elif isinstance(outcomes_raw, str):
        try:
            parsed = json.loads(outcomes_raw)
            if isinstance(parsed, list):
                outcomes = [str(o) for o in parsed]
        except json.JSONDecodeError:
            pass
    # Gamma returns the parent event in a nested `events: [{...}]` array.
    # The event slug is the canonical /event/<slug> URL segment; the market's
    # own slug often has a numeric suffix and won't route. Fall back to the
    # market slug only if no event is attached.
    event_slug = ""
    events = m.get("events")
    if isinstance(events, list) and events:
        first = events[0] if isinstance(events[0], dict) else {}
        event_slug = str(first.get("slug") or first.get("ticker") or "")
    slug = event_slug or str(m.get("slug") or "")
    return Market(
        condition_id=str(condition_id),
        slug=slug,
        title=str(title),
        category=classify_market(title),
        active=bool(m.get("active", True)),
        closed=bool(m.get("closed", False)),
        end_date=m.get("endDate") or m.get("end_date"),
        created_at=m.get("createdAt") or m.get("created_at"),
        volume=safe_float(m.get("volume") or m.get("volumeNum")),
        liquidity=safe_float(m.get("liquidity") or m.get("liquidityNum")),
        outcomes=outcomes,
        raw=m,
    )


# ===========================================================================
# Step 2: Fetch recent large trades per market
# ===========================================================================

def fetch_market_trades(market: Market) -> list[dict]:
    key = f"trades_{market.condition_id}"
    cached = cache_get(key)
    if cached is not None:
        return cached

    trades: list[dict] = []
    # Public data API works without auth; CLOB /trades is a fallback but
    # typically requires API keys for per-market queries.
    for client, path in (
        (data_api, "/trades"),
        (clob, "/trades"),
    ):
        params = {
            "market": market.condition_id,
            "limit": CFG.trades_per_market_limit,
        }
        payload = client.get(path, params=params)
        items = _extract_list(payload, ("data", "trades"))
        if items:
            trades = items
            break
    cache_put(key, trades)
    return trades


def extract_whales_from_trades(trades: list[dict], market: Market) -> dict[str, list[dict]]:
    """Return {address: [normalized_trade, ...]} for trades over the whale threshold."""
    buckets: dict[str, list[dict]] = {}
    for t in trades:
        size_usdc = _trade_size_usdc(t)
        if size_usdc < CFG.whale_trade_min_usdc:
            continue
        for addr in _trade_participants(t):
            trade_norm = _normalize_trade(t, market, size_usdc)
            buckets.setdefault(addr, []).append(trade_norm)
    return buckets


def _trade_size_usdc(t: dict) -> float:
    # Try several conventional field names
    candidates = [
        t.get("sizeUsdc"),
        t.get("notional"),
        t.get("usdcSize"),
        t.get("size_usd"),
        t.get("value"),
    ]
    for c in candidates:
        v = usdc_maybe(c)
        if v > 0:
            return v
    # Fall back to size * price
    size = usdc_maybe(t.get("size") or t.get("amount") or t.get("quantity"))
    price = safe_float(t.get("price") or t.get("avgPrice") or 0)
    if size and price:
        return size * price
    return 0.0


def _trade_participants(t: dict) -> list[str]:
    addrs: list[str] = []
    for key in ("maker", "taker", "makerAddress", "takerAddress", "proxyWallet",
                "owner", "user", "trader", "account"):
        v = t.get(key)
        if isinstance(v, str) and v.startswith("0x"):
            addrs.append(to_checksum(v))
    # dedupe preserving order
    seen: set[str] = set()
    out: list[str] = []
    for a in addrs:
        if a not in seen:
            seen.add(a)
            out.append(a)
    return out


def _normalize_trade(t: dict, market: Market, size_usdc: float) -> dict:
    return {
        "market_id": market.condition_id,
        "market_title": market.title,
        "market_category": market.category,
        "side": (t.get("side") or t.get("outcome") or "").upper(),
        "price": safe_float(t.get("price") or t.get("avgPrice")),
        "size_usdc": round(size_usdc, 2),
        "timestamp": t.get("timestamp") or t.get("time") or t.get("createdAt"),
        "tx_hash": t.get("transactionHash") or t.get("txHash") or t.get("hash"),
    }


# ===========================================================================
# Step 3: Per-wallet historical trades & positions
# ===========================================================================

def fetch_wallet_history(addr: str) -> dict[str, Any]:
    key = f"wallet_{addr.lower()}"
    cached = cache_get(key)
    if cached is not None:
        return cached

    # Full activity stream is the source of truth for PnL reconstruction.
    activity = fetch_activity(data_api, addr, max_events=CFG.wallet_activity_limit)
    # Positions endpoint still useful for current open positions (unrealized).
    positions = _fetch_wallet_positions(addr)
    # Trades kept for back-compat + volume metrics (redundant with activity).
    trades = _fetch_wallet_trades(addr)

    result = {
        "address": addr,
        "trades": trades,
        "positions": positions,
        "activity": activity,
    }
    cache_put(key, result)
    return result


# ---------------------------------------------------------------------------
# Closed markets (for resolution outcomes)
# ---------------------------------------------------------------------------

def fetch_closed_markets() -> dict[str, int]:
    """Return {conditionId: winning_outcome_index} for all closed markets.

    Uses gamma `/markets?closed=true` paginated. outcomePrices is a
    JSON-stringified array; index of the >0.5 value is the winner.
    Returns None-indexed (skipped) for ambiguous markets.
    """
    key = "closed_markets_v1"
    cached = cache_get(key)
    if cached is not None:
        return cached

    outcomes: dict[str, int] = {}
    offset = 0
    fetched = 0
    for _ in range(CFG.closed_market_pages):
        payload = gamma.get("/markets", params={
            "closed": "true", "limit": CFG.market_page_size, "offset": offset,
        })
        items = _extract_list(payload, ("data", "markets"))
        if not items:
            break
        for m in items:
            cid = m.get("conditionId")
            if not cid:
                continue
            op_raw = m.get("outcomePrices")
            try:
                op = json.loads(op_raw) if isinstance(op_raw, str) else (op_raw or [])
            except json.JSONDecodeError:
                op = []
            if not isinstance(op, list) or len(op) < 2:
                continue
            try:
                p0, p1 = float(op[0]), float(op[1])
            except (TypeError, ValueError):
                continue
            if p0 > 0.5 and p1 < 0.5:
                outcomes[str(cid)] = 0
            elif p1 > 0.5 and p0 < 0.5:
                outcomes[str(cid)] = 1
            # else: ambiguous / voided — skip
        fetched += len(items)
        if len(items) < CFG.market_page_size:
            break
        offset += len(items)
    log.info("Fetched outcomes for %d / %d closed markets", len(outcomes), fetched)
    cache_put(key, outcomes)
    return outcomes


def _fetch_wallet_trades(addr: str) -> list[dict]:
    for client, path, user_key in (
        (data_api, "/trades", "user"),
        (clob, "/trades", "maker"),
    ):
        params = {user_key: addr, "limit": CFG.wallet_history_limit}
        payload = client.get(path, params=params)
        items = _extract_list(payload, ("data", "trades"))
        if items:
            return items
    return []


def _fetch_wallet_positions(addr: str) -> list[dict]:
    params = {"user": addr, "limit": CFG.wallet_positions_limit}
    payload = data_api.get("/positions", params=params)
    items = _extract_list(payload, ("data", "positions"))
    return items or []


def _fetch_wallet_activity(addr: str) -> list[dict]:
    params = {"user": addr, "limit": 50}
    payload = data_api.get("/activity", params=params)
    items = _extract_list(payload, ("data", "activity"))
    return items or []


# ===========================================================================
# Aggregation
# ===========================================================================

def aggregate_wallet(addr: str, history: dict[str, Any], markets_by_id: dict[str, Market],
                     closed_outcomes: dict[str, int] | None = None) -> dict:
    """Per-wallet performance aggregate from the full activity ledger.

    Uses the complete /activity event stream to reconstruct per-market PnL
    (including redeemed positions that /positions has since dropped). Also
    attaches current open positions from /positions for the dashboard's
    Smart-money panel.
    """
    trades = history.get("trades") or []
    positions = history.get("positions") or []
    activity = history.get("activity") or []

    now_ts = dt.datetime.now(dt.timezone.utc).timestamp()

    # ---- Ledger from /activity (source of truth for PnL) -----------
    ledger = build_wallet_ledger(addr, activity, closed_outcomes or {})

    total_volume = 0.0
    markets_seen: set[str] = set()
    category_counts: dict[str, int] = {}
    first_trade_ts: float | None = None
    trade_sizes: list[float] = []
    earliest_entries: list[float] = []
    normalized_trades: list[dict] = []

    # ---- Volume and categories from trades stream (activity has same info
    # but trades array already normalized in other places) ------------
    for t in trades:
        mkt_id = str(t.get("market") or t.get("conditionId") or t.get("condition_id") or "")
        size_usdc = _trade_size_usdc(t)
        if size_usdc > 0:
            total_volume += size_usdc
            trade_sizes.append(size_usdc)
        if mkt_id:
            markets_seen.add(mkt_id)
        ts = _parse_ts(t.get("timestamp") or t.get("time") or t.get("createdAt"))
        if ts is not None and (first_trade_ts is None or ts < first_trade_ts):
            first_trade_ts = ts
        mkt = markets_by_id.get(mkt_id)
        if mkt:
            category_counts[mkt.category] = category_counts.get(mkt.category, 0) + 1
            if mkt.created_at and ts is not None:
                created = _parse_ts(mkt.created_at)
                if created is not None:
                    earliest_entries.append(max(0.0, ts - created))
        normalized_trades.append({
            "market_id": mkt_id,
            "market_title": (mkt.title if mkt else t.get("title") or ""),
            "side": (t.get("side") or t.get("outcome") or "").upper(),
            "price": safe_float(t.get("price")),
            "size_usdc": round(size_usdc, 2),
            "timestamp": t.get("timestamp") or t.get("time") or t.get("createdAt"),
        })

    # Build earliest-trade-per-market lookup so we can attach an entry
    # timestamp to each position.
    earliest_trade_ts: dict[str, float] = {}
    for ev in activity:
        if ev.get("type") != "TRADE" or (ev.get("side") or "").upper() != "BUY":
            continue
        mkt_id = str(ev.get("conditionId") or "")
        ts = safe_float(ev.get("timestamp"))
        if not mkt_id or not ts:
            continue
        prev = earliest_trade_ts.get(mkt_id)
        if prev is None or ts < prev:
            earliest_trade_ts[mkt_id] = ts

    # ---- Open positions for the dashboard (from /positions) --------
    open_positions: list[dict] = []
    resolved_positions: list[dict] = []  # unused now — resolved comes from ledger

    for p in positions:
        mkt_id = str(p.get("conditionId") or p.get("market") or "")
        mkt = markets_by_id.get(mkt_id)
        size = safe_float(p.get("size") or p.get("amount") or p.get("shares"))
        entry = safe_float(p.get("avgPrice") or p.get("entryPrice"))
        current = safe_float(p.get("curPrice") or p.get("currentPrice") or p.get("price"))
        initial_value = safe_float(p.get("initialValue"))
        current_value = safe_float(p.get("currentValue"))
        cash_pnl = safe_float(p.get("cashPnl"))
        pos_realized_pnl = safe_float(p.get("realizedPnl"))
        percent_pnl = safe_float(p.get("percentPnl"))
        end_date_raw = p.get("endDate")
        end_ts = _parse_ts(end_date_raw)
        redeemable = bool(p.get("redeemable"))

        # A position is "resolved" if the underlying market has ended or the
        # position is currently redeemable (payoff finalized).
        resolved = (end_ts is not None and end_ts < now_ts) or redeemable

        # Prefer eventSlug for the canonical /event/<slug> URL.
        pm_slug = (
            p.get("eventSlug") or p.get("event_slug")
            or (mkt.slug if mkt else None)
            or p.get("slug")
            or ""
        )

        entry_ts = earliest_trade_ts.get(mkt_id)
        entry_iso = (
            dt.datetime.fromtimestamp(entry_ts, dt.timezone.utc).isoformat().replace("+00:00", "Z")
            if entry_ts is not None else None
        )

        position_dict = {
            "market_id": mkt_id,
            "market_title": (mkt.title if mkt else p.get("title") or ""),
            "market_slug": pm_slug,
            "side": (p.get("outcome") or p.get("side") or "").upper(),
            "size": round(size, 2),                 # share count
            "entry_price": round(entry, 4),
            "current_price": round(current, 4),
            "initial_value_usdc": round(initial_value, 2),
            "current_value_usdc": round(current_value, 2),
            "cash_pnl_usdc": round(cash_pnl, 2),
            "realized_pnl_usdc": round(pos_realized_pnl, 2),
            "percent_pnl": round(percent_pnl / 100.0, 4),  # store as decimal
            "end_date": str(end_date_raw) if end_date_raw else None,
            "entry_timestamp": entry_iso,
            "redeemable": redeemable,
            # Back-compat fields for existing frontend:
            "size_usdc": round(initial_value, 2),
            "unrealized_pnl": round(cash_pnl, 2),
        }

        # All positions from /positions are "currently open" in the sense that
        # the wallet has not redeemed them yet. We attach ledger-derived
        # resolution state later.
        open_positions.append(position_dict)

    # ---- Ledger-derived totals are the authoritative numbers -------
    totals = ledger["totals"]
    ledger_markets = ledger["markets"]

    # Build a resolved-positions list from the ledger (for the expanded-row
    # view): every (market, side) that has a final PnL signal.
    resolved_from_ledger: list[dict] = []
    for m in ledger_markets:
        if not m["resolved"]:
            continue
        # Enrich with market title/slug from our markets snapshot if missing.
        mkt = markets_by_id.get(m["market_id"])
        title = m["market_title"] or (mkt.title if mkt else "")
        slug  = m["market_slug"]  or (mkt.slug  if mkt else "")
        initial_value = m["usdc_in"]
        cash_pnl = m["pnl_usdc"]
        roi = (cash_pnl / initial_value) if initial_value > 0 else 0.0
        resolved_from_ledger.append({
            "market_id": m["market_id"],
            "market_title": title,
            "market_slug": slug,
            "side": m["side"],
            "size": m["shares_bought"],
            "entry_price": round((m["usdc_in"] / m["shares_bought"]), 4) if m["shares_bought"] else 0.0,
            "initial_value_usdc": round(initial_value, 2),
            "cash_pnl_usdc": round(cash_pnl, 2),
            "percent_pnl": round(roi, 4),
            "end_date": None,
            "entry_timestamp": (
                dt.datetime.fromtimestamp(m["first_entry_ts"], dt.timezone.utc)
                    .isoformat().replace("+00:00", "Z")
                if m["first_entry_ts"] else None
            ),
            # Back-compat for existing frontend
            "size_usdc": round(initial_value, 2),
            "unrealized_pnl": round(cash_pnl, 2),
            "current_price": 0.0,  # resolved positions don't have a current price
        })
    resolved_from_ledger.sort(key=lambda r: r["cash_pnl_usdc"], reverse=True)

    avg_early_hours = (
        (sum(earliest_entries) / len(earliest_entries)) / 3600 if earliest_entries else None
    )
    avg_position_size = sum(trade_sizes) / len(trade_sizes) if trade_sizes else 0.0
    account_age_days = totals.get("account_age_days", 0.0) or (
        (now_ts - first_trade_ts) / 86400 if first_trade_ts is not None else 0
    )

    normalized_trades.sort(key=lambda x: _parse_ts(x.get("timestamp")) or 0, reverse=True)
    recent_trades = normalized_trades[:10]

    return {
        "address": addr,
        # Ledger-derived P&L (authoritative)
        "total_pnl_usdc": totals["total_pnl_usdc"],
        "realized_pnl_usdc": totals["realized_pnl_usdc"],
        "open_pnl_usdc": totals["open_pnl_usdc"],
        "capital_deployed_usdc": totals["capital_deployed_usdc"],
        "capital_resolved_usdc": totals["capital_resolved_usdc"],
        "overall_roi": totals["overall_roi"],
        "average_roi": totals["overall_roi"],
        "wins": totals["wins"],
        "losses": totals["losses"],
        "win_rate": totals["win_rate"],
        "resolved_markets": totals["resolved_markets"],
        "open_markets": totals["open_markets"],
        "pnl_30d":  totals["pnl_30d"],
        "pnl_90d":  totals["pnl_90d"],
        "pnl_365d": totals["pnl_365d"],
        "first_trade_ts": totals["first_trade_ts"],
        "last_trade_ts":  totals["last_trade_ts"],
        "account_age_days": round(account_age_days, 1),
        # Activity/volume proxies from trade stream
        "markets_participated": len(markets_seen),
        "total_volume_usdc": round(total_volume, 2),
        "avg_position_size_usdc": round(avg_position_size, 2),
        "avg_early_entry_hours": avg_early_hours,
        "category_breakdown": category_counts,
        # Displays
        "open_positions": open_positions,
        "resolved_positions": resolved_from_ledger,
        "recent_trades": recent_trades,
        # Raw ledger for downstream backtest
        "ledger_markets": ledger_markets,
    }


def _parse_ts(value: Any) -> float | None:
    """Best-effort timestamp parser. Returns None on any failure.

    Guarded against Windows' `datetime.timestamp()` raising OSError (Errno 22)
    on dates outside the local-tz-representable range.
    """
    if value is None:
        return None
    try:
        if isinstance(value, (int, float)):
            v = float(value)
            return v / 1000 if v > 1e12 else v
        s = str(value).strip()
        if not s:
            return None
        if s.isdigit():
            v = float(s)
            return v / 1000 if v > 1e12 else v
        parsed = dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
        # Ensure tz-aware so timestamp() doesn't hit local-tz edge cases.
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=dt.timezone.utc)
        return parsed.timestamp()
    except (ValueError, OSError, OverflowError, TypeError):
        return None


# ===========================================================================
# Orchestration
# ===========================================================================

def run() -> int:
    started = dt.datetime.now(dt.timezone.utc)
    log.info("=== fetch_whales pipeline start ===")

    # Step 1
    try:
        markets = fetch_active_markets()
    except Exception as exc:
        log.exception("Market discovery failed: %s", exc)
        markets = []
    markets_by_id = {m.condition_id: m for m in markets}

    # Step 1b — resolution outcomes for all closed markets (once, cached)
    closed_outcomes: dict[str, int] = {}
    if CFG.fetch_closed_markets:
        try:
            closed_outcomes = fetch_closed_markets()
        except Exception as exc:
            log.warning("Closed-markets fetch failed: %s", exc)

    write_json(DATA_DIR / "markets.json", {
        "updated_at": started.isoformat().replace("+00:00", "Z"),
        "markets": [m.to_dict() for m in markets],
    })
    if closed_outcomes:
        write_json(DATA_DIR / "closed_outcomes.json", {
            "updated_at": started.isoformat().replace("+00:00", "Z"),
            "outcomes": closed_outcomes,
        })

    # Step 2
    log.info("Scanning %d markets for whale trades (>= %s USDC)",
             len(markets), CFG.whale_trade_min_usdc)
    whale_buckets: dict[str, list[dict]] = {}
    for market in tqdm(markets, desc="scanning trades", unit="mkt"):
        try:
            trades = fetch_market_trades(market)
        except Exception as exc:
            log.warning("fetch trades failed for %s: %s", market.condition_id, exc)
            continue
        buckets = extract_whales_from_trades(trades, market)
        for addr, new_trades in buckets.items():
            whale_buckets.setdefault(addr, []).extend(new_trades)

    # Rank wallets by in-scan whale volume to decide which to deep-dive
    ranked = sorted(
        whale_buckets.items(),
        key=lambda kv: sum(t["size_usdc"] for t in kv[1]),
        reverse=True,
    )[: CFG.max_wallets_to_analyze]
    log.info("Discovered %d whale candidates, deep-diving top %d",
             len(whale_buckets), len(ranked))

    # Step 3 + aggregation
    wallet_aggregates: list[dict] = []
    for addr, whale_trades in tqdm(ranked, desc="wallet history", unit="w"):
        try:
            history = fetch_wallet_history(addr)
        except Exception as exc:
            log.warning("history fetch failed for %s: %s", addr, exc)
            history = {"address": addr, "trades": [], "positions": [], "activity": []}
        try:
            agg = aggregate_wallet(addr, history, markets_by_id, closed_outcomes)
        except Exception as exc:
            log.warning("aggregate failed for %s: %s", addr, exc)
            continue

        # Include in-scan whale trades so scoring can see recent conviction
        agg["whale_trades_in_scan"] = whale_trades[:20]
        wallet_aggregates.append(agg)

    # Outputs
    write_json(DATA_DIR / "whales_raw.json", {
        "updated_at": started.isoformat().replace("+00:00", "Z"),
        "config": {
            "whale_trade_min_usdc": CFG.whale_trade_min_usdc,
            "max_active_markets": CFG.max_active_markets,
            "max_wallets_to_analyze": CFG.max_wallets_to_analyze,
        },
        "wallets": wallet_aggregates,
    })
    write_json(DATA_DIR / "last_updated.json", {
        "updated_at": started.isoformat().replace("+00:00", "Z"),
        "markets_scanned": len(markets),
        "whales_found": len(whale_buckets),
        "wallets_analyzed": len(wallet_aggregates),
    })

    elapsed = (dt.datetime.now(dt.timezone.utc) - started).total_seconds()
    log.info("=== fetch_whales done in %.1fs ===", elapsed)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(run())
    except KeyboardInterrupt:
        log.warning("interrupted by user")
        sys.exit(130)
    except Exception as exc:
        log.exception("pipeline crashed: %s", exc)
        sys.exit(1)
