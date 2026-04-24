"""Autonomous trade executor for whale-tailing.

Runs as a background loop:
  1. Refresh live signals (polls watchlist via signal_detector)
  2. Check open positions for exit conditions
  3. Apply strict filters to new ENTER signals → open positions
  4. Notify Discord on every action

Two modes:
  * paper: no real orders placed. Simulates everything. Default.
  * live : calls py_clob_client to place real orders on Polymarket CLOB.

Kill switch: create a file named HALT in the project root. Bot pauses
new entries immediately. Delete the file to resume.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import sys
import time
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from utils import DATA_DIR, ROOT_DIR, log, read_json, write_json, safe_float
import signal_detector
import discord_notifier as discord


load_dotenv(ROOT_DIR / ".env")


# ===========================================================================
# Config — strict defaults for $100 bankroll experiment
# ===========================================================================

@dataclass
class ExecutorConfig:
    # Bankroll management
    starting_bankroll: float = 100.00
    max_position_size_usdc: float = 12.00   # 12% max per bet
    max_concurrent: int = 3
    daily_loss_limit_usdc: float = 25.00
    bankroll_floor_usdc: float = 50.00      # halt if bankroll drops this low

    # Signal filters (tighter than dashboard defaults)
    require_verdict: str = "ENTER"          # no LATE, no SKIP
    max_drift: float = 0.02                 # 2% max price drift from whale entry
    min_whale_size_usdc: float = 1000.0     # whale bet conviction
    min_entry_price: float = 0.30
    max_entry_price: float = 0.65

    # Exit rules
    stop_loss_pct: float = -0.30            # -30% unrealized → exit
    take_profit_pct: float =  0.40          # +40% unrealized → exit
    max_hold_days: int = 7                  # capital rotation

    # Loop cadence
    loop_interval_seconds: int = 60
    heartbeat_hours: int = 6

    mode: str = "paper"                     # "paper" or "live"


CFG = ExecutorConfig()

# Read env overrides
CFG.mode = os.environ.get("MODE", CFG.mode).lower()
_f = lambda k, d: float(os.environ.get(k, d))
_i = lambda k, d: int(os.environ.get(k, d))
CFG.starting_bankroll      = _f("STARTING_BANKROLL", CFG.starting_bankroll)
CFG.max_position_size_usdc = _f("MAX_POSITION_SIZE", CFG.max_position_size_usdc)
CFG.max_concurrent         = _i("MAX_CONCURRENT",   CFG.max_concurrent)
CFG.daily_loss_limit_usdc  = _f("DAILY_LOSS_LIMIT", CFG.daily_loss_limit_usdc)
CFG.bankroll_floor_usdc    = _f("BANKROLL_FLOOR",   CFG.bankroll_floor_usdc)
CFG.max_drift              = _f("MAX_DRIFT",        CFG.max_drift)
CFG.min_whale_size_usdc    = _f("MIN_WHALE_SIZE",   CFG.min_whale_size_usdc)
CFG.min_entry_price        = _f("MIN_ENTRY",        CFG.min_entry_price)
CFG.max_entry_price        = _f("MAX_ENTRY",        CFG.max_entry_price)
CFG.stop_loss_pct          = _f("STOP_LOSS_PCT",    CFG.stop_loss_pct)
CFG.take_profit_pct        = _f("TAKE_PROFIT_PCT",  CFG.take_profit_pct)
CFG.max_hold_days          = _i("MAX_HOLD_DAYS",    CFG.max_hold_days)
CFG.loop_interval_seconds  = _i("LOOP_INTERVAL",    CFG.loop_interval_seconds)

PORTFOLIO_PATH = DATA_DIR / "portfolio.json"
HALT_PATH      = ROOT_DIR / "HALT"


# ===========================================================================
# Portfolio state
# ===========================================================================

def load_portfolio() -> dict:
    p = read_json(PORTFOLIO_PATH, default=None)
    if p is None:
        p = {
            "bankroll": CFG.starting_bankroll,
            "open_positions": [],
            "closed_trades": [],
            "daily_pnl": 0.0,
            "daily_reset_date": dt.date.today().isoformat(),
            "total_signals_processed": 0,
            "total_entries_taken": 0,
            "halted": False,
            "halt_reason": None,
        }
        save_portfolio(p)
    # Daily rollover
    today = dt.date.today().isoformat()
    if p.get("daily_reset_date") != today:
        p["daily_pnl"] = 0.0
        p["daily_reset_date"] = today
    return p


def save_portfolio(p: dict) -> None:
    p["updated_at"] = dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")
    write_json(PORTFOLIO_PATH, p)


# ===========================================================================
# Filters — strict gating
# ===========================================================================

def signal_passes_strict_filters(sig: dict) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    if sig.get("verdict") != CFG.require_verdict:
        reasons.append(f"verdict={sig.get('verdict')}")
    drift = sig.get("drift")
    if drift is None:
        reasons.append("drift-unknown")
    elif abs(drift) > CFG.max_drift:
        reasons.append(f"drift={drift*100:+.1f}% exceeds {CFG.max_drift*100:.0f}%")
    if safe_float(sig.get("size_usdc")) < CFG.min_whale_size_usdc:
        reasons.append(f"whale-size=${sig.get('size_usdc',0):,.0f} < ${CFG.min_whale_size_usdc:,.0f}")
    entry = safe_float(sig.get("entry_price"))
    if entry < CFG.min_entry_price:
        reasons.append(f"entry {entry:.3f} < {CFG.min_entry_price}")
    if entry > CFG.max_entry_price:
        reasons.append(f"entry {entry:.3f} > {CFG.max_entry_price}")
    return (len(reasons) == 0, reasons)


def portfolio_can_open(p: dict) -> tuple[bool, str]:
    if p.get("halted"):
        return False, f"halted: {p.get('halt_reason')}"
    if p["bankroll"] < CFG.bankroll_floor_usdc:
        return False, f"bankroll ${p['bankroll']:.2f} below floor ${CFG.bankroll_floor_usdc}"
    if p["daily_pnl"] < -CFG.daily_loss_limit_usdc:
        return False, f"daily P&L ${p['daily_pnl']:.2f} past daily loss limit"
    if len(p["open_positions"]) >= CFG.max_concurrent:
        return False, f"{CFG.max_concurrent} concurrent positions open"
    return True, ""


def position_size_for(p: dict) -> float:
    # Cap at both per-position limit and what bankroll permits after
    # reserving for existing open positions.
    available = p["bankroll"] - sum(pos["size_usdc"] for pos in p["open_positions"])
    return max(0.0, min(CFG.max_position_size_usdc, available))


# ===========================================================================
# Entry logic
# ===========================================================================

def already_in_position(p: dict, market_id: str, side: str) -> bool:
    return any(
        pos["market_id"] == market_id and pos["side"] == side
        for pos in p["open_positions"]
    )


def open_position(p: dict, sig: dict, tail_size: float) -> dict:
    """Create + record a new position. In live mode, this is where the CLOB
    order would be submitted. In paper mode, just record intent."""
    now_iso = dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")
    pos = {
        "signal_id": sig.get("signal_id"),
        "market_id": sig.get("market_id"),
        "market_title": sig.get("market_title"),
        "market_slug": sig.get("market_slug"),
        "side": sig.get("side"),
        "outcome_idx": sig.get("outcome_idx"),
        "entry_price": safe_float(sig.get("entry_price")),
        "size_usdc": tail_size,
        "shares": tail_size / safe_float(sig.get("entry_price")) if sig.get("entry_price") else 0,
        "opened_at": now_iso,
        "opened_ts": dt.datetime.now(dt.timezone.utc).timestamp(),
        "whale_address": (sig.get("whale") or {}).get("address"),
        "mode": CFG.mode,
    }

    if CFG.mode == "live":
        try:
            order_result = submit_live_order(pos)
            pos["order_id"] = order_result.get("id")
            pos["tx_hash"] = order_result.get("tx_hash")
        except Exception as exc:
            log.exception("Live order submission failed: %s", exc)
            discord.notify_raw(f"⚠️ Live order FAILED for {pos['market_title']}: {exc}")
            return None  # don't record a failed position

    p["open_positions"].append(pos)
    p["bankroll"] -= tail_size  # reserve the capital
    p["total_entries_taken"] = p.get("total_entries_taken", 0) + 1
    save_portfolio(p)
    log.info("OPENED (%s) %s %s @ %.3f size $%.2f  market=%s",
             CFG.mode, pos["side"], pos["market_title"][:40] if pos["market_title"] else "?",
             pos["entry_price"], pos["size_usdc"], pos["market_id"][:12])
    return pos


def submit_live_order(pos: dict) -> dict:
    """Place a real order via py_clob_client. Returns {id, tx_hash}."""
    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import OrderArgs, OrderType
        from py_clob_client.order_builder.constants import BUY
    except ImportError as exc:
        raise RuntimeError("py-clob-client not installed. `pip install py-clob-client`") from exc

    pk = os.environ.get("POLYMARKET_PRIVATE_KEY")
    funder = os.environ.get("POLYMARKET_FUNDER_ADDRESS")
    if not pk or not funder:
        raise RuntimeError("POLYMARKET_PRIVATE_KEY and POLYMARKET_FUNDER_ADDRESS must be set")

    host = "https://clob.polymarket.com"
    client = ClobClient(host, key=pk, chain_id=137, funder=funder, signature_type=1)
    client.set_api_creds(client.create_or_derive_api_creds())

    token_id = pos.get("asset_id") or pos.get("token_id")
    if not token_id:
        # We may need to resolve token_id from market_id + outcome_idx.
        # For now, require signal to include it. TODO: resolve via /markets.
        raise RuntimeError(f"No token_id on position for market {pos['market_id']}")

    order = client.create_and_post_order(OrderArgs(
        token_id=token_id,
        price=pos["entry_price"],
        size=pos["shares"],
        side=BUY,
    ), order_type=OrderType.GTC)
    return {"id": order.get("orderID"), "tx_hash": order.get("transactionHash")}


# ===========================================================================
# Exit logic
# ===========================================================================

def get_current_price(client, condition_id: str, outcome_idx: int) -> float | None:
    """Same helper as signal_detector."""
    return signal_detector.fetch_current_price(client, condition_id, outcome_idx)


def check_exits(p: dict) -> list[dict]:
    """Return list of newly-closed trades."""
    if not p["open_positions"]:
        return []
    gamma = signal_detector.ApiClient(
        signal_detector.CFG.gamma_base,
        rate_limit=(signal_detector.CFG.rate_calls, signal_detector.CFG.rate_window_s),
    )
    now_ts = dt.datetime.now(dt.timezone.utc).timestamp()
    still_open = []
    closed = []
    for pos in p["open_positions"]:
        current_price = get_current_price(gamma, pos["market_id"], pos["outcome_idx"])
        if current_price is None:
            still_open.append(pos)
            continue
        entry = pos["entry_price"]
        shares = pos["shares"]
        unrealized_usdc = (current_price - entry) * shares
        unrealized_pct = (current_price / entry - 1.0) if entry > 0 else 0.0
        age_hours = (now_ts - pos["opened_ts"]) / 3600
        age_days  = age_hours / 24

        exit_reason = None
        if unrealized_pct <= CFG.stop_loss_pct:
            exit_reason = f"stop-loss ({unrealized_pct*100:.1f}%)"
        elif unrealized_pct >= CFG.take_profit_pct:
            exit_reason = f"take-profit (+{unrealized_pct*100:.1f}%)"
        elif age_days >= CFG.max_hold_days:
            exit_reason = f"max-hold ({age_days:.1f}d)"

        if exit_reason is None:
            still_open.append(pos)
            continue

        # Close position
        if CFG.mode == "live":
            try:
                submit_live_close(pos, current_price)
            except Exception as exc:
                log.exception("Live close failed: %s", exc)
                discord.notify_raw(f"⚠️ Live EXIT FAILED for {pos['market_title']}: {exc}")
                still_open.append(pos)
                continue

        pnl_usdc = unrealized_usdc
        pnl_pct = unrealized_pct * 100
        proceeds = pos["size_usdc"] + pnl_usdc
        p["bankroll"] += proceeds
        p["daily_pnl"] += pnl_usdc
        closed_trade = dict(pos)
        closed_trade["exit_price"] = current_price
        closed_trade["exit_reason"] = exit_reason
        closed_trade["pnl_usdc"] = round(pnl_usdc, 4)
        closed_trade["pnl_pct"]  = round(pnl_pct, 4)
        closed_trade["closed_at"] = dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")
        closed_trade["held_hours"] = round(age_hours, 2)
        p["closed_trades"].append(closed_trade)
        closed.append(closed_trade)
        log.info("CLOSED %s @ %.3f  P&L $%.2f (%.1f%%)  reason=%s",
                 pos["market_id"][:12], current_price, pnl_usdc, pnl_pct, exit_reason)

    p["open_positions"] = still_open
    save_portfolio(p)
    return closed


def submit_live_close(pos: dict, _current_price: float) -> None:
    """Placeholder — close position via CLOB sell. Implement when going live."""
    raise NotImplementedError("Live exit not yet implemented; run in paper mode")


# ===========================================================================
# Main loop
# ===========================================================================

def process_cycle(p: dict) -> None:
    # 1. Refresh signals
    try:
        signal_detector.run()
    except Exception as exc:
        log.warning("signal_detector failed: %s", exc)

    live = read_json(DATA_DIR / "live_signals.json", default={})
    signals = (live.get("signals") if isinstance(live, dict) else []) or []
    p["total_signals_processed"] = p.get("total_signals_processed", 0) + len(signals)

    # 2. Check exits first (frees up capital)
    closed = check_exits(p)
    for t in closed:
        discord.notify_exit(
            position=t, reason=t["exit_reason"],
            pnl_usdc=t["pnl_usdc"], pnl_pct=t["pnl_pct"],
            mode=CFG.mode,
        )

    # 3. Halt gates
    if HALT_PATH.exists():
        log.info("HALT file present — skipping new entries")
        return
    can_open, why = portfolio_can_open(p)
    if not can_open:
        log.info("portfolio_can_open: false — %s", why)
        # Only halt permanently on bankroll floor
        if "below floor" in why and not p.get("halted"):
            p["halted"] = True
            p["halt_reason"] = why
            save_portfolio(p)
            discord.notify_halt(why, p["bankroll"])
        return

    # 4. New entries
    entries_taken = 0
    for sig in signals:
        mid  = sig.get("market_id"); side = sig.get("side")
        if not mid or not side:
            continue
        if already_in_position(p, mid, side):
            continue
        passed, reasons = signal_passes_strict_filters(sig)
        if not passed:
            discord.notify_skip(sig, "; ".join(reasons))
            continue

        can_open_again, why = portfolio_can_open(p)
        if not can_open_again:
            break

        size = position_size_for(p)
        if size < 1.0:
            break

        pos = open_position(p, sig, size)
        if pos is not None:
            entries_taken += 1
            discord.notify_entry(sig, size, CFG.mode, p["bankroll"])


def main() -> int:
    log.info("=== trade_executor starting ===")
    log.info("mode=%s bankroll=$%.2f max_concurrent=%d filters(drift=%.0f%% entry=[%.2f,%.2f] whale_min=$%.0f)",
             CFG.mode, CFG.starting_bankroll, CFG.max_concurrent,
             CFG.max_drift*100, CFG.min_entry_price, CFG.max_entry_price, CFG.min_whale_size_usdc)
    if CFG.mode == "live":
        if not os.environ.get("POLYMARKET_PRIVATE_KEY"):
            log.error("LIVE mode requires POLYMARKET_PRIVATE_KEY in env — refusing to start")
            discord.notify_raw("⚠️ trade_executor refused to start: LIVE mode but no wallet key in env")
            return 1
        log.warning("!!! LIVE MODE — real orders will be placed !!!")

    last_heartbeat = 0.0
    while True:
        try:
            p = load_portfolio()
            process_cycle(p)

            now = time.time()
            if now - last_heartbeat > CFG.heartbeat_hours * 3600:
                live = read_json(DATA_DIR / "live_signals.json", default={})
                sigs = (live.get("signals") if isinstance(live, dict) else []) or []
                discord.notify_heartbeat(p, len(sigs), p.get("total_entries_taken", 0))
                last_heartbeat = now
        except KeyboardInterrupt:
            log.info("interrupted")
            return 0
        except Exception as exc:
            log.exception("cycle crashed: %s", exc)
            discord.notify_raw(f"⚠️ trade_executor cycle crashed: {exc}\n```{traceback.format_exc()[-500:]}```")

        time.sleep(CFG.loop_interval_seconds)


if __name__ == "__main__":
    sys.exit(main())
