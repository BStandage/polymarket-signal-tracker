"""Discord webhook notifier — rich embeds for trade events.

Usage:
    from discord_notifier import notify_entry, notify_exit, notify_halt

All functions are no-ops if DISCORD_WEBHOOK_URL isn't set in env.
"""

from __future__ import annotations

import os
import time
from typing import Any

import requests

from utils import log

WEBHOOK = os.environ.get("DISCORD_WEBHOOK_URL", "").strip()
POLYMARKET = "https://polymarket.com/event/"

COLOR_ENTER  = 0x4AFF8E  # phosphor green
COLOR_SKIP   = 0x888888  # gray
COLOR_EXIT_W = 0xFFD37A  # yellow (take-profit)
COLOR_EXIT_L = 0xFF6B83  # red (stop-loss)
COLOR_HALT   = 0xFF5F5F  # red (bot halted)


def _post(payload: dict, retries: int = 2) -> bool:
    if not WEBHOOK:
        return False
    for attempt in range(retries + 1):
        try:
            r = requests.post(WEBHOOK, json=payload, timeout=10)
            if r.status_code in (200, 204):
                return True
            if r.status_code == 429:
                time.sleep(float(r.headers.get("Retry-After", 2)))
                continue
            log.warning("Discord webhook %s: %s", r.status_code, r.text[:200])
            return False
        except requests.RequestException as exc:
            log.warning("Discord post failed (attempt %d): %s", attempt + 1, exc)
            time.sleep(1)
    return False


def _signal_fields(signal: dict, whale: dict) -> list[dict]:
    wins = whale.get("total_wins", "?")
    n    = whale.get("n_total", "?")
    wr   = (whale.get("raw_win_rate") or 0) * 100
    rw   = whale.get("recent_wins")
    rn   = whale.get("recent_n")
    recent_str = f"{rw}/{rn}" if rn else "—"
    addr = whale.get("address", "?")
    short = f"{addr[:6]}…{addr[-4:]}" if len(addr) > 10 else addr
    size = signal.get("size_usdc", 0)
    entry = signal.get("entry_price", 0)
    upside = ((1 - entry) / entry * 100) if entry > 0 else 0
    drift = signal.get("drift")
    drift_str = f"{drift*100:+.1f}%" if drift is not None else "—"
    return [
        {"name": "Entry / Size",
         "value": f"**{entry:.3f}** · ${size:,.0f} pos (max +{upside:.0f}% upside)",
         "inline": True},
        {"name": "Drift · Age",
         "value": f"{drift_str} · {signal.get('age_min', 0):.0f}m ago",
         "inline": True},
        {"name": "Whale",
         "value": f"`{short}` · {wins}/{n} ({wr:.0f}%) · 30d {recent_str}",
         "inline": False},
    ]


def notify_entry(signal: dict, tail_size_usdc: float, mode: str, bankroll: float) -> bool:
    """Send a Discord ping when we enter (or paper-enter) a tail position."""
    title = signal.get("market_title") or signal.get("market_id") or "—"
    side  = signal.get("side") or "?"
    slug  = signal.get("market_slug") or ""
    url   = f"{POLYMARKET}{slug}" if slug else None
    mode_tag = "📝 PAPER" if mode == "paper" else "💰 LIVE"

    fields = _signal_fields(signal, signal.get("whale", {}))
    fields.insert(0, {
        "name": "Tail size",
        "value": f"${tail_size_usdc:.2f} (bankroll ${bankroll:.2f})",
        "inline": False,
    })

    embed = {
        "title": f"🟢 {mode_tag} ENTRY · {side}",
        "description": f"**{title}**",
        "url": url,
        "color": COLOR_ENTER,
        "fields": fields,
        "footer": {"text": f"mode: {mode}"},
    }
    return _post({"embeds": [embed]})


def notify_skip(signal: dict, reason: str) -> bool:
    """Lightweight ping when a qualifying-but-filtered signal appears
    (optional — usually noisy, gated by env flag)."""
    if os.environ.get("NOTIFY_SKIPS", "").lower() not in ("1", "true", "yes"):
        return False
    title = signal.get("market_title") or "—"
    embed = {
        "title": "⚪ Skipped",
        "description": f"**{title}** · {reason}",
        "color": COLOR_SKIP,
    }
    return _post({"embeds": [embed]})


def notify_exit(position: dict, reason: str, pnl_usdc: float,
                pnl_pct: float, mode: str) -> bool:
    """Fired when a position closes (paper-closed or real-closed)."""
    color = COLOR_EXIT_W if pnl_usdc > 0 else COLOR_EXIT_L
    emoji = "🟢" if pnl_usdc > 0 else "🔴"
    title = position.get("market_title") or "—"
    side = position.get("side") or "?"
    mode_tag = "📝 PAPER" if mode == "paper" else "💰 LIVE"

    embed = {
        "title": f"{emoji} {mode_tag} EXIT · {side}",
        "description": f"**{title}**",
        "color": color,
        "fields": [
            {"name": "P&L",
             "value": f"**${pnl_usdc:+.2f}** ({pnl_pct:+.1f}%)",
             "inline": True},
            {"name": "Reason", "value": reason, "inline": True},
            {"name": "Entry → Exit",
             "value": f"{position.get('entry_price', 0):.3f} → {position.get('exit_price', 0):.3f}",
             "inline": False},
        ],
        "footer": {"text": f"held {position.get('held_hours', 0):.1f}h · mode: {mode}"},
    }
    return _post({"embeds": [embed]})


def notify_halt(reason: str, bankroll: float) -> bool:
    """Bot halted itself — bankroll floor or daily loss limit hit."""
    embed = {
        "title": "⏸ BOT HALTED",
        "description": reason,
        "color": COLOR_HALT,
        "fields": [
            {"name": "Current bankroll", "value": f"${bankroll:.2f}", "inline": True},
        ],
    }
    return _post({"embeds": [embed]})


def notify_heartbeat(portfolio: dict, signals_seen: int, enters_taken: int) -> bool:
    """Optional daily summary. Gated by env flag so it's opt-in."""
    if os.environ.get("NOTIFY_HEARTBEAT", "").lower() not in ("1", "true", "yes"):
        return False
    open_n = len(portfolio.get("open_positions", []))
    closed = portfolio.get("closed_trades", [])
    total_pnl = sum(t.get("pnl_usdc", 0) for t in closed)
    wins = sum(1 for t in closed if t.get("pnl_usdc", 0) > 0)
    losses = sum(1 for t in closed if t.get("pnl_usdc", 0) < 0)
    embed = {
        "title": "📊 Heartbeat",
        "color": 0xa8c8ff,
        "fields": [
            {"name": "Bankroll", "value": f"${portfolio.get('bankroll', 0):.2f}", "inline": True},
            {"name": "Open", "value": str(open_n), "inline": True},
            {"name": "Total P&L", "value": f"${total_pnl:+.2f}", "inline": True},
            {"name": "Record", "value": f"{wins}W / {losses}L", "inline": True},
            {"name": "Signals seen (cycle)", "value": str(signals_seen), "inline": True},
            {"name": "Entered", "value": str(enters_taken), "inline": True},
        ],
    }
    return _post({"embeds": [embed]})


def notify_raw(text: str) -> bool:
    """Plain text message — used for errors and misc."""
    return _post({"content": text})
