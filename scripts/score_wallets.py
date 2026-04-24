"""Polymarket whale tracker - scoring & ranking.

Consumes data/whales_raw.json (produced by fetch_whales.py) and produces the
leaderboard at data/whales.json.

Scoring weights are exposed at the top of the file for easy tuning.
"""

from __future__ import annotations

import datetime as dt
import math
import sys
from dataclasses import dataclass
from typing import Any

import numpy as np

from utils import (
    DATA_DIR,
    log,
    read_json,
    safe_float,
    write_json,
)


# ===========================================================================
# CONFIG
# ===========================================================================

@dataclass
class ScoringConfig:
    # Eligibility — real resolved history required
    min_resolved_markets: int = 8
    min_total_volume_usdc: float = 2_000.0
    min_account_age_days: int = 30
    require_positive_pnl: bool = True   # only surface profitable wallets

    # Output size
    top_n: int = 50

    # Weights (must sum to 1.0) — dominated by real PnL
    weight_pnl: float = 0.40           # absolute dollars made
    weight_roi: float = 0.25           # capital efficiency
    weight_win_rate: float = 0.20      # hit consistency
    weight_resolved_volume: float = 0.15  # breadth / sample size

    # Absolute PnL anchor — $250k across positions → 100
    pnl_anchor_usdc: float = 250_000.0

    # ROI normalization
    roi_cap: float = 2.0    # +200% → 100
    roi_floor: float = -0.5 # -50% → 0

    # Resolved-volume breadth anchor — 40 resolved positions → 100
    resolved_count_anchor: int = 40


CFG = ScoringConfig()


# ===========================================================================
# Helpers
# ===========================================================================

def _clamp(x: float, lo: float = 0.0, hi: float = 100.0) -> float:
    if math.isnan(x) or math.isinf(x):
        return lo
    return max(lo, min(hi, x))


def _normalize_roi(roi: float) -> float:
    span = CFG.roi_cap - CFG.roi_floor
    if span <= 0:
        return 0.0
    return _clamp(((roi - CFG.roi_floor) / span) * 100.0)


def _normalize_pnl(pnl_usdc: float) -> float:
    """Signed log-scaling of realized P&L against the anchor.

    $0 → 50 (neutral). Positive grows toward 100 on a log curve. Negative
    drops toward 0. A wallet at the anchor ($250k) scores ~95.
    """
    if pnl_usdc == 0:
        return 50.0
    anchor = max(1.0, CFG.pnl_anchor_usdc)
    mag = math.log10(abs(pnl_usdc) + 1) / math.log10(anchor + 1)
    mag = min(1.0, mag)
    return _clamp(50.0 + (mag * 50.0 if pnl_usdc > 0 else -mag * 50.0))


def _normalize_win_rate(win_rate: float, resolved: int) -> float:
    if resolved <= 0:
        return 0.0
    # Shrinkage toward 0.5 so small samples can't dominate.
    wins = win_rate * resolved
    effective = (wins + 5.0) / (resolved + 10.0)
    return _clamp(effective * 100.0)


def _normalize_resolved_volume(resolved: int) -> float:
    if resolved <= 0:
        return 0.0
    anchor = max(1, CFG.resolved_count_anchor)
    # sqrt-curve so the first few positions matter most
    return _clamp(math.sqrt(resolved / anchor) * 100.0)


# ===========================================================================
# Main scoring
# ===========================================================================

def score_wallet(wallet: dict) -> dict:
    total_pnl  = safe_float(wallet.get("total_pnl_usdc"))
    realized   = safe_float(wallet.get("realized_pnl_usdc"))
    capital    = safe_float(wallet.get("capital_deployed_usdc"))
    overall_roi = safe_float(wallet.get("overall_roi"))
    win_rate   = safe_float(wallet.get("win_rate"))
    resolved   = int(wallet.get("resolved_markets") or 0)

    pnl_score       = _normalize_pnl(total_pnl)
    roi_score       = _normalize_roi(overall_roi)
    win_score       = _normalize_win_rate(win_rate, resolved)
    breadth_score   = _normalize_resolved_volume(resolved)

    final = (
        pnl_score      * CFG.weight_pnl
        + roi_score    * CFG.weight_roi
        + win_score    * CFG.weight_win_rate
        + breadth_score * CFG.weight_resolved_volume
    )

    return {
        "address": wallet["address"],
        "final_score": round(final, 2),
        "pnl_score": round(pnl_score, 2),
        "roi_score": round(roi_score, 2),
        "win_rate_score": round(win_score, 2),
        "breadth_score": round(breadth_score, 2),
        # Real metrics
        "total_pnl_usdc": total_pnl,
        "realized_pnl_usdc": realized,
        "capital_deployed_usdc": capital,
        "overall_roi": overall_roi,
        "win_rate": win_rate,
        "wins": int(wallet.get("wins") or 0),
        "losses": int(wallet.get("losses") or 0),
        "resolved_markets": resolved,
        "markets_participated": int(wallet.get("markets_participated") or 0),
        "total_volume_usdc": safe_float(wallet.get("total_volume_usdc")),
        "avg_position_size_usdc": safe_float(wallet.get("avg_position_size_usdc")),
        "account_age_days": safe_float(wallet.get("account_age_days")),
        "category_breakdown": wallet.get("category_breakdown") or {},
        "open_positions": wallet.get("open_positions") or [],
        "resolved_positions": wallet.get("resolved_positions") or [],
        "recent_trades": wallet.get("recent_trades") or [],
        "label": None,
        # Back-compat fields (legacy frontend expects these names)
        "average_roi": overall_roi,
        "calibration_score": round(win_score, 2),
        "consistency_score": round(win_score, 2),
        "volume_score": round(breadth_score, 2),
        "early_entry_score": round(pnl_score, 2),
    }


def is_eligible(wallet: dict) -> bool:
    if int(wallet.get("resolved_markets") or 0) < CFG.min_resolved_markets:
        return False
    if safe_float(wallet.get("total_volume_usdc")) < CFG.min_total_volume_usdc:
        return False
    if safe_float(wallet.get("account_age_days")) < CFG.min_account_age_days:
        return False
    if CFG.require_positive_pnl and safe_float(wallet.get("total_pnl_usdc")) <= 0:
        return False
    return True


def run() -> int:
    total_w = (CFG.weight_pnl + CFG.weight_roi + CFG.weight_win_rate
               + CFG.weight_resolved_volume)
    if not math.isclose(total_w, 1.0, abs_tol=1e-6):
        log.warning("Scoring weights sum to %.4f, expected 1.0", total_w)

    raw = read_json(DATA_DIR / "whales_raw.json", default={})
    wallets = raw.get("wallets") if isinstance(raw, dict) else None
    if not wallets:
        log.error("No wallets found in whales_raw.json - did fetch_whales run?")
        write_json(DATA_DIR / "whales.json", {
            "updated_at": dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
            "wallets": [],
            "note": "no wallet data available",
        })
        return 0

    log.info("Scoring %d wallets", len(wallets))

    # Strict eligibility — no fallback. If no wallets have real resolved-market
    # history, the output is empty and the dashboard tells the truth.
    eligible = [w for w in wallets if is_eligible(w)]
    ineligible_ct = len(wallets) - len(eligible)
    if ineligible_ct:
        log.info(
            "Excluded %d wallets: failed filters (min_resolved=%d, min_vol=%s, min_age=%dd)",
            ineligible_ct, CFG.min_resolved_markets,
            CFG.min_total_volume_usdc, CFG.min_account_age_days,
        )

    scored = [score_wallet(w) for w in eligible]
    scored.sort(key=lambda w: w["final_score"], reverse=True)
    top = scored[: CFG.top_n]
    for i, w in enumerate(top, start=1):
        w["rank"] = i

    output: dict[str, Any] = {
        "updated_at": dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
        "scoring_weights": {
            "pnl": CFG.weight_pnl,
            "roi": CFG.weight_roi,
            "win_rate": CFG.weight_win_rate,
            "resolved_volume": CFG.weight_resolved_volume,
        },
        "filters": {
            "min_resolved_markets": CFG.min_resolved_markets,
            "min_total_volume_usdc": CFG.min_total_volume_usdc,
            "min_account_age_days": CFG.min_account_age_days,
            "require_positive_pnl": CFG.require_positive_pnl,
        },
        "wallets": top,
    }
    write_json(DATA_DIR / "whales.json", output)
    log.info("Wrote %d ranked wallets to whales.json", len(top))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(run())
    except Exception as exc:
        log.exception("score_wallets crashed: %s", exc)
        sys.exit(1)
