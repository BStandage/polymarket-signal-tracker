# Polymarket Whale Tracker

A static, zero-backend dashboard that ranks the top Polymarket wallets by
forecasting skill. A Python pipeline scheduled in GitHub Actions refreshes
`data/*.json` every two hours; a vanilla-JS frontend served from
`docs/` renders a sortable leaderboard, per-whale radar chart, and
market heatmap on top of that data.

## What it does

1. Pulls active markets from the Polymarket Gamma API
2. Scans recent trades on each market and flags wallets that made
   any trade ≥ `whale_trade_min_usdc` (default **500 USDC**)
3. For each candidate, pulls their full trade / position / activity
   history across all markets
4. Aggregates per-wallet metrics: volume, markets, win-rate, ROI,
   entry timing, category breakdown, current open positions
5. Scores and ranks the top 50 wallets
6. Writes static JSON that a dark-themed dashboard consumes in the browser

## Scoring

Each wallet gets five sub-scores normalized to 0–100, combined into a
weighted composite:

| Metric              | Weight | What it captures |
|---------------------|--------|------------------|
| `roi_score`         | 30%    | Average realized ROI across resolved markets. Linear from `roi_floor` to `roi_cap` (defaults: −50% → 0, +100% → 100). |
| `calibration_score` | 25%    | Brier-style score: when the wallet expressed a view via price + ROI, how often did it match the resolution? Falls back to shrunken win-rate if trade-level PnL is missing. |
| `consistency_score` | 20%    | Win-rate across resolved markets, shrunken toward 0.5 so 1/1 winners can't game the leaderboard. |
| `volume_score`      | 10%    | log10-scaled total volume against a "top trader" anchor (default 500k USDC). |
| `early_entry_score` | 15%    | Average hours between market creation and this wallet's trade, linearly rewarded out to 7 days ahead. |

`final_score = Σ weight_i * metric_i`

Wallets are **filtered out** before scoring if they have:
- fewer than 5 resolved markets, or
- under 1,000 USDC lifetime volume, or
- an account age under 30 days

All weights, thresholds, and anchors live at the top of
[scripts/score_wallets.py](scripts/score_wallets.py) in the `ScoringConfig`
dataclass, and discovery parameters (market count, whale threshold,
rate limits, cache TTL) live in `Config` at the top of
[scripts/fetch_whales.py](scripts/fetch_whales.py). Adjust either without
touching the logic below.

## Project layout

```
.github/workflows/update_data.yml   # cron every 2h + manual dispatch
scripts/
  fetch_whales.py        # discovery + history pipeline
  score_wallets.py       # metric scoring + ranking
  utils.py               # rate limiting, retries, helpers
  requirements.txt
docs/                    # GitHub Pages root
  index.html
  style.css
  app.js
  media/whale_shark.mp4  # optional background footage (not in repo)
  data/
    whales.json          # ranked leaderboard  (consumed by frontend)
    markets.json         # active market snapshot
    last_updated.json    # metadata + timestamps
    whales_raw.json      # pre-scoring aggregates (gitignored)
    .cache/              # per-market / per-wallet caches (gitignored)
```

## Running locally

```bash
python -m venv .polymarket
.polymarket/Scripts/activate      # Windows
# source .polymarket/bin/activate # macOS/Linux
pip install -r scripts/requirements.txt

python scripts/fetch_whales.py    # writes data/whales_raw.json + markets.json
python scripts/score_wallets.py   # writes data/whales.json

# serve docs/ locally to preview the frontend (any static server works)
python -m http.server --directory docs 8000
# -> http://localhost:8000
```

Logs go to `scripts/pipeline.log`. Intermediate per-market trade pulls and
per-wallet histories are cached under `data/.cache/` with a 2-hour TTL,
so a crashed run can resume from where it left off.

## Fork and self-host

1. Fork this repo
2. In **Settings → Pages**, set Source to **Deploy from branch**,
   branch `main`, folder `/docs`. The dashboard will live at
   `https://<your-user>.github.io/<repo-name>/`.
3. The `Update whale data` workflow runs every 2 hours on GitHub's
   schedule and commits refreshed JSON back to `main`. Grant it
   write access under **Settings → Actions → General → Workflow permissions
   → Read and write permissions** (required for the bot to push the
   refreshed data commit).
4. To run it on demand, open **Actions → Update whale data → Run workflow**.

### Tuning

- **Whale threshold** — edit `Config.whale_trade_min_usdc` in
  [scripts/fetch_whales.py](scripts/fetch_whales.py).
- **Wallet depth** — `Config.max_wallets_to_analyze` caps how many
  candidates get deep-dived per run (trade-off: runtime vs coverage).
- **Scoring weights** — edit the `weight_*` fields in `ScoringConfig`
  in [scripts/score_wallets.py](scripts/score_wallets.py). They should
  sum to 1.0.
- **Filters** — `min_resolved_markets`, `min_total_volume_usdc`,
  `min_account_age_days` in `ScoringConfig`.
- **Schedule** — the cron expression in
  [.github/workflows/update_data.yml](.github/workflows/update_data.yml).

## API references

- [Polymarket Gamma API](https://docs.polymarket.com/) — active markets
- [Polymarket CLOB API](https://docs.polymarket.com/#clob-api) — order book / trades
- [Polymarket Data API](https://docs.polymarket.com/#data-api) — positions / activity

## Disclaimer

This dashboard is **for research and entertainment only**. It is not
financial advice. Past performance of any wallet is no guarantee of
future performance, and on-chain addresses can be reused, transferred,
or proxied in ways that may not reflect a single trader. Do your own
research before acting on anything you see here.
