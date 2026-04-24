"""Shared utilities for the Polymarket whale tracker pipeline.

Provides:
    - Logging setup
    - Rate limited / retrying HTTP session
    - USDC decimal normalization
    - EIP-55 checksum address formatting
    - Market type classifier
    - JSON read/write helpers with atomic writes
"""

from __future__ import annotations

import json
import logging
import os
import random
import re
import tempfile
import time
from pathlib import Path
from typing import Any, Iterable

import requests
from requests.adapters import HTTPAdapter

try:
    from eth_utils import to_checksum_address as _eth_to_checksum
except Exception:  # pragma: no cover - fall back silently if eth_utils missing
    _eth_to_checksum = None


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

ROOT_DIR = Path(__file__).resolve().parent.parent
# Data is written into docs/ so it ships with GitHub Pages as siblings of
# index.html. The frontend fetches via plain relative "data/*.json".
DATA_DIR = ROOT_DIR / "docs" / "data"
SCRIPTS_DIR = ROOT_DIR / "scripts"
LOG_PATH = SCRIPTS_DIR / "pipeline.log"
CACHE_DIR = DATA_DIR / ".cache"

DATA_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def setup_logging(name: str = "polymarket", level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(level)
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    fh = logging.FileHandler(LOG_PATH, encoding="utf-8")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(sh)
    logger.propagate = False
    return logger


log = setup_logging()


# ---------------------------------------------------------------------------
# HTTP session with retries + rate limiting
# ---------------------------------------------------------------------------

class RateLimiter:
    """Simple token-bucket style limiter: at most N calls per window seconds."""

    def __init__(self, calls: int = 5, per_seconds: float = 1.0) -> None:
        self.calls = max(1, int(calls))
        self.per = float(per_seconds)
        self._timestamps: list[float] = []

    def wait(self) -> None:
        now = time.monotonic()
        cutoff = now - self.per
        self._timestamps = [t for t in self._timestamps if t > cutoff]
        if len(self._timestamps) >= self.calls:
            sleep_for = self.per - (now - self._timestamps[0])
            if sleep_for > 0:
                time.sleep(sleep_for)
        self._timestamps.append(time.monotonic())


class ApiClient:
    """Thin wrapper around requests.Session with retry + rate limiting."""

    def __init__(
        self,
        base_url: str = "",
        rate_limit: tuple[int, float] = (5, 1.0),
        max_retries: int = 5,
        timeout: float = 20.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.mount("https://", HTTPAdapter(pool_connections=10, pool_maxsize=20))
        self.session.headers.update({
            "User-Agent": "polymarket-whale-tracker/1.0",
            "Accept": "application/json",
        })
        self.limiter = RateLimiter(*rate_limit)
        self.max_retries = max_retries
        self.timeout = timeout

    def get(self, path: str, params: dict | None = None) -> Any:
        url = path if path.startswith("http") else f"{self.base_url}{path}"
        last_exc: Exception | None = None
        for attempt in range(self.max_retries):
            self.limiter.wait()
            try:
                resp = self.session.get(url, params=params, timeout=self.timeout)
                if resp.status_code == 429:
                    delay = _backoff(attempt) + _parse_retry_after(resp)
                    log.warning("429 from %s, sleeping %.1fs", url, delay)
                    time.sleep(delay)
                    continue
                if 500 <= resp.status_code < 600:
                    delay = _backoff(attempt)
                    log.warning("%s from %s, retry in %.1fs", resp.status_code, url, delay)
                    time.sleep(delay)
                    continue
                resp.raise_for_status()
                if not resp.content:
                    return None
                return resp.json()
            except (requests.Timeout, requests.ConnectionError) as exc:
                last_exc = exc
                delay = _backoff(attempt)
                log.warning("Network error on %s (%s), retry in %.1fs", url, exc, delay)
                time.sleep(delay)
            except ValueError as exc:  # JSON decode
                last_exc = exc
                log.warning("Invalid JSON from %s: %s", url, exc)
                return None
            except requests.HTTPError as exc:
                # 4xx other than 429 - don't retry, log at debug to avoid
                # noise when callers fall back to an alternate endpoint.
                log.debug("HTTP error on %s: %s", url, exc)
                return None
        log.error("Exhausted retries on %s: %s", url, last_exc)
        return None


def _backoff(attempt: int, base: float = 0.5, cap: float = 15.0) -> float:
    return min(cap, base * (2 ** attempt)) + random.random() * 0.25


def _parse_retry_after(resp: requests.Response) -> float:
    val = resp.headers.get("Retry-After")
    if not val:
        return 0.0
    try:
        return float(val)
    except ValueError:
        return 0.0


# ---------------------------------------------------------------------------
# USDC normalization
# ---------------------------------------------------------------------------

USDC_DECIMALS = 6
USDC_SCALE = 10 ** USDC_DECIMALS


def usdc_from_raw(raw: Any) -> float:
    """Convert a raw on-chain USDC integer (6 decimals) into a float USDC value."""
    if raw is None:
        return 0.0
    try:
        return float(raw) / USDC_SCALE
    except (TypeError, ValueError):
        return 0.0


def usdc_maybe(value: Any) -> float:
    """Best-effort USDC coercion.

    Polymarket APIs return sizes sometimes as raw integer strings (on-chain units)
    and sometimes as human-readable floats. Heuristic: if value looks like an
    integer string with many digits, treat as raw; otherwise treat as float.
    """
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        f = float(value)
        # anything over 1e9 is almost certainly raw on-chain units
        return f / USDC_SCALE if f > 1e9 else f
    s = str(value).strip()
    if not s:
        return 0.0
    if s.isdigit() and len(s) >= 7:
        return usdc_from_raw(s)
    try:
        return float(s)
    except ValueError:
        return 0.0


# ---------------------------------------------------------------------------
# Address checksumming
# ---------------------------------------------------------------------------

_ADDR_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")


def is_address(value: Any) -> bool:
    return isinstance(value, str) and bool(_ADDR_RE.match(value))


def to_checksum(addr: str) -> str:
    """Return EIP-55 checksummed form; fallback to lowercase if keccak unavailable."""
    if not is_address(addr):
        return addr
    if _eth_to_checksum is not None:
        try:
            return _eth_to_checksum(addr)
        except Exception:
            pass
    return addr.lower()


# ---------------------------------------------------------------------------
# Market type classification
# ---------------------------------------------------------------------------

_MARKET_KEYWORDS: list[tuple[str, tuple[str, ...]]] = [
    ("politics", ("election", "president", "senate", "congress", "gop", "democrat",
                  "republican", "primary", "vote", "campaign", "biden", "trump",
                  "harris", "parliament", "governor", "mayor", "political")),
    ("crypto", ("bitcoin", "btc", "ethereum", "eth", "solana", "sol", "crypto",
                "coinbase", "binance", "ripple", "xrp", "stablecoin", "defi",
                "ether", "altcoin", "memecoin")),
    ("sports", ("nba", "nfl", "mlb", "nhl", "ufc", "soccer", "football", "basketball",
                "baseball", "hockey", "tennis", "golf", "olympics", "world cup",
                "super bowl", "champion", "playoffs", "fifa", "wimbledon")),
    ("economics", ("inflation", "cpi", "fed", "fomc", "interest rate", "recession",
                   "gdp", "jobs report", "unemployment", "treasury")),
    ("tech", ("openai", "tesla", "apple", "google", "microsoft", "meta", "amazon",
              "nvidia", "ai ", "ipo", "earnings", "launch")),
    ("entertainment", ("oscar", "grammy", "emmy", "box office", "movie", "album",
                       "song", "tv show", "netflix")),
    ("geopolitics", ("russia", "ukraine", "china", "israel", "iran", "nato",
                     "war", "ceasefire", "invasion", "sanction")),
]


def classify_market(title: str | None) -> str:
    if not title:
        return "other"
    t = title.lower()
    for label, keywords in _MARKET_KEYWORDS:
        for kw in keywords:
            if kw in t:
                return label
    return "other"


# ---------------------------------------------------------------------------
# JSON helpers
# ---------------------------------------------------------------------------

def read_json(path: Path | str, default: Any = None) -> Any:
    p = Path(path)
    if not p.exists():
        return default
    try:
        with p.open("r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as exc:
        log.warning("Failed to read %s: %s", p, exc)
        return default


def write_json(path: Path | str, data: Any) -> None:
    """Atomic JSON write using a tempfile rename."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".tmp_", suffix=".json", dir=str(p.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2, default=str)
        os.replace(tmp, p)
    finally:
        if os.path.exists(tmp):
            try:
                os.unlink(tmp)
            except OSError:
                pass


def chunked(seq: Iterable, size: int) -> Iterable[list]:
    batch: list = []
    for item in seq:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default
