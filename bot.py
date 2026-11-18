#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
 APEX-QUANT-OMNI SOVEREIGN v75.0  --  2-WAY COMMAND EDITION
 Institutional-Grade XAUUSD (Spot Gold) Hybrid Quantitative Signal Engine
================================================================================
 Modules:
   * Multi-Source Free Fallback Data Pipeline (yfinance -> Yahoo Chart API ->
     yfinance 60m -> Yahoo Chart API 60m -> Stooq daily degraded mode)
   * Market Session Governor (Friday close / rollover / Asian-open noise)
   * 5-Pillar Hybrid Engine (SMC, Order Flow, Z-Score, MTF Harmony, ATR Risk)
   * Grid Search Parameter Optimizer (ATR period x Z threshold)
   * Auto-Trailing Stop Engine (Breakeven @ TP1 + fractional ATR trail)
   * SQLite Artifact Persistence + Forensic Audit Logger
   * Exponential Backoff Webhook Delivery (Telegram + Discord)
   * Interactive 2-Way Telegram Command Listener (/check, /status, /help)
   * Headless Matplotlib Backtest Chart (Agg backend)
================================================================================
 Author  : Managing Director of Quantitative Research
 Version : 75.0
 License : Proprietary / Internal
================================================================================
"""

from __future__ import annotations

# ------------------------------------------------------------------------------
# CRITICAL: Headless backend MUST be configured before any pyplot import.
# ------------------------------------------------------------------------------
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

matplotlib.rcParams["figure.autolayout"] = True
matplotlib.rcParams["axes.grid"] = True
matplotlib.rcParams["grid.alpha"] = 0.25

import argparse
import io
import json
import logging
import math
import os
import re
import sqlite3
import sys
import time
import traceback
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from html import unescape
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple
from urllib.parse import quote

import numpy as np
import pandas as pd
import requests
import yfinance as yf

# ==============================================================================
# 1. CONFIGURATION
# ==============================================================================

VERSION = "75.0"
SYSTEM_NAME = "APEX-QUANT-OMNI SOVEREIGN"

DB_PATH = os.environ.get("DB_PATH", "trading_signals.db")
CHART_PATH = os.environ.get("CHART_PATH", "backtest_performance.png")

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "").strip()

ACCOUNT_BALANCE = float(os.environ.get("ACCOUNT_BALANCE", "10000") or 10000)
RISK_PERCENT = float(os.environ.get("RISK_PERCENT", "1.0") or 1.0)
CONTRACT_SIZE = float(os.environ.get("CONTRACT_SIZE", "100") or 100)  # XAUUSD: 100 oz / lot

POLL_SECONDS = int(os.environ.get("POLL_SECONDS", "240") or 240)
NOTIFY_NEUTRAL = os.environ.get("NOTIFY_NEUTRAL", "0").strip() in ("1", "true", "True", "yes")
FORCE_NOTIFY = os.environ.get("FORCE_NOTIFY", "0").strip() in ("1", "true", "True", "yes")

EPS = 1e-8
HTTP_TIMEOUT = 20
MIN_CONFLUENCE = 30.0

Z_WINDOW_DEFAULT = 50
ATR_PERIOD_DEFAULT = 14
ATR_RISK_MULT = 1.5
TP_R_MULTIPLES = (1.5, 2.5, 4.0)

GRID_ATR_PERIODS = (10, 14, 20)
GRID_Z_THRESHOLDS = (1.5, 2.0, 2.5)

UA_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,text/plain,*/*",
}

# ==============================================================================
# 2. LOGGING
# ==============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)sZ | %(levelname)-8s | %(name)-12s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("APEX-OMNI")


# ==============================================================================
# 3. GENERIC UTILITIES
# ==============================================================================


def utcnow() -> datetime:
    """Timezone-aware UTC now."""
    return datetime.now(timezone.utc)


def iso_now() -> str:
    """ISO-8601 UTC timestamp (seconds precision)."""
    return utcnow().replace(microsecond=0).isoformat().replace("+00:00", "Z")


def clamp(value: float, low: float, high: float) -> float:
    """Clamp a float into [low, high]."""
    return max(low, min(high, value))


def safe_float(value: Any, fallback: float = 0.0) -> float:
    """Best-effort numeric coercion that never raises."""
    try:
        out = float(value)
        if math.isnan(out) or math.isinf(out):
            return fallback
        return out
    except (TypeError, ValueError):
        return fallback


def split_message(text: str, limit: int = 3800) -> List[str]:
    """Split a long message on newline boundaries without breaking HTML tags mid-way."""
    if len(text) <= limit:
        return [text]
    chunks: List[str] = []
    current = ""
    for line in text.split("\n"):
        candidate = (current + "\n" + line) if current else line
        if len(candidate) > limit:
            if current:
                chunks.append(current)
            current = line[:limit]
        else:
            current = candidate
    if current:
        chunks.append(current)
    return chunks


def strip_html(text: str) -> str:
    """Remove HTML tags/entities for plain-text Discord delivery."""
    return unescape(re.sub(r"<[^>]+>", "", text))


# ==============================================================================
# 4. SQLITE PERSISTENCE LAYER
# ==============================================================================

DDL_STATEMENTS = (
    """
    CREATE TABLE IF NOT EXISTS signals (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        ts            TEXT    NOT NULL,
        symbol        TEXT    NOT NULL,
        decision      TEXT    NOT NULL,
        entry         REAL,
        sl            REAL,
        tp1           REAL,
        tp2           REAL,
        tp3           REAL,
        confluence    REAL,
        lots          REAL,
        risk_usd      REAL,
        atr           REAL,
        zscore        REAL,
        smc_state     TEXT,
        source        TEXT,
        payload       TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS audit (
        id       INTEGER PRIMARY KEY AUTOINCREMENT,
        ts       TEXT NOT NULL,
        level    TEXT NOT NULL,
        event    TEXT NOT NULL,
        metrics  TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS state (
        key   TEXT PRIMARY KEY,
        value TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS open_positions (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        signal_id  INTEGER,
        direction  TEXT,
        entry      REAL,
        sl         REAL,
        tp1        REAL,
        tp2        REAL,
        tp3        REAL,
        atr        REAL,
        tp1_hit    INTEGER DEFAULT 0,
        last_ts    TEXT,
        status     TEXT DEFAULT 'OPEN',
        updated_ts TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS optimization_runs (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        ts           TEXT,
        atr_period   INTEGER,
        z_threshold  REAL,
        trades       INTEGER,
        win_rate     REAL,
        profit_factor REAL,
        max_drawdown REAL,
        equity_curve TEXT
    )
    """,
)


def db_connect(path: str = DB_PATH) -> sqlite3.Connection:
    """Open (or create) the forensic SQLite database."""
    conn = sqlite3.connect(path, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    """Create all tables if they do not exist."""
    for ddl in DDL_STATEMENTS:
        conn.execute(ddl)
    conn.commit()


def set_state(conn: sqlite3.Connection, key: str, value: str) -> None:
    """Upsert a key/value pair into the state table."""
    conn.execute(
        "INSERT INTO state(key, value) VALUES(?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (str(key), str(value)),
    )
    conn.commit()


def get_state(conn: sqlite3.Connection, key: str, default: Optional[str] = None) -> Optional[str]:
    """Read a key from the state table."""
    row = conn.execute("SELECT value FROM state WHERE key = ?", (str(key),)).fetchone()
    return row["value"] if row else default


def forensic_audit_log(
    conn: sqlite3.Connection,
    event: str,
    metrics: Optional[Dict[str, Any]] = None,
    level: str = "INFO",
) -> None:
    """
    Institutional forensic audit logger.

    Emits the raw metric payload to STDOUT and persists the event into SQLite
    so that the GitHub Actions artifact carries a durable chain of custody.
    """
    metrics = metrics or {}
    payload = json.dumps(metrics, default=str, ensure_ascii=False)

    banner = "=" * 78
    log.log(
        getattr(logging, level.upper(), logging.INFO),
        "\n%s\n[FORENSIC] %s :: %s\n%s\n%s",
        banner,
        iso_now(),
        event,
        payload,
        banner,
    )

    try:
        conn.execute(
            "INSERT INTO audit(ts, level, event, metrics) VALUES(?, ?, ?, ?)",
            (iso_now(), level.upper(), str(event), payload),
        )
        conn.commit()
    except sqlite3.Error as exc:  # pragma: no cover - defensive
        log.error("Audit persistence failure: %s", exc)


def persist_signal(conn: sqlite3.Connection, sig: Dict[str, Any]) -> int:
    """Insert a signal row and return the auto-generated id."""
    cur = conn.execute(
        """
        INSERT INTO signals
            (ts, symbol, decision, entry, sl, tp1, tp2, tp3, confluence,
             lots, risk_usd, atr, zscore, smc_state, source, payload)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            sig["timestamp"],
            sig["symbol"],
            sig["decision"],
            float(sig["entry"]),
            float(sig["sl"]),
            float(sig["tp1"]),
            float(sig["tp2"]),
            float(sig["tp3"]),
            float(sig["confluence"]),
            float(sig["lots"]),
            float(sig["risk_usd"]),
            float(sig["atr"]),
            float(sig["zscore"]),
            str(sig["smc_state"]),
            str(sig["source"]),
            json.dumps(sig, default=str, ensure_ascii=False),
        ),
    )
    conn.commit()
    return int(cur.lastrowid)


def persist_optimization(conn: sqlite3.Connection, row: Dict[str, Any]) -> None:
    """Record one grid-search evaluation for forensic replay."""
    conn.execute(
        """
        INSERT INTO optimization_runs
            (ts, atr_period, z_threshold, trades, win_rate,
             profit_factor, max_drawdown, equity_curve)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            iso_now(),
            int(row["atr_period"]),
            float(row["z_threshold"]),
            int(row["trades"]),
            float(row["win_rate"]),
            float(row["profit_factor"]),
            float(row["max_drawdown"]),
            json.dumps(row.get("curve", [])[:400], default=str),
        ),
    )
    conn.commit()


# ==============================================================================
# 5. EXPONENTIAL BACKOFF HTTP DELIVERY
# ==============================================================================


def http_post_backoff(
    url: str,
    *,
    json_payload: Optional[Dict[str, Any]] = None,
    data: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    max_retries: int = 3,
    base_delay: float = 2.0,
    label: str = "webhook",
) -> Optional[requests.Response]:
    """
    POST with exponential backoff.

    Retries up to `max_retries` times using delays of 2s, 4s, 8s.
    Honors HTTP 429 `Retry-After`. Never raises.
    """
    delays = [base_delay * (2 ** i) for i in range(max_retries)]  # 2, 4, 8
    attempt = 0
    last_error: Optional[Any] = None

    while attempt <= max_retries:
        try:
            resp = requests.post(
                url,
                json=json_payload,
                data=data,
                headers=headers,
                timeout=HTTP_TIMEOUT,
            )
            if 200 <= resp.status_code < 300:
                log.info("[%s] delivered HTTP %s (attempt %d)", label, resp.status_code, attempt + 1)
                return resp

            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After", "")
                wait = delays[attempt] if attempt < len(delays) else delays[-1]
                try:
                    wait = max(wait, float(retry_after))
                except (TypeError, ValueError):
                    pass
                log.warning("[%s] HTTP 429 rate limit -> sleeping %.1fs", label, wait)
                time.sleep(wait)
                attempt += 1
                continue

            if 500 <= resp.status_code < 600:
                last_error = RuntimeError(f"HTTP {resp.status_code}: {resp.text[:200]}")
            else:
                log.error("[%s] non-retryable HTTP %s: %s", label, resp.status_code, resp.text[:300])
                return resp

        except requests.RequestException as exc:
            last_error = exc
            log.warning("[%s] attempt %d transport failure: %s", label, attempt + 1, exc)

        if attempt < max_retries:
            wait = delays[attempt]
            log.info("[%s] exponential backoff -> sleeping %.1fs", label, wait)
            time.sleep(wait)
        attempt += 1

    log.error("[%s] retries exhausted. Last error: %s", label, last_error)
    return None


def tg_api(method: str) -> str:
    """Build a Telegram Bot API endpoint URL."""
    return f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}"


def send_telegram_html(text: str, chat_id: Optional[str] = None) -> bool:
    """Deliver an HTML-formatted Telegram message with backoff + chunking."""
    if not TELEGRAM_BOT_TOKEN:
        log.warning("Telegram token absent -> delivery skipped.")
        return False
    target = chat_id or TELEGRAM_CHAT_ID
    if not target:
        log.warning("Telegram chat id absent -> delivery skipped.")
        return False

    ok = True
    for chunk in split_message(text, 3800):
        resp = http_post_backoff(
            tg_api("sendMessage"),
            json_payload={
                "chat_id": target,
                "text": chunk,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            label="telegram",
        )
        if resp is None or resp.status_code >= 300:
            ok = False
    return ok


def send_discord(text: str) -> bool:
    """Deliver a plain-text Discord webhook message with backoff + chunking."""
    if not DISCORD_WEBHOOK_URL:
        log.info("Discord webhook absent -> delivery skipped.")
        return False

    plain = strip_html(text)
    ok = True
    for chunk in split_message(plain, 1900):
        resp = http_post_backoff(
            DISCORD_WEBHOOK_URL,
            json_payload={"content": chunk, "username": f"{SYSTEM_NAME} v{VERSION}"},
            label="discord",
        )
        if resp is None or resp.status_code >= 300:
            ok = False
    return ok


def send_emergency_alert(error_text: str) -> None:
    """Best-effort emergency notification on catastrophic failure."""
    body = (
        f"🚨 <b>{SYSTEM_NAME} v{VERSION} — CRITICAL FAILURE</b> 🚨\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🕒 <b>Timestamp (UTC):</b> {iso_now()}\n"
        f"🧨 <b>Traceback (tail):</b>\n<pre>{error_text[-1200:]}</pre>"
    )
    try:
        send_telegram_html(body)
    except Exception:  # pragma: no cover
        log.exception("Emergency Telegram alert failed.")
    try:
        send_discord(body)
    except Exception:  # pragma: no cover
        log.exception("Emergency Discord alert failed.")


# ==============================================================================
# 6. MULTI-SOURCE FALLBACK DATA PIPELINE (v75.1 — YAHOO-SAFE)
# ==============================================================================

_last_yahoo_throttle: float = 0.0   # detik epoch saat terakhir kena 429


def _yahoo_soft_backoff() -> None:
    """Jeda ringan sebelum menyentuh Yahoo jika baru saja kena 429."""
    global _last_yahoo_throttle
    if _last_yahoo_throttle <= 0:
        return
    elapsed = time.time() - _last_yahoo_throttle
    if elapsed < 4.0:
        time.sleep(4.0 - elapsed)


def _flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        df = df.copy()
        df.columns = df.columns.get_level_values(0)
    return df


def _finalize_ohlcv(df, min_rows: int = 60):
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return None
    df = _flatten_columns(df).copy()
    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]
    df = df.rename(columns={"adj_close": "close", "price": "close"})
    for col in ("open", "high", "low", "close"):
        if col not in df.columns:
            return None
    if "volume" not in df.columns:
        df["volume"] = 0.0
    df = df[["open", "high", "low", "close", "volume"]]
    df = df.apply(pd.to_numeric, errors="coerce")
    df = df.dropna(subset=["open", "high", "low", "close"])
    if df.empty:
        return None
    if not isinstance(df.index, pd.DatetimeIndex):
        try:
            df.index = pd.to_datetime(df.index, utc=True)
        except Exception:
            return None
    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC")
    else:
        df.index = df.index.tz_convert("UTC")
    df = df[~df.index.duplicated(keep="last")].sort_index()
    df = df[df["high"] >= df["low"]]
    if len(df) < min_rows:
        return None
    return df


def source_yfinance(symbol: str, interval: str, period: str):
    _yahoo_soft_backoff()
    try:
        raw = yf.download(
            symbol, interval=interval, period=period,
            progress=False, auto_adjust=False, threads=False,
        )
    except Exception as exc:
        msg = str(exc)
        if "404" in msg or "delisted" in msg.lower():
            raise RuntimeError(f"symbol {symbol} delisted on Yahoo") from exc
        raise
    return _finalize_ohlcv(raw)


def source_yahoo_chart_api(symbol: str, interval: str, range_: str):
    global _last_yahoo_throttle
    _yahoo_soft_backoff()
    last_error = None
    for host in ("https://query1.finance.yahoo.com", "https://query2.finance.yahoo.com"):
        try:
            url = (
                f"{host}/v8/finance/chart/{quote(symbol, safe='')}"
                f"?interval={interval}&range={range_}&includePrePost=false"
            )
            resp = requests.get(url, headers=UA_HEADERS, timeout=HTTP_TIMEOUT)
            if resp.status_code == 429:
                _last_yahoo_throttle = time.time()
                raise RuntimeError("HTTP 429 rate limit")
            resp.raise_for_status()
            payload = resp.json()
            result = payload["chart"]["result"][0]
            ts = result.get("timestamp") or []
            if not ts:
                raise RuntimeError("empty timestamp array")
            q = result["indicators"]["quote"][0]
            frame = pd.DataFrame(
                {
                    "open": q.get("open"),
                    "high": q.get("high"),
                    "low": q.get("low"),
                    "close": q.get("close"),
                    "volume": q.get("volume") or [0] * len(ts),
                },
                index=pd.to_datetime(ts, unit="s", utc=True),
            )
            cleaned = _finalize_ohlcv(frame)
            if cleaned is not None:
                return cleaned
            raise RuntimeError("insufficient cleaned rows")
        except Exception as exc:
            if "429" in str(exc):
                _last_yahoo_throttle = time.time()
            last_error = exc
            log.warning("Yahoo chart API host %s failed: %s", host, exc)
    if last_error:
        raise last_error
    return None


def source_stooq_daily(symbol: str = "xauusd"):
    """
    Stooq parser defensif: toleran terhadap BOM, header casing, dan
    baris meta. Akan raise jika memang bukan CSV OHLCV valid.
    """
    url = f"https://stooq.com/q/d/l/?s={symbol}&i=d"
    resp = requests.get(url, headers=UA_HEADERS, timeout=HTTP_TIMEOUT)
    resp.raise_for_status()
    text = (resp.text or "").lstrip("\ufeff").strip()

    if not text or "Date" not in text[:400]:
        snippet = text[:200].replace("\n", " ")
        raise RuntimeError(f"stooq unexpected payload: {snippet!r}")

    try:
        raw = pd.read_csv(io.StringIO(text), sep=None, engine="python")
    except Exception as exc:
        raise RuntimeError(f"stooq csv parse failed: {exc}") from exc

    raw.columns = [str(c).strip().lower() for c in raw.columns]
    if "date" not in raw.columns or "close" not in raw.columns:
        raise RuntimeError(f"stooq schema mismatch: {list(raw.columns)[:6]}")

    raw["date"] = pd.to_datetime(raw["date"], utc=True, errors="coerce")
    raw = raw.dropna(subset=["date"]).set_index("date")
    return _finalize_ohlcv(raw, min_rows=60)


# ------------------------------------------------------------------------------
# TIER ORDER (v75.1)
#   1. GC=F futures        -> paling andal di Yahoo, basis ~0.3% vs spot
#   2. PAXG-USD gold token -> track spot dekat, 24/7
#   3. GLD ETF             -> sangat likuid, proxy spot
#   4. XAUUSD=X            -> dicoba lagi kalau Yahoo memulihkan
#   5. Yahoo chart GC=F    -> fallback chart API untuk futures
#   6. Stooq xauusd        -> daily degraded (last resort)
# ------------------------------------------------------------------------------
DATA_SOURCES = (
    ("yfinance-GC=F-15m",         lambda: source_yfinance("GC=F", "15m", "5d"),        False),
    ("yfinance-PAXG-USD-15m",     lambda: source_yfinance("PAXG-USD", "15m", "5d"),    False),
    ("yfinance-GLD-15m",          lambda: source_yfinance("GLD", "15m", "5d"),         False),
    ("yfinance-XAUUSD=X-15m",     lambda: source_yfinance("XAUUSD=X", "15m", "5d"),    False),
    ("yahoo-chart-GC=F-15m",      lambda: source_yahoo_chart_api("GC=F", "15m", "5d"), False),
    ("yahoo-chart-PAXG-USD-15m",  lambda: source_yahoo_chart_api("PAXG-USD", "15m", "5d"), False),
    ("yahoo-chart-XAUUSD=X-15m",  lambda: source_yahoo_chart_api("XAUUSD=X", "15m", "5d"), False),
    ("yfinance-GC=F-60m",         lambda: source_yfinance("GC=F", "60m", "1mo"),       False),
    ("stooq-XAUUSD-1d",           lambda: source_stooq_daily("xauusd"),                True),
)


def fetch_market_data():
    """Probe setiap sumber sampai ada yang mengembalikan OHLCV valid."""
    errors = []
    for name, loader, degraded in DATA_SOURCES:
        started = time.time()
        try:
            df = loader()
            if df is None or len(df) < 60:
                raise RuntimeError("insufficient rows returned")
            elapsed = time.time() - started
            bar_delta = pd.Series(df.index).diff().median()
            log.info(
                "Feed OK -> %s | rows=%d | bar=%s | latency=%.2fs | degraded=%s",
                name, len(df), bar_delta, elapsed, degraded,
            )
            return df, name, degraded
        except Exception as exc:
            errors.append(f"{name}: {exc}")
            log.warning("Feed FAILED -> %s :: %s", name, exc)
    raise RuntimeError("ALL DATA SOURCES EXHAUSTED :: " + " || ".join(errors))


# ==============================================================================
# 7. MARKET SESSION GOVERNOR
# ==============================================================================


def session_governor(moment: Optional[datetime] = None) -> Tuple[bool, List[str]]:
    """
    Validate UTC weekday/time against high-risk execution windows.

    Returns (allowed, list_of_blocking_reasons).
    """
    moment = moment or utcnow()
    weekday = moment.weekday()  # Monday=0 ... Sunday=6
    hour = moment.hour + moment.minute / 60.0
    reasons: List[str] = []

    # Friday institutional close-out (liquidity vacuum + weekend gap risk).
    if weekday == 4 and hour >= 20.5:
        reasons.append("Friday close-out window (>= 20:30 UTC)")

    # Weekend closure.
    if weekday == 5:
        reasons.append("Saturday — market closed")

    # Sunday pre-open dead zone.
    if weekday == 6 and hour < 22.0:
        reasons.append("Sunday pre-open dead zone (< 22:00 UTC)")

    # Daily rollover / swap / spread explosion window.
    if 21.0 <= hour < 22.0:
        reasons.append("Daily rollover window (21:00-22:00 UTC)")

    # Asian open thin-liquidity noise.
    if 0.0 <= hour < 1.0:
        reasons.append("Asian open thin-liquidity noise (00:00-01:00 UTC)")

    return (len(reasons) == 0, reasons)


# ==============================================================================
# 8. INDICATOR PRIMITIVES (WITH ZERO-DIVISION GUARDS)
# ==============================================================================


def atr_series(df: pd.DataFrame, period: int = ATR_PERIOD_DEFAULT) -> pd.Series:
    """Wilder-style simple ATR with an epsilon-denominator guard."""
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close = df["close"].astype(float)
    prev_close = close.shift(1)

    true_range = pd.concat(
        [
            (high - low),
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)

    atr = true_range.rolling(period, min_periods=period).mean()
    return atr.fillna(atr.mean() if not atr.dropna().empty else EPS) + EPS


def zscore_series(close: pd.Series, window: int = Z_WINDOW_DEFAULT) -> pd.Series:
    """Rolling Z-Score with a hard epsilon added to the standard deviation."""
    mean = close.rolling(window, min_periods=window).mean()
    std = close.rolling(window, min_periods=window).std(ddof=0)
    return (close - mean) / (std + EPS)


def ema_series(series: pd.Series, span: int) -> pd.Series:
    """Standard exponential moving average."""
    return series.ewm(span=span, adjust=False).mean()


def resample_htf(df: pd.DataFrame, rule: str = "1h") -> pd.DataFrame:
    """Resample an LTF frame into the higher-timeframe fractal series."""
    ohlc = pd.DataFrame(
        {
            "open": df["open"].resample(rule).first(),
            "high": df["high"].resample(rule).max(),
            "low": df["low"].resample(rule).min(),
            "close": df["close"].resample(rule).last(),
            "volume": df["volume"].resample(rule).sum(),
        }
    )
    return ohlc.dropna(subset=["open", "high", "low", "close"])


# ==============================================================================
# 9. FIVE-PILLAR HYBRID QUANTITATIVE ENGINE
# ==============================================================================


@dataclass
class PillarResult:
    """Container for one pillar's signed directional verdict."""

    name: str
    score: float  # signed: -100 (max bearish) .. +100 (max bullish)
    bias: str  # BULLISH / BEARISH / NEUTRAL
    detail: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.score > 5.0:
            self.bias = "BULLISH"
        elif self.score < -5.0:
            self.bias = "BEARISH"
        else:
            self.bias = "NEUTRAL"


def pillar_smc(df: pd.DataFrame, lookback: int = 3) -> PillarResult:
    """
    PILLAR I — Smart Money Concepts.

    Fractal swing detection, Break-of-Structure (BOS) and
    Change-of-Character (ChoCh) classification.
    """
    highs = df["high"].to_numpy(dtype=float)
    lows = df["low"].to_numpy(dtype=float)
    closes = df["close"].to_numpy(dtype=float)
    n = len(closes)

    if n < (2 * lookback + 5):
        return PillarResult("SMC", 0.0, "NEUTRAL", {"error": "insufficient bars"})

    swing_highs: List[Tuple[int, float]] = []
    swing_lows: List[Tuple[int, float]] = []

    for i in range(lookback, n - lookback):
        window_high = highs[i - lookback: i + lookback + 1]
        window_low = lows[i - lookback: i + lookback + 1]
        if highs[i] >= window_high.max():
            swing_highs.append((i, float(highs[i])))
        if lows[i] <= window_low.min():
            swing_lows.append((i, float(lows[i])))

    if not swing_highs or not swing_lows:
        return PillarResult("SMC", 0.0, "NEUTRAL", {"error": "no confirmed swings"})

    last_sh_idx, last_sh = swing_highs[-1]
    last_sl_idx, last_sl = swing_lows[-1]

    prev_sh = swing_highs[-2][1] if len(swing_highs) > 1 else last_sh
    prev_sl = swing_lows[-2][1] if len(swing_lows) > 1 else last_sl

    atr_value = safe_float(atr_series(df, ATR_PERIOD_DEFAULT).iloc[-1], EPS)
    price = float(closes[-1])

    detail: Dict[str, Any] = {
        "last_swing_high": round(last_sh, 5),
        "last_swing_low": round(last_sl, 5),
        "prev_swing_high": round(prev_sh, 5),
        "prev_swing_low": round(prev_sl, 5),
        "structure": "RANGE",
        "event": "NONE",
    }

    score = 0.0

    if price > last_sh:
        magnitude = clamp((price - last_sh) / (atr_value + EPS), 0.0, 1.0)
        score = 55.0 + 45.0 * magnitude
        detail["structure"] = "BULLISH"
        detail["event"] = "BOS_BULLISH"
        # Change of character: prior structure was making lower highs.
        if last_sh < prev_sh and last_sl < prev_sl:
            score = min(100.0, score + 10.0)
            detail["event"] = "ChOCh_BULLISH"
    elif price < last_sl:
        magnitude = clamp((last_sl - price) / (atr_value + EPS), 0.0, 1.0)
        score = -(55.0 + 45.0 * magnitude)
        detail["structure"] = "BEARISH"
        detail["event"] = "BOS_BEARISH"
        if last_sh > prev_sh and last_sl > prev_sl:
            score = max(-100.0, score - 10.0)
            detail["event"] = "ChOCh_BEARISH"
    else:
        span = max(last_sh - last_sl, EPS)
        position = clamp((price - last_sl) / span, 0.0, 1.0)
        score = (position - 0.5) * 60.0
        detail["structure"] = "RANGE"
        detail["event"] = "RANGE_ROTATION"
        detail["range_position"] = round(position, 4)

    detail["atr"] = round(atr_value, 5)
    return PillarResult("SMC", clamp(score, -100.0, 100.0), "NEUTRAL", detail)


def pillar_order_flow(df: pd.DataFrame, window: int = 20) -> PillarResult:
    """
    PILLAR II — Order Flow.

    Volume-weighted body-to-wick ratio plus rejection-tail pressure.
    """
    tail = df.tail(window)
    if len(tail) < 5:
        return PillarResult("ORDER_FLOW", 0.0, "NEUTRAL", {"error": "insufficient bars"})

    high = tail["high"].to_numpy(dtype=float)
    low = tail["low"].to_numpy(dtype=float)
    open_ = tail["open"].to_numpy(dtype=float)
    close = tail["close"].to_numpy(dtype=float)
    volume = tail["volume"].to_numpy(dtype=float)

    if volume.sum() <= 0:
        volume = np.ones_like(volume)

    bar_range = (high - low) + EPS
    body = close - open_
    upper_wick = high - np.maximum(close, open_)
    lower_wick = np.minimum(close, open_) - low

    body_ratio = np.clip(np.abs(body) / bar_range, 0.0, 1.0)
    directional_pressure = np.sign(body) * body_ratio * volume
    rejection = (lower_wick - upper_wick) / bar_range  # positive = buy-side tail

    total_volume = volume.sum() + EPS
    pressure_component = float(directional_pressure.sum() / total_volume) * 60.0
    rejection_component = float((rejection * volume).sum() / total_volume) * 40.0

    score = clamp(pressure_component + rejection_component, -1.0, 1.0) * 100.0

    detail = {
        "pressure_component": round(pressure_component, 4),
        "rejection_component": round(rejection_component, 4),
        "avg_body_ratio": round(float(body_ratio.mean()), 4),
        "window": window,
    }
    return PillarResult("ORDER_FLOW", clamp(score, -100.0, 100.0), "NEUTRAL", detail)


def pillar_zscore(df: pd.DataFrame, window: int = Z_WINDOW_DEFAULT) -> PillarResult:
    """
    PILLAR III — Z-Score Mean Reversion.

    Negative Z (oversold) scores bullish; positive Z (overbought) scores bearish.
    """
    if len(df) < window + 5:
        return PillarResult("ZSCORE", 0.0, "NEUTRAL", {"error": "insufficient bars"})

    z_series = zscore_series(df["close"].astype(float), window)
    z_last = safe_float(z_series.iloc[-1], 0.0)

    score = clamp(-z_last * 35.0, -100.0, 100.0)
    detail = {
        "zscore": round(z_last, 4),
        "window": window,
        "regime": "OVERSOLD" if z_last <= -1.5 else ("OVERBOUGHT" if z_last >= 1.5 else "BALANCED"),
    }
    return PillarResult("ZSCORE", score, "NEUTRAL", detail)


def pillar_mtf(df: pd.DataFrame, fast: int = 20, slow: int = 50) -> PillarResult:
    """
    PILLAR IV — Multi-Timeframe Fractal Harmony.

    1-Hour macro EMA alignment confirmed against the 15-Min execution trend.
    """
    htf = resample_htf(df, "1h")
    if len(htf) < slow + 2:
        htf = df.copy()
        if len(htf) < slow + 2:
            return PillarResult("MTF", 0.0, "NEUTRAL", {"error": "insufficient HTF bars"})

    ema_fast = ema_series(htf["close"].astype(float), fast)
    ema_slow = ema_series(htf["close"].astype(float), slow)

    fast_last = safe_float(ema_fast.iloc[-1])
    slow_last = safe_float(ema_slow.iloc[-1])
    htf_close = safe_float(htf["close"].iloc[-1])

    htf_atr = safe_float(atr_series(htf, ATR_PERIOD_DEFAULT).iloc[-1], EPS)
    spread = (fast_last - slow_last) / (htf_atr + EPS)

    ltf_fast = safe_float(ema_series(df["close"].astype(float), fast).iloc[-1])
    ltf_slow = safe_float(ema_series(df["close"].astype(float), slow).iloc[-1])

    macro_bull = fast_last > slow_last and htf_close > slow_last
    macro_bear = fast_last < slow_last and htf_close < slow_last
    micro_bull = ltf_fast > ltf_slow
    micro_bear = ltf_fast < ltf_slow

    score = clamp(spread * 40.0, -70.0, 70.0)

    harmony = "FRACTAL_CONFLICT"
    if macro_bull and micro_bull:
        score = clamp(score + 30.0, -100.0, 100.0)
        harmony = "FULL_BULLISH_HARMONY"
    elif macro_bear and micro_bear:
        score = clamp(score - 30.0, -100.0, 100.0)
        harmony = "FULL_BEARISH_HARMONY"
    elif macro_bull and micro_bear:
        harmony = "MACRO_BULL_MICRO_PULLBACK"
    elif macro_bear and micro_bull:
        harmony = "MACRO_BEAR_MICRO_BOUNCE"

    detail = {
        "htf_ema_fast": round(fast_last, 5),
        "htf_ema_slow": round(slow_last, 5),
        "htf_atr": round(htf_atr, 5),
        "spread_atr_units": round(spread, 4),
        "harmony": harmony,
    }
    return PillarResult("MTF", clamp(score, -100.0, 100.0), "NEUTRAL", detail)


def pillar_atr_regime(df: pd.DataFrame, period: int = ATR_PERIOD_DEFAULT) -> PillarResult:
    """
    PILLAR V — ATR Risk & Volatility Regime.

    Non-directional quality gate: returns a signed score of 0 but exposes a
    quality multiplier used to discount confluence in dead or chaotic tape.
    """
    atr_series_full = atr_series(df, period)
    atr_last = safe_float(atr_series_full.iloc[-1], EPS)
    price = safe_float(df["close"].iloc[-1], 1.0)

    recent = atr_series_full.tail(120).dropna()
    if len(recent) >= 20:
        percentile = float((recent <= atr_last).mean())
    else:
        percentile = 0.5

    if percentile < 0.15:
        quality = 0.60
        regime = "COMPRESSED"
    elif percentile > 0.95:
        quality = 0.70
        regime = "EXPLOSIVE"
    else:
        quality = 1.00
        regime = "NOMINAL"

    detail = {
        "atr": round(atr_last, 5),
        "atr_percentile": round(percentile, 4),
        "atr_pct_of_price": round(atr_last / max(price, EPS) * 100.0, 5),
        "regime": regime,
        "quality_multiplier": quality,
    }
    return PillarResult("ATR_REGIME", 0.0, "NEUTRAL", detail)


CONFLUENCE_WEIGHTS: Dict[str, float] = {
    "SMC": 0.30,
    "ORDER_FLOW": 0.20,
    "ZSCORE": 0.30,
    "MTF": 0.20,
}


def compute_confluence(pillars: Sequence[PillarResult]) -> Tuple[float, float, float]:
    """
    Weighted directional confluence.

    Returns (signed_confluence, quality_multiplier, final_score_0_100).
    """
    signed = 0.0
    quality = 1.0
    for pillar in pillars:
        if pillar.name in CONFLUENCE_WEIGHTS:
            signed += pillar.score * CONFLUENCE_WEIGHTS[pillar.name]
        elif pillar.name == "ATR_REGIME":
            quality = float(pillar.detail.get("quality_multiplier", 1.0))

    final = clamp(abs(signed) * quality, 0.0, 100.0)
    return clamp(signed, -100.0, 100.0), quality, final


# ==============================================================================
# 10. RISK, POSITION SIZING & SIGNAL CONSTRUCTION
# ==============================================================================


def build_risk_levels(
    entry: float,
    direction: str,
    atr_value: float,
    atr_mult: float = ATR_RISK_MULT,
) -> Dict[str, float]:
    """Construct SL/TP1/TP2/TP3 from ATR with a guaranteed minimum 1:3 RRR."""
    atr_value = max(atr_value, EPS)
    stop_distance = max(atr_value * atr_mult, EPS)

    if direction == "BUY":
        sl = entry - stop_distance
        tps = [entry + stop_distance * r for r in TP_R_MULTIPLES]
    else:
        sl = entry + stop_distance
        tps = [entry - stop_distance * r for r in TP_R_MULTIPLES]

    return {
        "sl": round(sl, 2),
        "tp1": round(tps[0], 2),
        "tp2": round(tps[1], 2),
        "tp3": round(tps[2], 2),
        "stop_distance": stop_distance,
        "rrr_tp3": TP_R_MULTIPLES[2],
    }


def position_size(
    balance: float,
    risk_pct: float,
    stop_distance_usd: float,
    contract_size: float = CONTRACT_SIZE,
) -> Tuple[float, float]:
    """Return (lots, risk_amount_usd) for a fixed-fractional risk model."""
    risk_amount = max(balance, 0.0) * max(risk_pct, 0.0) / 100.0
    denominator = max(stop_distance_usd * contract_size, EPS)
    lots = risk_amount / denominator
    lots = math.floor(lots * 100.0) / 100.0
    if lots < 0.01:
        lots = 0.01
    lots = min(lots, 100.0)
    realized_risk = lots * stop_distance_usd * contract_size
    return round(lots, 2), round(realized_risk, 2)


def analyze_market(
    df: pd.DataFrame,
    source: str,
    degraded: bool,
) -> Dict[str, Any]:
    """
    Execute the full 5-pillar evaluation and return a forensic analysis payload.
    """
    if df is None or len(df) < 30:
        raise RuntimeError("Insufficient market data for analysis")

    z_window = int(clamp(len(df) // 6, 20, Z_WINDOW_DEFAULT))

    pillars: List[PillarResult] = [
        pillar_smc(df),
        pillar_order_flow(df),
        pillar_zscore(df, window=z_window),
        pillar_mtf(df),
        pillar_atr_regime(df),
    ]

    signed, quality, confluence_score = compute_confluence(pillars)

    decision = "NEUTRAL"
    if confluence_score >= MIN_CONFLUENCE:
        decision = "BUY" if signed > 0 else "SELL"

    entry = safe_float(df["close"].iloc[-1])
    atr_value = safe_float(atr_series(df, ATR_PERIOD_DEFAULT).iloc[-1], EPS)
    z_last = safe_float(zscore_series(df["close"].astype(float), z_window).iloc[-1], 0.0)

    smc_pillar = next(p for p in pillars if p.name == "SMC")

    levels = build_risk_levels(entry, decision if decision != "NEUTRAL" else "BUY", atr_value)
    lots, risk_usd = position_size(ACCOUNT_BALANCE, RISK_PERCENT, levels["stop_distance"])

    analysis: Dict[str, Any] = {
        "timestamp": iso_now(),
        "symbol": "XAUUSD",
        "source": source,
        "degraded_feed": bool(degraded),
        "bars": int(len(df)),
        "last_bar_utc": df.index[-1].isoformat(),
        "entry": round(entry, 2),
        "decision": decision,
        "signed_confluence": round(signed, 2),
        "confluence": round(confluence_score, 2),
        "quality_multiplier": round(quality, 4),
        "atr": round(atr_value, 5),
        "zscore": round(z_last, 4),
        "z_window": z_window,
        "smc_state": smc_pillar.detail.get("event", "UNKNOWN"),
        "sl": levels["sl"],
        "tp1": levels["tp1"],
        "tp2": levels["tp2"],
        "tp3": levels["tp3"],
        "stop_distance": round(levels["stop_distance"], 5),
        "lots": lots,
        "risk_usd": risk_usd,
        "account_balance": ACCOUNT_BALANCE,
        "risk_percent": RISK_PERCENT,
        "pillars": [
            {"name": p.name, "score": round(p.score, 2), "bias": p.bias, "detail": p.detail}
            for p in pillars
        ],
    }
    return analysis


# ==============================================================================
# 11. GRID SEARCH PARAMETER OPTIMIZATION & BACKTESTER
# ==============================================================================


def _open_position(
    direction: str,
    price: float,
    atr_value: float,
    atr_mult: float,
    rr: Sequence[float],
) -> Dict[str, Any]:
    """Create an in-flight backtest position record."""
    stop_distance = max(atr_value * atr_mult, EPS)
    if direction == "BUY":
        sl = price - stop_distance
        tps = [price + stop_distance * r for r in rr]
    else:
        sl = price + stop_distance
        tps = [price - stop_distance * r for r in rr]

    return {
        "direction": direction,
        "entry": price,
        "sl": sl,
        "sl_dist": stop_distance,
        "tp1": tps[0],
        "tp2": tps[1],
        "tp3": tps[2],
        "hit": {"tp1": False, "tp2": False, "tp3": False},
        "remaining": 1.0,
        "realized": 0.0,
    }


def backtest_params(
    df: pd.DataFrame,
    atr_period: int,
    z_threshold: float,
    z_window: int = Z_WINDOW_DEFAULT,
    atr_mult: float = ATR_RISK_MULT,
    rr: Sequence[float] = TP_R_MULTIPLES,
    start_balance: float = ACCOUNT_BALANCE,
    risk_pct: float = RISK_PERCENT,
) -> Dict[str, Any]:
    """
    Deterministic single-parameter backtest used by the grid search optimizer.

    Entries: mean-reversion on |Z| > threshold.
    Exits  : ATR stop, partial TP1/TP2/TP3, breakeven shift after TP1.
    """
    empty = {
        "atr_period": atr_period,
        "z_threshold": z_threshold,
        "trades": 0,
        "wins": 0,
        "losses": 0,
        "win_rate": 0.0,
        "profit_factor": 0.0,
        "max_drawdown": 0.0,
        "net_pnl": 0.0,
        "curve": [round(start_balance, 2)],
        "curve_labels": [iso_now()],
    }

    if df is None or len(df) < max(z_window, atr_period) + 30:
        return empty

    work = pd.DataFrame(
        {
            "open": df["open"].astype(float),
            "high": df["high"].astype(float),
            "low": df["low"].astype(float),
            "close": df["close"].astype(float),
        }
    )
    work["atr"] = atr_series(work, atr_period)
    work["z"] = zscore_series(work["close"], z_window)
    work = work.dropna()

    if len(work) < 30:
        return empty

    closes = work["close"].to_numpy(dtype=float)
    highs = work["high"].to_numpy(dtype=float)
    lows = work["low"].to_numpy(dtype=float)
    atrs = work["atr"].to_numpy(dtype=float)
    zs = work["z"].to_numpy(dtype=float)
    index = work.index

    risk_usd = max(start_balance, 0.0) * max(risk_pct, 0.0) / 100.0

    equity = float(start_balance)
    peak = equity
    max_drawdown = 0.0
    curve: List[float] = [round(equity, 2)]
    curve_labels: List[str] = [index[0].isoformat()]

    trades = 0
    wins = 0
    losses = 0
    gross_profit = 0.0
    gross_loss = 0.0

    position: Optional[Dict[str, Any]] = None

    def _finalize(pos: Dict[str, Any], bar_ts: Any) -> None:
        nonlocal equity, peak, max_drawdown, trades, wins, losses, gross_profit, gross_loss
        r_multiple = pos["realized"]
        pnl = risk_usd * r_multiple
        equity += pnl
        peak = max(peak, equity)
        if peak > 0:
            drawdown = (peak - equity) / peak * 100.0
            max_drawdown = max(max_drawdown, drawdown)
        curve.append(round(equity, 2))
        curve_labels.append(bar_ts.isoformat() if hasattr(bar_ts, "isoformat") else str(bar_ts))
        trades += 1
        if pnl >= 0:
            wins += 1
            gross_profit += pnl
        else:
            losses += 1
            gross_loss += abs(pnl)

    for i in range(len(work)):
        if position is None:
            if zs[i] <= -z_threshold:
                position = _open_position("BUY", closes[i], atrs[i], atr_mult, rr)
            elif zs[i] >= z_threshold:
                position = _open_position("SELL", closes[i], atrs[i], atr_mult, rr)
            continue

        bar_high = highs[i]
        bar_low = lows[i]
        stop_hit = False

        if position["direction"] == "BUY":
            if bar_low <= position["sl"]:
                stop_hit = True
            else:
                for level, fraction in (("tp1", 0.5), ("tp2", 0.3), ("tp3", 0.2)):
                    if not position["hit"][level] and bar_high >= position[level]:
                        position["hit"][level] = True
                        position["realized"] += fraction * (
                            (position[level] - position["entry"]) / position["sl_dist"]
                        )
                        position["remaining"] -= fraction
                        if level == "tp1":
                            position["sl"] = max(position["sl"], position["entry"])
        else:
            if bar_high >= position["sl"]:
                stop_hit = True
            else:
                for level, fraction in (("tp1", 0.5), ("tp2", 0.3), ("tp3", 0.2)):
                    if not position["hit"][level] and bar_low <= position[level]:
                        position["hit"][level] = True
                        position["realized"] += fraction * (
                            (position["entry"] - position[level]) / position["sl_dist"]
                        )
                        position["remaining"] -= fraction
                        if level == "tp1":
                            position["sl"] = min(position["sl"], position["entry"])

        if stop_hit and position["remaining"] > 1e-9:
            if position["direction"] == "BUY":
                r_exit = (position["sl"] - position["entry"]) / position["sl_dist"]
            else:
                r_exit = (position["entry"] - position["sl"]) / position["sl_dist"]
            position["realized"] += position["remaining"] * r_exit
            position["remaining"] = 0.0

        if position["remaining"] <= 1e-9:
            _finalize(position, index[i])
            position = None

    # Force-close any residual exposure at the final bar.
    if position is not None:
        if position["direction"] == "BUY":
            r_exit = (closes[-1] - position["entry"]) / position["sl_dist"]
        else:
            r_exit = (position["entry"] - closes[-1]) / position["sl_dist"]
        position["realized"] += position["remaining"] * r_exit
        position["remaining"] = 0.0
        _finalize(position, index[-1])

    win_rate = (wins / trades * 100.0) if trades else 0.0
    if gross_loss > 0:
        profit_factor = gross_profit / gross_loss
    elif gross_profit > 0:
        profit_factor = 99.0
    else:
        profit_factor = 0.0

    return {
        "atr_period": atr_period,
        "z_threshold": z_threshold,
        "trades": trades,
        "wins": wins,
        "losses": losses,
        "win_rate": round(win_rate, 2),
        "profit_factor": round(min(profit_factor, 99.0), 4),
        "max_drawdown": round(max_drawdown, 2),
        "net_pnl": round(equity - start_balance, 2),
        "curve": curve,
        "curve_labels": curve_labels,
    }


def grid_search_optimize(
    df: pd.DataFrame,
    conn: Optional[sqlite3.Connection] = None,
) -> Dict[str, Any]:
    """
    Exhaustive grid search across ATR periods and Z-Score thresholds.

    Selection objective: highest Profit Factor, tie-broken by trade count.
    """
    results: List[Dict[str, Any]] = []
    for atr_period in GRID_ATR_PERIODS:
        for z_threshold in GRID_Z_THRESHOLDS:
            try:
                result = backtest_params(df, atr_period, z_threshold)
            except Exception as exc:
                log.warning("Grid cell ATR=%s Z=%s failed: %s", atr_period, z_threshold, exc)
                continue
            results.append(result)
            if conn is not None:
                try:
                    persist_optimization(conn, result)
                except sqlite3.Error as exc:
                    log.warning("Optimization persistence failed: %s", exc)

    if not results:
        return {
            "atr_period": ATR_PERIOD_DEFAULT,
            "z_threshold": 2.0,
            "trades": 0,
            "win_rate": 0.0,
            "profit_factor": 0.0,
            "max_drawdown": 0.0,
            "net_pnl": 0.0,
            "curve": [],
            "curve_labels": [],
            "grid": [],
        }

    results.sort(key=lambda r: (r["profit_factor"], r["trades"]), reverse=True)
    best = dict(results[0])
    best["grid"] = [
        {
            "atr_period": r["atr_period"],
            "z_threshold": r["z_threshold"],
            "trades": r["trades"],
            "profit_factor": r["profit_factor"],
            "win_rate": r["win_rate"],
            "max_drawdown": r["max_drawdown"],
        }
        for r in results
    ]
    return best


# ==============================================================================
# 12. AUTO-TRAILING STOP ENGINE
# ==============================================================================


class TrailingStopEngine:
    """
    Stateful auto-trailing stop machine persisted in SQLite.

    Protocol:
      * Pre-TP1  : fixed forensic ATR stop.
      * Post-TP1 : stop ratchets to breakeven.
      * Post-TP1 : fractional ATR trail (0.75 x ATR) follows price.
    """

    TRAIL_FRACTION = 0.75

    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    def register(self, signal_id: int, sig: Dict[str, Any]) -> None:
        """Register a freshly generated signal into the trailing state machine."""
        try:
            self.conn.execute(
                """
                INSERT INTO open_positions
                    (signal_id, direction, entry, sl, tp1, tp2, tp3, atr,
                     tp1_hit, last_ts, status, updated_ts)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?, 'OPEN', ?)
                """,
                (
                    int(signal_id),
                    str(sig["decision"]),
                    float(sig["entry"]),
                    float(sig["sl"]),
                    float(sig["tp1"]),
                    float(sig["tp2"]),
                    float(sig["tp3"]),
                    float(sig["atr"]),
                    iso_now(),
                    iso_now(),
                ),
            )
            self.conn.commit()
            log.info("Trailing engine registered signal_id=%s (%s)", signal_id, sig["decision"])
        except sqlite3.Error as exc:
            log.error("Trailing registration failed: %s", exc)

    def refresh(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Advance every OPEN position across all bars newer than its last stamp."""
        try:
            rows = self.conn.execute(
                "SELECT * FROM open_positions WHERE status = 'OPEN'"
            ).fetchall()
        except sqlite3.Error as exc:
            log.error("Trailing refresh query failed: %s", exc)
            return []

        updates: List[Dict[str, Any]] = []

        for row in rows:
            record = dict(row)
            direction = str(record["direction"])
            entry = float(record["entry"])
            sl = float(record["sl"])
            tp1 = float(record["tp1"])
            tp2 = float(record["tp2"])
            tp3 = float(record["tp3"])
            atr_value = max(float(record["atr"]), EPS)
            tp1_hit = bool(record["tp1_hit"])
            status = "OPEN"

            last_ts_raw = record.get("last_ts")
            try:
                last_ts = pd.Timestamp(last_ts_raw) if last_ts_raw else None
                if last_ts is not None and last_ts.tzinfo is None:
                    last_ts = last_ts.tz_localize("UTC")
            except Exception:
                last_ts = None

            bars = df[df.index > last_ts] if last_ts is not None else df.tail(1)
            last_bar_ts = last_ts

            for bar_ts, bar in bars.iterrows():
                bar_high = float(bar["high"])
                bar_low = float(bar["low"])
                bar_close = float(bar["close"])
                last_bar_ts = bar_ts

                if direction == "BUY":
                    if bar_low <= sl:
                        status = "STOPPED"
                        break
                    if not tp1_hit and bar_high >= tp1:
                        tp1_hit = True
                        sl = max(sl, entry)
                    if bar_high >= tp2 and tp1_hit:
                        sl = max(sl, tp1)
                    if bar_high >= tp3 and tp1_hit:
                        sl = max(sl, tp2)
                    if tp1_hit:
                        sl = max(sl, bar_close - self.TRAIL_FRACTION * atr_value)
                else:
                    if bar_high >= sl:
                        status = "STOPPED"
                        break
                    if not tp1_hit and bar_low <= tp1:
                        tp1_hit = True
                        sl = min(sl, entry)
                    if bar_low <= tp2 and tp1_hit:
                        sl = min(sl, tp1)
                    if bar_low <= tp3 and tp1_hit:
                        sl = min(sl, tp2)
                    if tp1_hit:
                        sl = min(sl, bar_close + self.TRAIL_FRACTION * atr_value)

            stamp = last_bar_ts.isoformat() if last_bar_ts is not None else iso_now()

            try:
                self.conn.execute(
                    """
                    UPDATE open_positions
                       SET sl = ?, tp1_hit = ?, last_ts = ?, status = ?, updated_ts = ?
                     WHERE id = ?
                    """,
                    (
                        float(sl),
                        int(tp1_hit),
                        stamp,
                        status,
                        iso_now(),
                        int(record["id"]),
                    ),
                )
            except sqlite3.Error as exc:
                log.error("Trailing update failed for id=%s: %s", record["id"], exc)

            updates.append(
                {
                    "id": int(record["id"]),
                    "signal_id": record["signal_id"],
                    "direction": direction,
                    "entry": entry,
                    "sl": round(sl, 2),
                    "tp1_hit": tp1_hit,
                    "status": status,
                    "trail_mode": (
                        "BREAKEVEN_LOCKED" if tp1_hit and sl == entry
                        else ("ATR_TRAILING" if tp1_hit else "FIXED_ATR_STOP")
                    ),
                }
            )

        try:
            self.conn.commit()
        except sqlite3.Error as exc:
            log.error("Trailing commit failed: %s", exc)

        return updates


# ==============================================================================
# 13. BACKTEST CHART GENERATION (AGG BACKEND)
# ==============================================================================


def plot_backtest(
    df: pd.DataFrame,
    opt_result: Dict[str, Any],
    path: str = CHART_PATH,
) -> bool:
    """Render the institutional backtest performance chart safely."""
    fig = None
    try:
        fig, (ax_equity, ax_price) = plt.subplots(2, 1, figsize=(13, 8.5))

        curve = list(opt_result.get("curve") or [])
        labels = list(opt_result.get("curve_labels") or [])

        if len(curve) >= 2:
            if len(labels) == len(curve):
                try:
                    x_values = pd.to_datetime(labels, utc=True, errors="coerce")
                    if x_values.isna().any():
                        x_values = np.arange(len(curve))
                except Exception:
                    x_values = np.arange(len(curve))
            else:
                x_values = np.arange(len(curve))

            ax_equity.plot(x_values, curve, color="#0a7d32", linewidth=1.8)
            ax_equity.fill_between(
                x_values,
                curve,
                min(curve),
                color="#0a7d32",
                alpha=0.12,
            )
            ax_equity.set_title(
                f"Equity Curve — ATR({opt_result.get('atr_period')}) / "
                f"Z-Thr({opt_result.get('z_threshold')}) | "
                f"PF={opt_result.get('profit_factor')} | "
                f"WinRate={opt_result.get('win_rate')}% | "
                f"MaxDD={opt_result.get('max_drawdown')}%",
                fontsize=11,
                fontweight="bold",
            )
        else:
            ax_equity.text(
                0.5, 0.5, "No completed backtest trades",
                ha="center", va="center", transform=ax_equity.transAxes,
            )
            ax_equity.set_title("Equity Curve — Empty", fontsize=11, fontweight="bold")

        ax_equity.set_ylabel("Equity (USD)")

        price_tail = df.tail(320)
        ax_price.plot(
            price_tail.index, price_tail["close"].astype(float),
            color="#1f3b73", linewidth=1.4, label="XAUUSD Close",
        )
        ax_price.plot(
            price_tail.index,
            ema_series(price_tail["close"].astype(float), 20),
            color="#d97706", linewidth=1.1, label="EMA 20",
        )
        ax_price.plot(
            price_tail.index,
            ema_series(price_tail["close"].astype(float), 50),
            color="#b91c1c", linewidth=1.1, label="EMA 50",
        )
        ax_price.set_title(
            f"{SYSTEM_NAME} v{VERSION} — Price Structure (last {len(price_tail)} bars)",
            fontsize=11,
            fontweight="bold",
        )
        ax_price.set_ylabel("Price (USD)")
        ax_price.legend(loc="upper left", fontsize=8, framealpha=0.85)

        fig.tight_layout()
        fig.savefig(path, dpi=120, bbox_inches="tight")
        log.info("Backtest chart written -> %s", path)
        return True

    except Exception as exc:
        log.exception("Backtest chart generation failed: %s", exc)
        return False
    finally:
        if fig is not None:
            plt.close(fig)


# ==============================================================================
# 14. WEBHOOK NOTIFICATION TEMPLATES
# ==============================================================================


def build_institutional_alert(
    analysis: Dict[str, Any],
    optimization: Dict[str, Any],
    session_ok: bool,
    session_reasons: Sequence[str],
    trailing: Sequence[Dict[str, Any]],
) -> str:
    """Compose the institutional forensic HTML report."""
    decision = analysis["decision"]
    decision_label = "VERIFIED BUY" if decision == "BUY" else (
        "VERIFIED SELL" if decision == "SELL" else "NEUTRAL / STAND ASIDE"
    )

    trail_rows = [t for t in trailing if t.get("status") == "OPEN"]
    if trail_rows:
        trail_mode = trail_rows[0].get("trail_mode", "FIXED_ATR_STOP")
    else:
        trail_mode = "Armed @ TP1 Breakeven"

    session_line = "OPEN / NOMINAL" if session_ok else "SUPPRESSED — " + "; ".join(session_reasons)
    feed_line = "DEGRADED FALLBACK" if analysis["degraded_feed"] else "Nominal / Failover Ready"

    return (
        f"🛡️ <b>{SYSTEM_NAME} v{VERSION} (2-WAY COMMAND EDITION)</b> 🛡️\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🌟 <b>Asset:</b> XAUUSD (SPOT GOLD)\n"
        f"📊 <b>Signal Decision:</b> <b>{decision_label}</b>\n"
        f"🎯 <b>Precision Entry:</b> {analysis['entry']}\n"
        f"🛑 <b>Forensic Stop Loss:</b> {analysis['sl']}\n"
        f"📈 <b>Take Profit 1 (1.5R):</b> {analysis['tp1']}\n"
        f"📈 <b>Take Profit 2 (2.5R):</b> {analysis['tp2']}\n"
        f"🚀 <b>Take Profit 3 (4.0R):</b> {analysis['tp3']}\n"
        f"🔄 <b>Trailing Stop Protocol:</b> {trail_mode}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🤖 <b>Interactive 2-Way Command Status:</b>\n"
        f"• <i>Command Listener:</i> Active / Ready for /check\n"
        f"• <i>Multi-Source Feed:</i> {feed_line}\n"
        f"• <i>Feed Identifier:</i> {analysis['source']}\n"
        f"• <i>Session Governor:</i> {session_line}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚙️ <b>Grid Search & Exponential Backoff:</b>\n"
        f"• <i>Optimal ATR Period:</i> {optimization.get('atr_period')}\n"
        f"• <i>Optimal Z-Threshold:</i> {optimization.get('z_threshold')}\n"
        f"• <i>Webhook Status:</i> Delivered via Exponential Backoff\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 <b>Risk & Position Sizing:</b>\n"
        f"• <i>Recommended Lot:</i> {analysis['lots']:.2f} Lots ({analysis['risk_percent']:.2f}% Risk)\n"
        f"• <i>Risk Amount (USD):</i> ${analysis['risk_usd']:.2f}\n"
        f"• <i>Stop Distance:</i> {analysis['stop_distance']}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔬 <b>Forensic Audit, DB Artifact & Backtest:</b>\n"
        f"• <i>Confluence Score:</i> {analysis['confluence']:.1f}/100\n"
        f"• <i>SMC State:</i> {analysis['smc_state']}\n"
        f"• <i>Z-Score:</i> {analysis['zscore']:.3f}\n"
        f"• <i>SQLite Artifact:</i> Saved & Uploaded\n"
        f"• <i>Optimized Win Rate:</i> {optimization.get('win_rate', 0.0):.1f}%\n"
        f"• <i>Profit Factor:</i> {optimization.get('profit_factor', 0.0):.2f}\n"
        f"• <i>Max Drawdown:</i> {optimization.get('max_drawdown', 0.0):.1f}%\n"
        f"• <i>Trades Sampled:</i> {optimization.get('trades', 0)}\n"
        f"• <i>Timestamp (UTC):</i> {analysis['timestamp']}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━"
    )


def build_check_reply(
    analysis: Dict[str, Any],
    session_ok: bool,
    session_reasons: Sequence[str],
    optimization: Dict[str, Any],
) -> str:
    """Compact on-demand /check response."""
    decision = analysis["decision"]
    decision_label = "VERIFIED BUY" if decision == "BUY" else (
        "VERIFIED SELL" if decision == "SELL" else "NEUTRAL / STAND ASIDE"
    )
    pillar_lines = "\n".join(
        f"  • <i>{p['name']}:</i> {p['score']:+.1f} ({p['bias']})" for p in analysis["pillars"]
    )
    session_line = "OPEN" if session_ok else "SUPPRESSED — " + "; ".join(session_reasons)

    return (
        f"🔎 <b>{SYSTEM_NAME} — LIVE /CHECK</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🌟 <b>Asset:</b> XAUUSD (SPOT GOLD)\n"
        f"📊 <b>Decision:</b> <b>{decision_label}</b>\n"
        f"🎯 <b>Entry:</b> {analysis['entry']}\n"
        f"🛑 <b>SL:</b> {analysis['sl']} | <b>TP1:</b> {analysis['tp1']} | "
        f"<b>TP2:</b> {analysis['tp2']} | <b>TP3:</b> {analysis['tp3']}\n"
        f"🔬 <b>Confluence:</b> {analysis['confluence']:.1f}/100 "
        f"(signed {analysis['signed_confluence']:+.1f})\n"
        f"📐 <b>SMC State:</b> {analysis['smc_state']}\n"
        f"📉 <b>Z-Score:</b> {analysis['zscore']:.3f} (win {analysis['z_window']})\n"
        f"🌡️ <b>ATR:</b> {analysis['atr']}\n"
        f"💰 <b>Lot:</b> {analysis['lots']:.2f} | <b>Risk:</b> ${analysis['risk_usd']:.2f}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🧩 <b>Pillar Breakdown:</b>\n{pillar_lines}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚙️ <b>Optimal Params:</b> ATR({optimization.get('atr_period')}) / "
        f"Z({optimization.get('z_threshold')}) | PF {optimization.get('profit_factor', 0.0):.2f} | "
        f"WR {optimization.get('win_rate', 0.0):.1f}%\n"
        f"🗓️ <b>Session:</b> {session_line}\n"
        f"📡 <b>Feed:</b> {analysis['source']}\n"
        f"🕒 <b>UTC:</b> {analysis['timestamp']}"
    )


# ==============================================================================
# 15. MARKET DATA CACHE (FOR INTERACTIVE COMMANDS)
# ==============================================================================


class MarketCache:
    """Short-lived cache so /check spam does not hammer the free feeds."""

    def __init__(self, ttl_seconds: int = 60) -> None:
        self.ttl = ttl_seconds
        self._payload: Optional[Tuple[pd.DataFrame, str, bool]] = None
        self._stamp: float = 0.0

    def get(self, force: bool = False) -> Tuple[pd.DataFrame, str, bool]:
        now = time.time()
        if not force and self._payload is not None and (now - self._stamp) < self.ttl:
            return self._payload
        payload = fetch_market_data()
        self._payload = payload
        self._stamp = now
        return payload

    def invalidate(self) -> None:
        self._payload = None
        self._stamp = 0.0


MARKET_CACHE = MarketCache(ttl_seconds=60)
LISTENER_STARTED_AT: float = time.time()


# ==============================================================================
# 16. INTERACTIVE 2-WAY TELEGRAM COMMAND MODULE
# ==============================================================================


def tg_get_updates(offset: int, timeout: int = 10) -> List[Dict[str, Any]]:
    """Long-poll the Telegram getUpdates endpoint."""
    if not TELEGRAM_BOT_TOKEN:
        return []
    try:
        resp = requests.get(
            tg_api("getUpdates"),
            params={
                "offset": offset,
                "timeout": timeout,
                "allowed_updates": json.dumps(["message"]),
            },
            timeout=HTTP_TIMEOUT + timeout,
        )
        resp.raise_for_status()
        payload = resp.json()
        if not payload.get("ok"):
            log.warning("Telegram getUpdates returned ok=false: %s", payload)
            return []
        return payload.get("result", []) or []
    except Exception as exc:
        log.warning("Telegram getUpdates failure: %s", exc)
        return []


def tg_register_commands() -> None:
    """Publish the interactive command menu to Telegram."""
    if not TELEGRAM_BOT_TOKEN:
        return
    commands = [
        {"command": "check", "description": "Instant XAUUSD quantitative status"},
        {"command": "status", "description": "System health & forensic state"},
        {"command": "signal", "description": "Latest persisted signal"},
        {"command": "help", "description": "Show command reference"},
    ]
    try:
        http_post_backoff(
            tg_api("setMyCommands"),
            json_payload={"commands": commands},
            label="telegram-setMyCommands",
            max_retries=1,
        )
    except Exception as exc:
        log.warning("setMyCommands failed: %s", exc)


def handle_status_command(conn: sqlite3.Connection) -> str:
    """Assemble the /status system health report."""
    session_ok, reasons = session_governor()

    try:
        signal_count = conn.execute("SELECT COUNT(*) AS c FROM signals").fetchone()["c"]
        last_row = conn.execute(
            "SELECT ts, decision, entry, confluence FROM signals ORDER BY id DESC LIMIT 1"
        ).fetchone()
        open_count = conn.execute(
            "SELECT COUNT(*) AS c FROM open_positions WHERE status = 'OPEN'"
        ).fetchone()["c"]
    except sqlite3.Error:
        signal_count = 0
        last_row = None
        open_count = 0

    uptime = int(time.time() - LISTENER_STARTED_AT)
    last_signal_line = (
        f"{last_row['ts']} | {last_row['decision']} @ {last_row['entry']} "
        f"(conf {last_row['confluence']:.1f})"
        if last_row else "None recorded"
    )

    return (
        f"🩺 <b>{SYSTEM_NAME} v{VERSION} — SYSTEM STATUS</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"• <i>Session Governor:</i> {'OPEN' if session_ok else 'SUPPRESSED'}\n"
        f"• <i>Block Reasons:</i> {', '.join(reasons) if reasons else 'None'}\n"
        f"• <i>Telegram Token:</i> {'CONFIGURED' if TELEGRAM_BOT_TOKEN else 'MISSING'}\n"
        f"• <i>Telegram Chat:</i> {'CONFIGURED' if TELEGRAM_CHAT_ID else 'MISSING'}\n"
        f"• <i>Discord Webhook:</i> {'CONFIGURED' if DISCORD_WEBHOOK_URL else 'MISSING'}\n"
        f"• <i>SQLite DB:</i> {DB_PATH} ({signal_count} signals)\n"
        f"• <i>Open Positions:</i> {open_count}\n"
        f"• <i>Listener Uptime:</i> {uptime}s\n"
        f"• <i>Last Signal:</i> {last_signal_line}\n"
        f"• <i>UTC Now:</i> {iso_now()}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━"
    )


def handle_signal_command(conn: sqlite3.Connection) -> str:
    """Return the most recent persisted signal payload."""
    try:
        row = conn.execute("SELECT * FROM signals ORDER BY id DESC LIMIT 1").fetchone()
    except sqlite3.Error:
        row = None

    if not row:
        return "📭 <b>No signals persisted yet.</b> Run the engine once or send /check."

    return (
        f"📜 <b>LATEST PERSISTED SIGNAL</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"• <i>Timestamp:</i> {row['ts']}\n"
        f"• <i>Decision:</i> {row['decision']}\n"
        f"• <i>Entry:</i> {row['entry']}\n"
        f"• <i>SL:</i> {row['sl']}\n"
        f"• <i>TP1 / TP2 / TP3:</i> {row['tp1']} / {row['tp2']} / {row['tp3']}\n"
        f"• <i>Confluence:</i> {row['confluence']:.1f}/100\n"
        f"• <i>Lots:</i> {row['lots']:.2f} | <i>Risk:</i> ${row['risk_usd']:.2f}\n"
        f"• <i>Source:</i> {row['source']}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━"
    )


HELP_TEXT = (
    f"🛡️ <b>{SYSTEM_NAME} v{VERSION} — 2-WAY COMMAND CONSOLE</b>\n"
    "━━━━━━━━━━━━━━━━━━━━━━\n"
    "• <b>/check</b> — Instant live XAUUSD quantitative status "
    "(Z-Score, SMC, confluence, entry/SL/TP, sizing)\n"
    "• <b>/status</b> — System health, session governor, DB state\n"
    "• <b>/signal</b> — Latest persisted signal from the SQLite artifact\n"
    "• <b>/help</b> — This reference card\n"
    "━━━━━━━━━━━━━━━━━━━━━━\n"
    "<i>Signals refresh every cron cycle; /check bypasses the schedule.</i>"
)


def handle_update(conn: sqlite3.Connection, update: Dict[str, Any]) -> None:
    """Dispatch one inbound Telegram update to the correct command handler."""
    message = update.get("message") or {}
    text = (message.get("text") or "").strip()
    chat = message.get("chat") or {}
    chat_id = str(chat.get("id", "")) if chat.get("id") is not None else ""

    if not text or not chat_id:
        return

    if TELEGRAM_CHAT_ID and chat_id != str(TELEGRAM_CHAT_ID):
        log.warning("Rejected command from unauthorized chat_id=%s", chat_id)
        send_telegram_html("⛔ Unauthorized chat.", chat_id=chat_id)
        return

    command = text.split()[0].split("@")[0].lower()
    log.info("Telegram command received: %s (chat=%s)", command, chat_id)

    if command in ("/start", "/help"):
        send_telegram_html(HELP_TEXT, chat_id=chat_id)

    elif command == "/status":
        send_telegram_html(handle_status_command(conn), chat_id=chat_id)

    elif command == "/signal":
        send_telegram_html(handle_signal_command(conn), chat_id=chat_id)

    elif command == "/check":
        send_telegram_html("⏳ <i>Running live 5-pillar analysis…</i>", chat_id=chat_id)
        try:
            df, source, degraded = MARKET_CACHE.get()
            analysis = analyze_market(df, source, degraded)
            session_ok, reasons = session_governor()
            optimization = grid_search_optimize(df, conn=None)
            reply = build_check_reply(analysis, session_ok, reasons, optimization)
            forensic_audit_log(
                conn,
                "TELEGRAM_CHECK",
                {
                    "decision": analysis["decision"],
                    "confluence": analysis["confluence"],
                    "zscore": analysis["zscore"],
                    "source": source,
                },
            )
        except Exception as exc:
            log.exception("Interactive /check failed")
            reply = f"❌ <b>/check failed:</b> <code>{str(exc)[:400]}</code>"
        send_telegram_html(reply, chat_id=chat_id)

    else:
        send_telegram_html(
            f"❓ Unknown command <code>{command}</code>.\nSend /help for the command list.",
            chat_id=chat_id,
        )


def run_command_listener(conn: sqlite3.Connection, budget_seconds: int = POLL_SECONDS) -> int:
    """
    Interactive 2-way Telegram polling loop bounded by a time budget.

    Designed for serverless GitHub Actions: consumes as much of the job window
    as is safely available, then exits so the artifact upload can proceed.
    """
    if not TELEGRAM_BOT_TOKEN:
        log.info("Telegram token absent -> command listener skipped.")
        return 0

    deadline = time.time() + max(int(budget_seconds), 0)
    try:
        offset = int(get_state(conn, "tg_offset", "0") or 0)
    except (TypeError, ValueError):
        offset = 0

    tg_register_commands()
    processed = 0
    log.info("Telegram command listener online for %ss (offset=%s)", budget_seconds, offset)

    while time.time() < deadline:
        updates = tg_get_updates(offset, timeout=10)
        if not updates:
            time.sleep(1.0)
            continue

        for update in updates:
            update_id = update.get("update_id")
            if update_id is not None:
                offset = int(update_id) + 1
                set_state(conn, "tg_offset", str(offset))
            try:
                handle_update(conn, update)
                processed += 1
            except Exception:
                log.exception("Telegram update handler crashed")

    log.info("Telegram command listener offline (processed=%d)", processed)
    return processed


# ==============================================================================
# 17. CORE EXECUTION CYCLE
# ==============================================================================


def run_cycle(
    conn: sqlite3.Connection,
    notify: bool = True,
    listen_budget: int = POLL_SECONDS,
) -> Dict[str, Any]:
    """
    Execute one complete sovereign analysis cycle:
    fetch -> analyze -> optimize -> signal -> trail -> persist -> notify -> listen.
    """
    cycle_summary: Dict[str, Any] = {"status": "OK"}

    # ---- 1. Data ingestion -----------------------------------------------
    df, source, degraded = MARKET_CACHE.get(force=True)
    forensic_audit_log(
        conn,
        "DATA_INGESTION",
        {
            "source": source,
            "degraded": degraded,
            "rows": int(len(df)),
            "first_bar": df.index[0].isoformat(),
            "last_bar": df.index[-1].isoformat(),
        },
    )

    # ---- 2. Grid search optimization -------------------------------------
    optimization = grid_search_optimize(df, conn)
    forensic_audit_log(
        conn,
        "GRID_SEARCH_COMPLETE",
        {
            "optimal_atr_period": optimization.get("atr_period"),
            "optimal_z_threshold": optimization.get("z_threshold"),
            "profit_factor": optimization.get("profit_factor"),
            "win_rate": optimization.get("win_rate"),
            "max_drawdown": optimization.get("max_drawdown"),
            "trades": optimization.get("trades"),
            "cells_evaluated": len(optimization.get("grid", [])),
        },
    )

    # ---- 3. Five-pillar analysis -----------------------------------------
    analysis = analyze_market(df, source, degraded)
    forensic_audit_log(
        conn,
        "PILLAR_ANALYSIS",
        {
            "decision": analysis["decision"],
            "confluence": analysis["confluence"],
            "signed_confluence": analysis["signed_confluence"],
            "zscore": analysis["zscore"],
            "atr": analysis["atr"],
            "smc_state": analysis["smc_state"],
            "pillars": analysis["pillars"],
        },
    )

    # ---- 4. Session governor ---------------------------------------------
    session_ok, session_reasons = session_governor()
    forensic_audit_log(
        conn,
        "SESSION_GOVERNOR",
        {"allowed": session_ok, "reasons": list(session_reasons), "utc": iso_now()},
        level="INFO" if session_ok else "WARNING",
    )

    # ---- 5. Trailing stop engine -----------------------------------------
    trailing_engine = TrailingStopEngine(conn)
    trailing_updates: List[Dict[str, Any]] = []
    try:
        trailing_updates = trailing_engine.refresh(df)
        if trailing_updates:
            forensic_audit_log(conn, "TRAILING_ENGINE", {"positions": trailing_updates})
    except Exception as exc:
        log.exception("Trailing engine refresh failed")
        forensic_audit_log(conn, "TRAILING_ENGINE_ERROR", {"error": str(exc)}, level="ERROR")

    # ---- 6. Signal persistence & registration ----------------------------
    signal_id: Optional[int] = None
    if analysis["decision"] in ("BUY", "SELL"):
        try:
            signal_id = persist_signal(conn, analysis)
            trailing_engine.register(signal_id, analysis)
            forensic_audit_log(
                conn,
                "SIGNAL_PERSISTED",
                {
                    "signal_id": signal_id,
                    "decision": analysis["decision"],
                    "entry": analysis["entry"],
                    "sl": analysis["sl"],
                    "tp1": analysis["tp1"],
                    "tp2": analysis["tp2"],
                    "tp3": analysis["tp3"],
                    "lots": analysis["lots"],
                    "risk_usd": analysis["risk_usd"],
                },
            )
        except Exception as exc:
            log.exception("Signal persistence failed")
            forensic_audit_log(conn, "SIGNAL_PERSIST_ERROR", {"error": str(exc)}, level="ERROR")

    cycle_summary.update(
        {
            "decision": analysis["decision"],
            "confluence": analysis["confluence"],
            "signal_id": signal_id,
            "session_ok": session_ok,
        }
    )

    # ---- 7. Backtest chart ------------------------------------------------
    chart_ok = plot_backtest(df, optimization, CHART_PATH)
    cycle_summary["chart_written"] = chart_ok

    # ---- 8. Webhook dispatch ---------------------------------------------
    should_dispatch = (
        notify
        and (
            analysis["decision"] in ("BUY", "SELL")
            or NOTIFY_NEUTRAL
        )
        and (session_ok or FORCE_NOTIFY)
    )

    if should_dispatch:
        alert = build_institutional_alert(
            analysis, optimization, session_ok, session_reasons, trailing_updates
        )
        tg_ok = send_telegram_html(alert)
        dc_ok = send_discord(alert)
        forensic_audit_log(
            conn,
            "WEBHOOK_DISPATCH",
            {"telegram": tg_ok, "discord": dc_ok, "decision": analysis["decision"]},
        )
        cycle_summary["telegram_delivered"] = tg_ok
        cycle_summary["discord_delivered"] = dc_ok
    else:
        reason = (
            "session_suppressed" if not session_ok
            else ("neutral_signal" if analysis["decision"] == "NEUTRAL" else "notify_disabled")
        )
        log.info("Webhook dispatch skipped (%s).", reason)
        forensic_audit_log(conn, "WEBHOOK_SKIPPED", {"reason": reason})

    # ---- 9. Interactive command listener ---------------------------------
    if listen_budget and listen_budget > 0:
        run_command_listener(conn, listen_budget)

    forensic_audit_log(conn, "CYCLE_COMPLETE", cycle_summary)
    return cycle_summary


# ==============================================================================
# 18. ENTRYPOINT
# ==============================================================================


def main(argv: Optional[Sequence[str]] = None) -> int:
    """CLI entrypoint supporting once / check / listen modes."""
    parser = argparse.ArgumentParser(
        prog="bot.py",
        description=f"{SYSTEM_NAME} v{VERSION} — XAUUSD Hybrid Quant Engine",
    )
    parser.add_argument(
        "--mode",
        choices=["once", "check", "listen"],
        default="once",
        help="once = full cycle + listener | check = analysis only | listen = commands only",
    )
    parser.add_argument(
        "--listen-seconds",
        type=int,
        default=POLL_SECONDS,
        help="Budget for the interactive Telegram command listener.",
    )
    parser.add_argument(
        "--no-notify",
        action="store_true",
        help="Suppress webhook dispatch (analysis still runs).",
    )
    args = parser.parse_args(argv)

    conn = db_connect()
    init_db(conn)
    forensic_audit_log(
        conn,
        "BOOT",
        {
            "version": VERSION,
            "mode": args.mode,
            "listen_seconds": args.listen_seconds,
            "db_path": DB_PATH,
            "chart_path": CHART_PATH,
            "account_balance": ACCOUNT_BALANCE,
            "risk_percent": RISK_PERCENT,
            "python": sys.version.split()[0],
        },
    )

    try:
        if args.mode == "listen":
            run_command_listener(conn, args.listen_seconds)
            return 0

        if args.mode == "check":
            df, source, degraded = MARKET_CACHE.get(force=True)
            analysis = analyze_market(df, source, degraded)
            optimization = grid_search_optimize(df, conn)
            session_ok, reasons = session_governor()
            print(build_check_reply(analysis, session_ok, reasons, optimization))
            plot_backtest(df, optimization, CHART_PATH)
            forensic_audit_log(
                conn,
                "CHECK_MODE_COMPLETE",
                {"decision": analysis["decision"], "confluence": analysis["confluence"]},
            )
            return 0

        run_cycle(conn, notify=not args.no_notify, listen_budget=args.listen_seconds)
        return 0

    except Exception as exc:
        tb = traceback.format_exc()
        log.error("FATAL CYCLE FAILURE: %s", exc)
        forensic_audit_log(conn, "FATAL", {"error": str(exc), "trace": tb[-2000:]}, level="ERROR")
        send_emergency_alert(tb)
        return 1

    finally:
        try:
            conn.commit()
        except Exception:
            pass
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
