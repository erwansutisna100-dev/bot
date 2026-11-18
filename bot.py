#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
 APEX-QUANT-OMNI SOVEREIGN v75.1  (FORENSIC-HARDENED EDITION)
 Institutional-Grade XAUUSD Hybrid Quantitative Signal Engine
================================================================================
 v75.1 CORRECTIVE CHANGES (vs v75.0):
   [FIX-1] _finalize_ohlcv   : eliminate Adj Close / Close duplicate column
   [FIX-2] atr_series        : Series-type assert + NaN-safe fallback
   [FIX-3] zscore_series     : clip(-10,10) + fillna(0) — no infinity/NaN
   [FIX-4] analyze_market    : assert df.columns.is_unique at entry
   [FIX-5] backtest_params   : assert unique columns per cell
   [FIX-6] grid_search_opt   : log exception class + full message
   [FIX-7] pillar_smc/of     : assert Series before to_numpy
   [FIX-8] source_yfinance   : auto_adjust=True (no Adj Close)
   [FIX-9] _finalize_ohlcv   : paranoid final uniqueness re-check
================================================================================
"""

from __future__ import annotations

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
from datetime import datetime, timezone
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

VERSION = "75.1"
SYSTEM_NAME = "APEX-QUANT-OMNI SOVEREIGN"

DB_PATH    = os.environ.get("DB_PATH", "trading_signals.db")
CHART_PATH = os.environ.get("CHART_PATH", "backtest_performance.png")

TELEGRAM_BOT_TOKEN  = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID    = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "").strip()

ACCOUNT_BALANCE = float(os.environ.get("ACCOUNT_BALANCE", "10000") or 10000)
RISK_PERCENT    = float(os.environ.get("RISK_PERCENT", "1.0") or 1.0)
CONTRACT_SIZE   = float(os.environ.get("CONTRACT_SIZE", "100") or 100)

POLL_SECONDS   = int(os.environ.get("POLL_SECONDS", "240") or 240)
NOTIFY_NEUTRAL = os.environ.get("NOTIFY_NEUTRAL", "0").strip() in ("1","true","True","yes")
FORCE_NOTIFY   = os.environ.get("FORCE_NOTIFY", "0").strip() in ("1","true","True","yes")

EPS                = 1e-8
HTTP_TIMEOUT       = 20
MIN_CONFLUENCE     = 30.0
Z_WINDOW_DEFAULT   = 50
ATR_PERIOD_DEFAULT = 14
ATR_RISK_MULT      = 1.5
TP_R_MULTIPLES     = (1.5, 2.5, 4.0)
PARTIAL_WEIGHTS    = (0.50, 0.30, 0.20)

GRID_ATR_PERIODS  = (10, 14, 20)
GRID_Z_THRESHOLDS = (1.5, 2.0, 2.5)

UA_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"),
    "Accept": "application/json,text/plain,*/*",
}

# --- END SECTION 1 ---

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

# --- END SECTION 2 ---

# ==============================================================================
# 3. GENERIC UTILITIES
# ==============================================================================

def utcnow() -> datetime:
    return datetime.now(timezone.utc)

def iso_now() -> str:
    return utcnow().replace(microsecond=0).isoformat().replace("+00:00", "Z")

def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))

def safe_float(value: Any, fallback: float = 0.0) -> float:
    try:
        out = float(value)
        return fallback if math.isnan(out) or math.isinf(out) else out
    except (TypeError, ValueError):
        return fallback

def split_message(text: str, limit: int = 3800) -> List[str]:
    if len(text) <= limit:
        return [text]
    chunks, current = [], ""
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
    return unescape(re.sub(r"<[^>]+>", "", text))

# --- END SECTION 3 ---

# ==============================================================================
# 4. SQLITE PERSISTENCE
# ==============================================================================

DDL_STATEMENTS = (
    """CREATE TABLE IF NOT EXISTS signals (
        id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT NOT NULL,
        symbol TEXT NOT NULL, decision TEXT NOT NULL,
        entry REAL, sl REAL, tp1 REAL, tp2 REAL, tp3 REAL,
        confluence REAL, lots REAL, risk_usd REAL,
        atr REAL, zscore REAL, smc_state TEXT, source TEXT, payload TEXT)""",
    """CREATE TABLE IF NOT EXISTS audit (
        id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT NOT NULL,
        level TEXT NOT NULL, event TEXT NOT NULL, metrics TEXT)""",
    "CREATE TABLE IF NOT EXISTS state (key TEXT PRIMARY KEY, value TEXT)",
    """CREATE TABLE IF NOT EXISTS open_positions (
        id INTEGER PRIMARY KEY AUTOINCREMENT, signal_id INTEGER,
        direction TEXT, entry REAL, sl REAL, tp1 REAL, tp2 REAL, tp3 REAL,
        atr REAL, tp1_hit INTEGER DEFAULT 0, last_ts TEXT,
        status TEXT DEFAULT 'OPEN', updated_ts TEXT)""",
    """CREATE TABLE IF NOT EXISTS optimization_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT,
        atr_period INTEGER, z_threshold REAL, trades INTEGER,
        win_rate REAL, profit_factor REAL, max_drawdown REAL, equity_curve TEXT)""",
)

def db_connect(path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(path, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn

def init_db(conn: sqlite3.Connection) -> None:
    for ddl in DDL_STATEMENTS:
        conn.execute(ddl)
    conn.commit()

def set_state(conn, key, value):
    conn.execute("INSERT INTO state(key,value) VALUES(?,?) "
                 "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                 (str(key), str(value)))
    conn.commit()

def get_state(conn, key, default=None):
    row = conn.execute("SELECT value FROM state WHERE key=?", (str(key),)).fetchone()
    return row["value"] if row else default

def forensic_audit_log(conn, event, metrics=None, level="INFO"):
    metrics = metrics or {}
    payload = json.dumps(metrics, default=str, ensure_ascii=False)
    banner = "=" * 78
    log.log(getattr(logging, level.upper(), logging.INFO),
            "\n%s\n[FORENSIC] %s :: %s\n%s\n%s",
            banner, iso_now(), event, payload, banner)
    try:
        conn.execute("INSERT INTO audit(ts,level,event,metrics) VALUES(?,?,?,?)",
                     (iso_now(), level.upper(), str(event), payload))
        conn.commit()
    except sqlite3.Error as exc:
        log.error("Audit persistence failure: %s", exc)

def persist_signal(conn, sig) -> int:
    cur = conn.execute(
        """INSERT INTO signals
           (ts,symbol,decision,entry,sl,tp1,tp2,tp3,confluence,lots,risk_usd,
            atr,zscore,smc_state,source,payload)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (sig["timestamp"], sig["symbol"], sig["decision"],
         float(sig["entry"]), float(sig["sl"]),
         float(sig["tp1"]), float(sig["tp2"]), float(sig["tp3"]),
         float(sig["confluence"]), float(sig["lots"]), float(sig["risk_usd"]),
         float(sig["atr"]), float(sig["zscore"]),
         str(sig["smc_state"]), str(sig["source"]),
         json.dumps(sig, default=str, ensure_ascii=False)))
    conn.commit()
    return int(cur.lastrowid)

def persist_optimization(conn, row) -> None:
    conn.execute(
        """INSERT INTO optimization_runs
           (ts,atr_period,z_threshold,trades,win_rate,profit_factor,max_drawdown,equity_curve)
           VALUES (?,?,?,?,?,?,?,?)""",
        (iso_now(), int(row["atr_period"]), float(row["z_threshold"]),
         int(row["trades"]), float(row["win_rate"]),
         float(row["profit_factor"]), float(row["max_drawdown"]),
         json.dumps(row.get("curve", [])[:400], default=str)))
    conn.commit()

# --- END SECTION 4 ---

# ==============================================================================
# 5. EXPONENTIAL BACKOFF HTTP DELIVERY
# ==============================================================================

def http_post_backoff(url, *, json_payload=None, data=None, headers=None,
                     max_retries=3, base_delay=2.0, label="webhook"):
    delays = [base_delay * (2 ** i) for i in range(max_retries)]
    attempt, last_error = 0, None
    while attempt <= max_retries:
        try:
            resp = requests.post(url, json=json_payload, data=data,
                                 headers=headers, timeout=HTTP_TIMEOUT)
            if 200 <= resp.status_code < 300:
                log.info("[%s] delivered HTTP %s (attempt %d)",
                         label, resp.status_code, attempt + 1)
                return resp
            if resp.status_code == 429:
                wait = delays[attempt] if attempt < len(delays) else delays[-1]
                try:
                    wait = max(wait, float(resp.headers.get("Retry-After", "")))
                except (TypeError, ValueError):
                    pass
                log.warning("[%s] HTTP 429 -> sleeping %.1fs", label, wait)
                time.sleep(wait)
                attempt += 1
                continue
            if 500 <= resp.status_code < 600:
                last_error = RuntimeError(f"HTTP {resp.status_code}: {resp.text[:200]}")
            else:
                log.error("[%s] non-retryable HTTP %s: %s",
                          label, resp.status_code, resp.text[:300])
                return resp
        except requests.RequestException as exc:
            last_error = exc
            log.warning("[%s] attempt %d transport failure: %s",
                        label, attempt + 1, exc)
        if attempt < max_retries:
            wait = delays[attempt]
            log.info("[%s] backoff -> sleeping %.1fs", label, wait)
            time.sleep(wait)
        attempt += 1
    log.error("[%s] retries exhausted. Last: %s", label, last_error)
    return None

def tg_api(method: str) -> str:
    return f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}"

def send_telegram_html(text: str, chat_id: Optional[str] = None) -> bool:
    if not TELEGRAM_BOT_TOKEN:
        return False
    target = chat_id or TELEGRAM_CHAT_ID
    if not target:
        return False
    ok = True
    for chunk in split_message(text, 3800):
        resp = http_post_backoff(
            tg_api("sendMessage"),
            json_payload={"chat_id": target, "text": chunk,
                          "parse_mode": "HTML",
                          "disable_web_page_preview": True},
            label="telegram")
        if resp is None or resp.status_code >= 300:
            ok = False
    return ok

def send_discord(text: str) -> bool:
    if not DISCORD_WEBHOOK_URL:
        return False
    plain = strip_html(text)
    ok = True
    for chunk in split_message(plain, 1900):
        resp = http_post_backoff(
            DISCORD_WEBHOOK_URL,
            json_payload={"content": chunk,
                          "username": f"{SYSTEM_NAME} v{VERSION}"},
            label="discord")
        if resp is None or resp.status_code >= 300:
            ok = False
    return ok

def send_emergency_alert(error_text: str) -> None:
    body = (f"🚨 <b>{SYSTEM_NAME} v{VERSION} — CRITICAL FAILURE</b> 🚨\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🕒 <b>Timestamp (UTC):</b> {iso_now()}\n"
            f"🧨 <b>Traceback (tail):</b>\n<pre>{error_text[-1200:]}</pre>")
    try:
        send_telegram_html(body)
    except Exception:
        log.exception("Emergency Telegram alert failed.")
    try:
        send_discord(body)
    except Exception:
        log.exception("Emergency Discord alert failed.")

# --- END SECTION 5 ---

# ==============================================================================
# 6. MULTI-SOURCE FALLBACK DATA PIPELINE (v75.1 FORENSIC-HARDENED)
# ==============================================================================

_last_yahoo_throttle: float = 0.0

def _yahoo_soft_backoff() -> None:
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

def _finalize_ohlcv(df: Optional[pd.DataFrame], min_rows: int = 60) -> Optional[pd.DataFrame]:
    """
    [FIX-1] Adj Close vs Close duplicate elimination.
    [FIX-9] Paranoid final column-uniqueness re-check.
    """
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return None

    df = df.copy()
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    df.columns = [str(c).strip().lower().replace(" ", "_").replace("-", "_")
                  for c in df.columns]

    adj_variants = ("adj_close", "adjclose", "adjusted_close")
    has_close = "close" in df.columns
    for variant in adj_variants:
        if variant in df.columns:
            if has_close:
                df = df.drop(columns=[variant])
            else:
                df = df.rename(columns={variant: "close"})
                has_close = True

    if "price" in df.columns and "close" not in df.columns:
        df = df.rename(columns={"price": "close"})

    if df.columns.duplicated().any():
        dupes = sorted(set(df.columns[df.columns.duplicated()].tolist()))
        log.warning("Post-normalization duplicate columns -> dropping: %s", dupes)
        df = df.loc[:, ~df.columns.duplicated(keep="first")]

    for col in ("open", "high", "low", "close"):
        if col not in df.columns:
            return None

    if "volume" not in df.columns:
        df["volume"] = 0.0

    df = df[["open", "high", "low", "close", "volume"]].copy()
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

    if not df.columns.is_unique:
        log.error("Column uniqueness violated after finalize — rejecting frame.")
        return None
    if len(df) < min_rows:
        return None
    return df

def atr_series(df: pd.DataFrame, period: int = ATR_PERIOD_DEFAULT) -> pd.Series:
    """[FIX-2] Wilder-style ATR with Series-type assert + NaN-safe fallback."""
    high  = df["high"].astype(float)
    low   = df["low"].astype(float)
    close = df["close"].astype(float)
    if not (isinstance(high, pd.Series) and isinstance(low, pd.Series)
            and isinstance(close, pd.Series)):
        raise ValueError("atr_series: OHLC columns must be 1-D Series. "
                         "Duplicate columns present? Check _finalize_ohlcv.")
    prev_close = close.shift(1)
    true_range = pd.concat(
        [(high - low), (high - prev_close).abs(), (low - prev_close).abs()],
        axis=1).max(axis=1)
    atr = true_range.rolling(period, min_periods=period).mean()
    mean_val = atr.mean()
    if pd.isna(mean_val) or math.isinf(mean_val):
        mean_val = 0.0
    atr = atr.fillna(mean_val).fillna(0.0)
    return atr + EPS

def zscore_series(close: pd.Series, window: int = Z_WINDOW_DEFAULT) -> pd.Series:
    """[FIX-3] Rolling Z-Score with clip(-10,10) + fillna(0)."""
    if not isinstance(close, pd.Series):
        raise ValueError("zscore_series: close must be a 1-D Series.")
    mean = close.rolling(window, min_periods=window).mean()
    std  = close.rolling(window, min_periods=window).std(ddof=0)
    z = (close - mean) / (std + EPS)
    return z.clip(-10.0, 10.0).fillna(0.0)

def ema_series(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()

def resample_htf(df: pd.DataFrame, rule: str = "1h") -> pd.DataFrame:
    return pd.DataFrame({
        "open":   df["open"].resample(rule).first(),
        "high":   df["high"].resample(rule).max(),
        "low":    df["low"].resample(rule).min(),
        "close":  df["close"].resample(rule).last(),
        "volume": df["volume"].resample(rule).sum(),
    }).dropna(subset=["open", "high", "low", "close"])

def source_yfinance(symbol: str, interval: str, period: str) -> Optional[pd.DataFrame]:
    """[FIX-8] auto_adjust=True eliminates Adj Close at source."""
    _yahoo_soft_backoff()
    try:
        raw = yf.download(symbol, interval=interval, period=period,
                          progress=False, auto_adjust=True, threads=False)
    except Exception as exc:
        msg = str(exc)
        if "404" in msg or "delisted" in msg.lower():
            raise RuntimeError(f"symbol {symbol} delisted/unavailable") from exc
        raise
    if raw is None or raw.empty:
        raise RuntimeError(f"empty feed for {symbol}")
    finalized = _finalize_ohlcv(raw)
    if finalized is None:
        raise RuntimeError(f"finalize rejected data for {symbol}")
    return finalized

def source_yahoo_chart_api(symbol: str, interval: str = "15m",
                            range_: str = "5d") -> Optional[pd.DataFrame]:
    global _last_yahoo_throttle
    endpoint = f"https://query1.finance.yahoo.com/v8/finance/chart/{quote(symbol)}"
    params = {"interval": interval, "range": range_,
              "includePrePost": "false", "events": "div,splits"}
    _yahoo_soft_backoff()
    try:
        resp = requests.get(endpoint, params=params,
                            headers=UA_HEADERS, timeout=HTTP_TIMEOUT)
        if resp.status_code == 429:
            _last_yahoo_throttle = time.time()
            raise RuntimeError("Yahoo Chart API HTTP 429")
        resp.raise_for_status()
        payload = resp.json()
    except requests.RequestException as exc:
        if "429" in str(exc):
            _last_yahoo_throttle = time.time()
        raise
    result = (payload.get("chart") or {}).get("result") or []
    if not result:
        raise RuntimeError(f"chart API returned no result for {symbol}")
    node = result[0]
    timestamps = node.get("timestamp") or []
    quote_ = (node.get("indicators") or {}).get("quote") or [{}]
    q = quote_[0] if quote_ else {}
    if not timestamps or "close" not in q:
        raise RuntimeError(f"chart API missing fields for {symbol}")
    df = pd.DataFrame({
        "open":   q.get("open"),
        "high":   q.get("high"),
        "low":    q.get("low"),
        "close":  q.get("close"),
        "volume": q.get("volume") or [0] * len(timestamps),
    })
    df.index = pd.to_datetime(timestamps, unit="s", utc=True)
    return _finalize_ohlcv(df)

def source_stooq_daily(symbol: str = "xauusd") -> Optional[pd.DataFrame]:
    url = f"https://stooq.com/q/d/l/?s={quote(symbol)}&i=d"
    try:
        resp = requests.get(url, headers=UA_HEADERS, timeout=HTTP_TIMEOUT)
        resp.raise_for_status()
    except requests.RequestException as exc:
        raise RuntimeError(f"Stooq HTTP failure: {exc}") from exc
    text = resp.text.strip()
    if not text or "Date" not in text.split("\n")[0]:
        raise RuntimeError("Stooq returned non-CSV content")
    try:
        df = pd.read_csv(io.StringIO(text))
    except Exception as exc:
        raise RuntimeError(f"Stooq CSV parse error: {exc}") from exc
    df.columns = [c.lower() for c in df.columns]
    if "date" not in df.columns:
        raise RuntimeError("Stooq missing date column")
    df["date"] = pd.to_datetime(df["date"], utc=True, errors="coerce")
    df = df.set_index("date").sort_index()
    return _finalize_ohlcv(df)

DATA_SOURCES: Tuple[Dict[str, Any], ...] = (
    {"name": "yfinance-GC=F-15m",        "call": lambda: source_yfinance("GC=F", "15m", "5d")},
    {"name": "yfinance-PAXG-USD-15m",    "call": lambda: source_yfinance("PAXG-USD", "15m", "5d")},
    {"name": "yfinance-GLD-15m",         "call": lambda: source_yfinance("GLD", "15m", "5d")},
    {"name": "yfinance-XAUUSD=X-15m",    "call": lambda: source_yfinance("XAUUSD=X", "15m", "5d")},
    {"name": "yahoo-chart-GC=F-15m",     "call": lambda: source_yahoo_chart_api("GC=F", "15m", "5d")},
    {"name": "yahoo-chart-PAXG-USD-15m", "call": lambda: source_yahoo_chart_api("PAXG-USD", "15m", "5d")},
    {"name": "yahoo-chart-XAUUSD=X-15m", "call": lambda: source_yahoo_chart_api("XAUUSD=X", "15m", "5d")},
    {"name": "yfinance-GC=F-60m",        "call": lambda: source_yfinance("GC=F", "60m", "1mo")},
    {"name": "stooq-xauusd-daily",       "call": lambda: source_stooq_daily("xauusd")},
)

def fetch_market_data() -> Tuple[pd.DataFrame, str, bool]:
    """Iterate DATA_SOURCES. Returns (df, source_name, degraded_flag)."""
    errors = []
    for source in DATA_SOURCES:
        name = source["name"]
        try:
            df = source["call"]()
            if df is None or df.empty:
                raise RuntimeError("empty frame")
            if not df.columns.is_unique:
                raise RuntimeError(
                    f"duplicate columns: {df.columns[df.columns.duplicated()].tolist()}")
            degraded = any(tag in name for tag in ("60m", "daily", "stooq"))
            return df, name, degraded
        except Exception as exc:
            errors.append(f"{name}: {type(exc).__name__}: {exc}")
            log.warning("Feed source failed -> %s | %s", name, exc)
    raise RuntimeError("All data sources exhausted:\n" + "\n".join(errors))

# --- END SECTION 6 ---

# ==============================================================================
# 7. MARKET SESSION GOVERNOR
# ==============================================================================

def session_governor(ts: Optional[datetime] = None) -> Tuple[bool, str]:
    """Return (is_open, reason). Blocks weekends and low-liquidity windows."""
    ts = ts or utcnow()
    weekday = ts.weekday()  # Mon=0 ... Sun=6
    hour = ts.hour + ts.minute / 60.0

    if weekday == 5:
        return False, "Saturday — market closed"
    if weekday == 6 and hour < 22.0:
        return False, "Sunday pre-open (before 22:00 UTC)"
    if weekday == 4 and hour >= 20.5:
        return False, "Friday close window (after 20:30 UTC)"
    if weekday == 0 and hour < 0.5:
        return False, "Monday reopen stabilization"
    return True, "open"

# --- END SECTION 7 ---

# ==============================================================================
# 8. FIVE-PILLAR ANALYSIS ENGINE
# ==============================================================================

@dataclass
class PillarResult:
    name:    str
    score:   float          # -100..+100
    detail:  str
    metrics: Dict[str, Any] = field(default_factory=dict)

def _assert_series(*series: Tuple[str, Any]) -> None:
    """[FIX-7] Assert each element is a 1-D pandas Series."""
    for label, val in series:
        if not isinstance(val, pd.Series):
            raise ValueError(
                f"{label} is {type(val).__name__}, expected pd.Series. "
                "Duplicate columns likely — check _finalize_ohlcv.")

def pillar_smc(df: pd.DataFrame) -> PillarResult:
    """Swing-based BOS/CHoCH detection with magnitude weighting."""
    tail = df.tail(120)
    highs = tail["high"].to_numpy(dtype=float)
    lows  = tail["low"].to_numpy(dtype=float)
    closes = tail["close"].to_numpy(dtype=float)

    _assert_series(("high", tail["high"]), ("low", tail["low"]),
                   ("close", tail["close"]))

    if len(highs) < 20:
        return PillarResult("smc", 0.0, "insufficient bars", {})

    lookback = 10
    last_high, last_low = None, None
    for i in range(lookback, len(highs) - 1):
        window_high = highs[i - lookback:i + 1]
        window_low  = lows[i - lookback:i + 1]
        if highs[i] >= window_high.max() - EPS:
            last_high = highs[i]
        if lows[i] <= window_low.min() + EPS:
            last_low = lows[i]

    price = closes[-1]
    atr_value = safe_float(atr_series(df, ATR_PERIOD_DEFAULT).iloc[-1], EPS)

    score = 0.0
    state = "no_structure"
    if last_high is not None and price > last_high:
        magnitude = clamp((price - last_high) / (atr_value + EPS), 0.0, 1.0)
        score = 55.0 + 45.0 * magnitude
        state = "bullish_BOS"
    elif last_low is not None and price < last_low:
        magnitude = clamp((last_low - price) / (atr_value + EPS), 0.0, 1.0)
        score = -55.0 - 45.0 * magnitude
        state = "bearish_BOS"
    elif last_high is not None and last_low is not None:
        mid = (last_high + last_low) / 2.0
        score = 20.0 if price > mid else -20.0
        state = "inside_range"

    return PillarResult("smc", score, state,
                        {"price": price, "atr": atr_value,
                         "last_high": last_high, "last_low": last_low})

def pillar_order_flow(df: pd.DataFrame) -> PillarResult:
    """Body-vs-wick directional pressure."""
    tail = df.tail(20)
    opens  = tail["open"].to_numpy(dtype=float)
    highs  = tail["high"].to_numpy(dtype=float)
    lows   = tail["low"].to_numpy(dtype=float)
    closes = tail["close"].to_numpy(dtype=float)
    volume = tail["volume"].to_numpy(dtype=float)

    _assert_series(("open", tail["open"]), ("close", tail["close"]),
                   ("high", tail["high"]), ("low", tail["low"]),
                   ("volume", tail["volume"]))

    if len(closes) < 5:
        return PillarResult("order_flow", 0.0, "insufficient bars", {})

    if volume.sum() <= 0:
        volume = np.ones_like(volume)

    body = closes - opens
    bar_range = np.maximum(highs - lows, EPS)
    body_ratio = np.clip(np.abs(body) / bar_range, 0.0, 1.0)
    directional_pressure = np.sign(body) * body_ratio * volume

    total_volume = float(volume.sum()) or 1.0
    pressure_component = float(directional_pressure.sum() / total_volume) * 60.0
    pressure_component = clamp(pressure_component, -100.0, 100.0)

    return PillarResult(
        "order_flow", pressure_component,
        "bullish_dominant" if pressure_component > 10
        else "bearish_dominant" if pressure_component < -10
        else "balanced",
        {"raw_pressure": pressure_component})

def pillar_zscore(df: pd.DataFrame) -> PillarResult:
    """Mean-reversion bias via rolling Z-Score."""
    close = df["close"]
    _assert_series(("close", close))
    z_series = zscore_series(close, Z_WINDOW_DEFAULT)
    z = safe_float(z_series.iloc[-1], 0.0)

    score = clamp(-z * 35.0, -100.0, 100.0)
    return PillarResult(
        "zscore", score,
        f"z={z:+.2f}",
        {"z": z})

def pillar_mtf(df: pd.DataFrame) -> PillarResult:
    """Higher-timeframe EMA alignment (resample to 1h)."""
    htf = resample_htf(df, "1h")
    slow = 50
    fast = 20

    if len(htf) < slow + 2:
        htf = df.copy()

    if len(htf) < max(fast, slow) + 2:
        return PillarResult("mtf", 0.0, "insufficient HTF bars", {})

    ema_fast = ema_series(htf["close"], fast)
    ema_slow = ema_series(htf["close"], slow)
    last_fast = safe_float(ema_fast.iloc[-1], 0.0)
    last_slow = safe_float(ema_slow.iloc[-1], 0.0)
    price = safe_float(htf["close"].iloc[-1], 0.0)

    score = 0.0
    state = "neutral"
    if last_fast > last_slow and price > last_fast:
        score = 60.0
        state = "bullish_aligned"
    elif last_fast < last_slow and price < last_fast:
        score = -60.0
        state = "bearish_aligned"
    elif last_fast > last_slow:
        score = 25.0
        state = "bullish_weak"
    elif last_fast < last_slow:
        score = -25.0
        state = "bearish_weak"

    return PillarResult("mtf", score, state,
                        {"fast": last_fast, "slow": last_slow, "price": price})

def pillar_atr_regime(df: pd.DataFrame) -> PillarResult:
    """ATR regime: compresses confluence when volatility is dead."""
    atr = atr_series(df, ATR_PERIOD_DEFAULT)
    recent = safe_float(atr.iloc[-1], EPS)
    baseline = safe_float(atr.tail(50).mean(), EPS)
    if baseline <= EPS:
        return PillarResult("atr_regime", 0.0, "unknown", {"multiplier": 1.0})

    ratio = recent / baseline
    multiplier = clamp(ratio, 0.4, 1.2)
    state = "expanding" if ratio > 1.15 else "compressed" if ratio < 0.85 else "normal"
    return PillarResult("atr_regime", 0.0, state,
                        {"ratio": ratio, "multiplier": multiplier})

# --- END SECTION 8 ---

# ==============================================================================
# 9. CONFLUENCE SYNTHESIS
# ==============================================================================

PILLAR_WEIGHTS = {
    "smc":        0.30,
    "order_flow": 0.20,
    "zscore":     0.30,
    "mtf":        0.20,
}

def synthesize_confluence(pillars: Sequence[PillarResult]) -> Dict[str, Any]:
    signed = 0.0
    quality_multiplier = 1.0
    for pillar in pillars:
        weight = PILLAR_WEIGHTS.get(pillar.name, 0.0)
        signed += pillar.score * weight
        if pillar.name == "atr_regime":
            quality_multiplier = float(pillar.metrics.get("multiplier", 1.0))

    signed *= quality_multiplier
    signed = clamp(signed, -100.0, 100.0)

    if signed >= MIN_CONFLUENCE:
        decision = "BUY"
    elif signed <= -MIN_CONFLUENCE:
        decision = "SELL"
    else:
        decision = "NEUTRAL"

    return {
        "confluence": round(signed, 2),
        "decision":   decision,
        "quality_multiplier": round(quality_multiplier, 3),
        "pillars":    [{"name": p.name, "score": round(p.score, 2),
                        "detail": p.detail, "metrics": p.metrics}
                       for p in pillars],
    }

# --- END SECTION 9 ---

# ==============================================================================
# 10. RISK MANAGEMENT & POSITION SIZING
# ==============================================================================

def position_size(entry: float, stop: float, balance: float,
                  risk_pct: float, contract: float) -> Tuple[float, float]:
    stop_distance = abs(entry - stop)
    if stop_distance <= EPS:
        return 0.01, 0.0
    risk_usd = balance * (risk_pct / 100.0)
    lots = risk_usd / (stop_distance * contract)
    lots = max(round(lots, 2), 0.01)
    return lots, round(risk_usd, 2)

def build_risk_levels(direction: str, entry: float, atr: float,
                      atr_mult: float = ATR_RISK_MULT,
                      tp_mults: Sequence[float] = TP_R_MULTIPLES) -> Dict[str, float]:
    stop_distance = max(atr * atr_mult, EPS)
    if direction == "BUY":
        sl = entry - stop_distance
        tps = [entry + stop_distance * r for r in tp_mults]
    else:
        sl = entry + stop_distance
        tps = [entry - stop_distance * r for r in tp_mults]
    return {
        "entry": round(entry, 2),
        "sl":    round(sl, 2),
        "tp1":   round(tps[0], 2),
        "tp2":   round(tps[1], 2),
        "tp3":   round(tps[2], 2),
        "stop_distance": round(stop_distance, 4),
    }

# --- END SECTION 10 ---
# ==============================================================================
# 11. MARKET ANALYSIS (v75.1 — [FIX-4])
# ==============================================================================

def analyze_market(df: pd.DataFrame, source: str, degraded: bool,
                   optimization: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Orchestrate all 5 pillars, synthesize confluence, build risk levels.

    [FIX-4] Fail-fast if df has duplicate columns (defense-in-depth).
    """
    if df is None or len(df) < 30:
        raise RuntimeError("analyze_market: insufficient market data")
    if not df.columns.is_unique:
        dupes = df.columns[df.columns.duplicated()].tolist()
        raise ValueError(f"analyze_market: duplicate columns present: {dupes}")

    atr_period = ATR_PERIOD_DEFAULT
    z_threshold = 2.0
    if optimization:
        atr_period = int(optimization.get("optimal_atr_period", ATR_PERIOD_DEFAULT))
        z_threshold = float(optimization.get("optimal_z_threshold", 2.0))

    pillars = [
        pillar_smc(df),
        pillar_order_flow(df),
        pillar_zscore(df),
        pillar_mtf(df),
        pillar_atr_regime(df),
    ]

    conf = synthesize_confluence(pillars)
    decision = conf["decision"]
    confluence = conf["confluence"]

    atr_value = safe_float(atr_series(df, atr_period).iloc[-1], EPS)
    z_val = safe_float(zscore_series(df["close"], Z_WINDOW_DEFAULT).iloc[-1], 0.0)
    price = safe_float(df["close"].iloc[-1], 0.0)

    levels = build_risk_levels(
        decision if decision != "NEUTRAL" else "BUY", price, atr_value)
    lots, risk_usd = position_size(
        levels["entry"], levels["sl"],
        ACCOUNT_BALANCE, RISK_PERCENT, CONTRACT_SIZE)

    smc_pillar = next((p for p in pillars if p.name == "smc"), None)
    smc_state = smc_pillar.detail if smc_pillar else "unknown"

    return {
        "timestamp": iso_now(),
        "symbol": "XAUUSD",
        "decision": decision,
        "confluence": confluence,
        "entry": levels["entry"],
        "sl": levels["sl"],
        "tp1": levels["tp1"],
        "tp2": levels["tp2"],
        "tp3": levels["tp3"],
        "lots": lots,
        "risk_usd": risk_usd,
        "atr": atr_value,
        "zscore": z_val,
        "smc_state": smc_state,
        "source": source,
        "degraded": degraded,
        "pillars": conf["pillars"],
        "quality_multiplier": conf["quality_multiplier"],
        "atr_period_used": atr_period,
        "z_threshold_used": z_threshold,
    }

# --- END SECTION 11 ---

# ==============================================================================
# 12. BACKTEST + GRID SEARCH (v75.1 — [FIX-5] [FIX-6])
# ==============================================================================

def backtest_params(
    df: pd.DataFrame,
    atr_period: int,
    z_threshold: float,
    atr_mult: float = ATR_RISK_MULT,
    rr: Sequence[float] = TP_R_MULTIPLES,
    risk_usd: float = 100.0,
) -> Dict[str, Any]:
    """
    Vectorized-scan backtest: open trades on Z-Score crosses, track partial TPs.

    [FIX-5] Assert unique columns + Series-type before building work frame.
    """
    empty = {"trades": 0, "win_rate": 0.0, "profit_factor": 0.0,
             "max_drawdown": 0.0, "curve": [0.0]}

    if df is None or len(df) < 60:
        return empty
    if df.columns.duplicated().any():
        raise ValueError(
            f"backtest_params: duplicate columns: "
            f"{df.columns[df.columns.duplicated()].tolist()}")
    for col in ("open", "high", "low", "close"):
        if not isinstance(df[col], pd.Series):
            raise ValueError(f"backtest_params: '{col}' is not a Series")

    work = pd.DataFrame({
        "open":  df["open"].astype(float),
        "high":  df["high"].astype(float),
        "low":   df["low"].astype(float),
        "close": df["close"].astype(float),
    })
    work["atr"] = atr_series(work, atr_period)
    work["z"]   = zscore_series(work["close"], Z_WINDOW_DEFAULT)
    work = work.dropna(subset=["atr", "z"])
    if len(work) < 30:
        return empty

    opens  = work["open"].to_numpy()
    highs  = work["high"].to_numpy()
    lows   = work["low"].to_numpy()
    closes = work["close"].to_numpy()
    atrs   = work["atr"].to_numpy()
    zs     = work["z"].to_numpy()
    index  = work.index

    def _open(direction: str, price: float, atr: float) -> Dict[str, Any]:
        stop_distance = max(atr * atr_mult, EPS)
        if direction == "BUY":
            sl = price - stop_distance
            tps = [price + stop_distance * r for r in rr]
        else:
            sl = price + stop_distance
            tps = [price - stop_distance * r for r in rr]
        return {"direction": direction, "entry": price, "sl": sl,
                "tps": list(tps), "sl_dist": stop_distance,
                "remaining": 1.0, "realized": 0.0}

    def _finalize(pos: Dict[str, Any], exit_price: float, bar_i: int) -> None:
        if pos["remaining"] <= 0:
            return
        r_dist = (exit_price - pos["entry"]) / pos["sl_dist"]
        if pos["direction"] == "SELL":
            r_dist = -r_dist
        pos["realized"] += pos["remaining"] * r_dist
        pos["remaining"] = 0.0
        trades.append({"r": pos["realized"],
                       "pnl": pos["realized"] * risk_usd,
                       "ts": str(index[bar_i])})

    trades: List[Dict[str, Any]] = []
    position: Optional[Dict[str, Any]] = None
    curve: List[float] = [0.0]
    equity = 0.0

    for i in range(len(work)):
        if position is None:
            if i > 0:
                if zs[i - 1] > -z_threshold and zs[i] <= -z_threshold:
                    position = _open("BUY", closes[i], atrs[i])
                elif zs[i - 1] < z_threshold and zs[i] >= z_threshold:
                    position = _open("SELL", closes[i], atrs[i])
            if position is None:
                curve.append(equity)
                continue

        bar_high = highs[i]
        bar_low = lows[i]

        if position["direction"] == "BUY":
            if bar_low <= position["sl"]:
                _finalize(position, position["sl"], i)
                position = None
            else:
                for tp_idx, tp in enumerate(position["tps"]):
                    if position["remaining"] <= 0:
                        break
                    if bar_high >= tp:
                        portion = (PARTIAL_WEIGHTS[tp_idx]
                                   if tp_idx < len(PARTIAL_WEIGHTS)
                                   else position["remaining"])
                        r_tp = (tp - position["entry"]) / position["sl_dist"]
                        position["realized"] += portion * r_tp
                        position["remaining"] -= portion
                        if tp_idx == 0:
                            position["sl"] = max(position["sl"], position["entry"])
        else:
            if bar_high >= position["sl"]:
                _finalize(position, position["sl"], i)
                position = None
            else:
                for tp_idx, tp in enumerate(position["tps"]):
                    if position["remaining"] <= 0:
                        break
                    if bar_low <= tp:
                        portion = (PARTIAL_WEIGHTS[tp_idx]
                                   if tp_idx < len(PARTIAL_WEIGHTS)
                                   else position["remaining"])
                        r_tp = (position["entry"] - tp) / position["sl_dist"]
                        position["realized"] += portion * r_tp
                        position["remaining"] -= portion
                        if tp_idx == 0:
                            position["sl"] = min(position["sl"], position["entry"])

        if position is not None and position["remaining"] <= 0:
            trades.append({"r": position["realized"],
                           "pnl": position["realized"] * risk_usd,
                           "ts": str(index[i])})
            position = None

        if position is not None:
            direction_sign = 1 if position["direction"] == "BUY" else -1
            unrealized = (position["remaining"]
                          * ((closes[i] - position["entry"]) / position["sl_dist"])
                          * direction_sign)
            curve.append(equity + position["realized"] + unrealized)
        else:
            equity = sum(t["r"] for t in trades) * risk_usd
            curve.append(equity)

    if position is not None:
        _finalize(position, closes[-1], len(work) - 1)
        equity = sum(t["r"] for t in trades) * risk_usd
        curve[-1] = equity

    if not trades:
        return empty

    wins = [t for t in trades if t["r"] > 0]
    losses = [t for t in trades if t["r"] < 0]
    win_rate = len(wins) / len(trades) * 100.0
    gross_win = sum(t["r"] for t in wins)
    gross_loss = abs(sum(t["r"] for t in losses))
    profit_factor = (gross_win / gross_loss if gross_loss > EPS
                     else (gross_win if gross_win > 0 else 0.0))

    peak = -float("inf")
    max_dd = 0.0
    for val in curve:
        peak = max(peak, val)
        if peak > 0:
            max_dd = max(max_dd, (peak - val) / peak)

    return {
        "trades": len(trades),
        "win_rate": round(win_rate, 2),
        "profit_factor": round(profit_factor, 3),
        "max_drawdown": round(max_dd * 100.0, 2),
        "curve": curve,
    }


def grid_search_optimize(df: pd.DataFrame,
                          conn: Optional[sqlite3.Connection] = None
                          ) -> Dict[str, Any]:
    """9-cell grid over (ATR period, Z threshold). [FIX-6] log class name."""
    results: List[Dict[str, Any]] = []
    cells = 0
    for atr_period in GRID_ATR_PERIODS:
        for z_threshold in GRID_Z_THRESHOLDS:
            cells += 1
            try:
                res = backtest_params(df, atr_period, z_threshold)
                res["atr_period"] = atr_period
                res["z_threshold"] = z_threshold
                results.append(res)
            except Exception as exc:
                log.warning("Grid cell ATR=%s Z=%s failed: %s: %s",
                            atr_period, z_threshold,
                            type(exc).__name__, exc)

    if not results:
        log.error("Grid search produced zero results across %d cells", cells)
        return {
            "optimal_atr_period": ATR_PERIOD_DEFAULT,
            "optimal_z_threshold": 2.0,
            "profit_factor": 0.0, "win_rate": 0.0,
            "max_drawdown": 0.0, "trades": 0,
            "cells_evaluated": cells,
            "curve": [0.0],
        }

    results.sort(key=lambda r: (r["profit_factor"], r["trades"]), reverse=True)
    best = dict(results[0])
    best["cells_evaluated"] = cells

    if conn is not None:
        try:
            persist_optimization(conn, best)
        except Exception as exc:
            log.warning("Persist optimization failed: %s", exc)

    return best

# --- END SECTION 12 ---

# ==============================================================================
# 13. AUTO-TRAILING STOP ENGINE
# ==============================================================================

class TrailingStopEngine:
    """SQLite-persisted position tracker with breakeven + ATR trail."""

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def register(self, sig: Dict[str, Any]) -> None:
        self.conn.execute(
            """INSERT INTO open_positions
               (signal_id, direction, entry, sl, tp1, tp2, tp3, atr,
                tp1_hit, last_ts, status, updated_ts)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (sig.get("signal_id"), sig["decision"],
             float(sig["entry"]), float(sig["sl"]),
             float(sig["tp1"]), float(sig["tp2"]), float(sig["tp3"]),
             float(sig["atr"]), 0, sig["timestamp"], "OPEN", iso_now()))
        self.conn.commit()

    def refresh(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT * FROM open_positions WHERE status='OPEN'").fetchall()
        closed: List[Dict[str, Any]] = []

        for row in rows:
            pos = dict(row)
            last_ts_raw = pos.get("last_ts") or ""
            try:
                last_ts = (pd.to_datetime(last_ts_raw, utc=True)
                           if last_ts_raw else df.index[0])
            except Exception:
                last_ts = df.index[0]

            bars = df[df.index > last_ts]
            status = pos["status"]
            sl = float(pos["sl"])
            tp1_hit = int(pos["tp1_hit"])
            direction = pos["direction"]

            for _ts, bar in bars.iterrows():
                bar_high = float(bar["high"])
                bar_low = float(bar["low"])
                bar_close = float(bar["close"])

                if direction == "BUY":
                    if bar_low <= sl:
                        status = "STOPPED"
                        break
                    if not tp1_hit and bar_high >= pos["tp1"]:
                        tp1_hit = 1
                        sl = max(sl, pos["entry"])
                    if tp1_hit:
                        trail = bar_close - 0.75 * pos["atr"]
                        sl = max(sl, trail)
                else:
                    if bar_high >= sl:
                        status = "STOPPED"
                        break
                    if not tp1_hit and bar_low <= pos["tp1"]:
                        tp1_hit = 1
                        sl = min(sl, pos["entry"])
                    if tp1_hit:
                        trail = bar_close + 0.75 * pos["atr"]
                        sl = min(sl, trail)

            new_last_ts = (str(bars.index[-1]) if len(bars) > 0 else str(last_ts))

            self.conn.execute(
                """UPDATE open_positions
                   SET sl=?, tp1_hit=?, last_ts=?, status=?, updated_ts=?
                   WHERE id=?""",
                (float(sl), int(tp1_hit), new_last_ts, status,
                 iso_now(), pos["id"]))

            if status == "STOPPED":
                closed.append({"id": pos["id"], "direction": direction,
                               "final_sl": sl})

        self.conn.commit()
        return closed

    def active_positions(self) -> List[Dict[str, Any]]:
        return [dict(r) for r in self.conn.execute(
            "SELECT * FROM open_positions WHERE status='OPEN'").fetchall()]

# --- END SECTION 13 ---

# ==============================================================================
# 14. CHART GENERATOR (HEADLESS MATPLOTLIB)
# ==============================================================================

def plot_backtest(curve: Sequence[float],
                   timestamps: Optional[Sequence[str]] = None,
                   path: str = CHART_PATH) -> Optional[str]:
    if not curve or len(curve) < 2:
        return None
    try:
        fig, ax = plt.subplots(figsize=(10, 4), dpi=100)
        ax.plot(range(len(curve)), list(curve),
                color="#2196F3", linewidth=1.5)
        ax.fill_between(range(len(curve)), list(curve),
                         min(curve), alpha=0.15, color="#2196F3")
        ax.axhline(0, color="gray", linestyle="--", linewidth=0.8)
        ax.set_title(f"{SYSTEM_NAME} v{VERSION} — Equity Curve")
        ax.set_ylabel("PnL (R units)")
        ax.set_xlabel("Trade")
        fig.tight_layout()
        fig.savefig(path)
        plt.close(fig)
        return path
    except Exception as exc:
        log.warning("Chart generation failed: %s", exc)
        return None

# --- END SECTION 14 ---

# ==============================================================================
# 15. INSTITUTIONAL ALERT TEMPLATES
# ==============================================================================

def build_institutional_alert(sig: Dict[str, Any],
                                optimization: Optional[Dict[str, Any]] = None
                                ) -> str:
    decision = sig["decision"]
    emoji = {"BUY": "🟢", "SELL": "🔴"}.get(decision, "⚪")
    lines = [
        f"{emoji} <b>{SYSTEM_NAME} v{VERSION}</b>",
        "━━━━━━━━━━━━━━━━━━━━━━",
        f"<b>Symbol:</b> {sig['symbol']}",
        f"<b>Decision:</b> {decision}",
        f"<b>Confluence:</b> {sig['confluence']}",
        f"<b>Quality:</b> ×{sig.get('quality_multiplier', 1.0)}",
        "",
        "<b>📐 Risk Levels</b>",
        f"Entry: <code>{sig['entry']}</code>",
        f"SL: <code>{sig['sl']}</code>",
        f"TP1: <code>{sig['tp1']}</code>",
        f"TP2: <code>{sig['tp2']}</code>",
        f"TP3: <code>{sig['tp3']}</code>",
        "",
        "<b>💰 Risk Profile</b>",
        f"Lots: {sig['lots']} | Risk USD: ${sig['risk_usd']}",
        f"ATR: {sig['atr']:.4f} | Z: {sig['zscore']:+.2f}",
        "",
        "<b>🔬 Pillars</b>",
    ]
    for p in sig.get("pillars", []):
        lines.append(f"• {p['name']:<10} {p['score']:>+7.2f}  ({p['detail']})")

    lines += [
        "",
        f"<b>📡 Source:</b> {sig['source']}"
        + (" ⚠️ degraded" if sig.get("degraded") else ""),
    ]
    if optimization:
        lines += [
            "<b>⚙️ Grid:</b> "
            f"ATR={optimization.get('optimal_atr_period')} "
            f"Z={optimization.get('optimal_z_threshold')}",
            f"<b>PF:</b> {optimization.get('profit_factor')} | "
            f"<b>WR:</b> {optimization.get('win_rate')}% | "
            f"<b>Trades:</b> {optimization.get('trades')}",
        ]
    lines.append(f"<b>🕒</b> {sig['timestamp']}")
    return "\n".join(lines)

# --- END SECTION 15 ---

# ==============================================================================
# 16. MARKET CACHE
# ==============================================================================

class MarketCache:
    def __init__(self, ttl: int = 60):
        self.ttl = ttl
        self.last_fetch: float = 0.0
        self.cached: Optional[Tuple[pd.DataFrame, str, bool]] = None

    def get(self, force: bool = False) -> Tuple[pd.DataFrame, str, bool]:
        now = time.time()
        if not force and self.cached and (now - self.last_fetch) < self.ttl:
            return self.cached
        t0 = time.time()
        df, source, degraded = fetch_market_data()
        latency = time.time() - t0
        self.cached = (df, source, degraded)
        self.last_fetch = now
        log.info("Feed OK -> %s | rows=%d | latency=%.2fs | degraded=%s",
                 source, len(df), latency, degraded)
        return self.cached


MARKET_CACHE = MarketCache(ttl=60)

# --- END SECTION 16 ---

# ==============================================================================
# 17. TELEGRAM COMMANDS (2-WAY INTERACTIVE)
# ==============================================================================

def tg_register_commands() -> None:
    if not TELEGRAM_BOT_TOKEN:
        return
    commands = [
        {"command": "check",  "description": "Live market analysis"},
        {"command": "status", "description": "Open positions"},
        {"command": "signal", "description": "Last signal"},
        {"command": "help",   "description": "Show help"},
    ]
    http_post_backoff(tg_api("setMyCommands"),
                      json_payload={"commands": commands},
                      max_retries=1, label="tg_setcmds")


def build_check_reply(sig: Dict[str, Any],
                      optimization: Dict[str, Any]) -> str:
    return (
        f"🔍 <b>Live Check — {SYSTEM_NAME} v{VERSION}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>Decision:</b> {sig['decision']}\n"
        f"<b>Confluence:</b> {sig['confluence']}\n"
        f"<b>Entry:</b> {sig['entry']}\n"
        f"<b>SL:</b> {sig['sl']}\n"
        f"<b>TPs:</b> {sig['tp1']} / {sig['tp2']} / {sig['tp3']}\n"
        f"<b>ATR:</b> {sig['atr']:.4f} | <b>Z:</b> {sig['zscore']:+.2f}\n"
        f"<b>Source:</b> {sig['source']}\n"
        f"<b>Grid:</b> ATR={optimization.get('optimal_atr_period')} "
        f"Z={optimization.get('optimal_z_threshold')}\n"
        f"<b>PF:</b> {optimization.get('profit_factor')} | "
        f"<b>WR:</b> {optimization.get('win_rate')}% | "
        f"<b>Trades:</b> {optimization.get('trades')}"
    )


def build_status_reply(active: List[Dict[str, Any]]) -> str:
    if not active:
        return "📊 <b>Status:</b> No open positions."
    lines = ["📊 <b>Active Positions:</b>", "━━━━━━━━━━━━━━━━━━━━━━"]
    for p in active:
        lines.append(
            f"#{p['id']} <b>{p['direction']}</b> "
            f"entry={p['entry']} sl={p['sl']} tp1_hit={p['tp1_hit']}")
    return "\n".join(lines)


def build_signal_reply(conn: sqlite3.Connection) -> str:
    row = conn.execute(
        "SELECT * FROM signals ORDER BY id DESC LIMIT 1").fetchone()
    if not row:
        return "📭 No signals recorded yet."
    return (
        f"📩 <b>Last Signal</b>\n"
        f"<b>Time:</b> {row['ts']}\n"
        f"<b>Decision:</b> {row['decision']}\n"
        f"<b>Entry:</b> {row['entry']} | <b>SL:</b> {row['sl']}\n"
        f"<b>TP1:</b> {row['tp1']} | <b>TP2:</b> {row['tp2']} | "
        f"<b>TP3:</b> {row['tp3']}\n"
        f"<b>Confluence:</b> {row['confluence']}"
    )


def build_help_reply() -> str:
    return (
        f"🤖 <b>{SYSTEM_NAME} v{VERSION}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"/check — live analysis\n"
        f"/status — open positions\n"
        f"/signal — last recorded signal\n"
        f"/help — this message"
    )


def handle_update(conn: sqlite3.Connection, update: Dict[str, Any],
                  engine: TrailingStopEngine) -> None:
    msg = update.get("message") or {}
    text = (msg.get("text") or "").strip()
    chat_id = (msg.get("chat") or {}).get("id")
    if not chat_id or not text:
        return
    cmd = text.split()[0].split("@")[0].lower()

    try:
        if cmd == "/check":
            df, source, degraded = MARKET_CACHE.get(force=True)
            opt = grid_search_optimize(df, conn=None)
            sig = analyze_market(df, source, degraded, opt)
            send_telegram_html(build_check_reply(sig, opt), chat_id=str(chat_id))
        elif cmd == "/status":
            send_telegram_html(build_status_reply(engine.active_positions()),
                               chat_id=str(chat_id))
        elif cmd == "/signal":
            send_telegram_html(build_signal_reply(conn), chat_id=str(chat_id))
        elif cmd in ("/help", "/start"):
            send_telegram_html(build_help_reply(), chat_id=str(chat_id))
    except Exception:
        log.exception("Command handler failed for cmd=%s", cmd)
        send_telegram_html("❌ Command failed — see logs.",
                           chat_id=str(chat_id))


def run_command_listener(conn: sqlite3.Connection, engine: TrailingStopEngine,
                          budget_seconds: int = POLL_SECONDS) -> None:
    """Long-poll Telegram for `budget_seconds`, dispatching commands."""
    if not TELEGRAM_BOT_TOKEN:
        log.warning("Listener skipped: TELEGRAM_BOT_TOKEN not set")
        return

    offset = int(get_state(conn, "tg_offset", "0") or "0")
    deadline = time.time() + budget_seconds
    log.info("Command listener active for %ds (offset=%d)",
             budget_seconds, offset)

    while time.time() < deadline:
        try:
            resp = requests.get(
                tg_api("getUpdates"),
                params={"offset": offset, "timeout": 25,
                        "allowed_updates": '["message"]'},
                timeout=30)
            data = resp.json()
            for update in data.get("result", []):
                offset = int(update["update_id"]) + 1
                set_state(conn, "tg_offset", str(offset))
                handle_update(conn, update, engine)
        except requests.RequestException as exc:
            log.warning("Poll transport failure: %s", exc)
            time.sleep(2)
        except Exception:
            log.exception("Listener iteration failed")
            time.sleep(2)

# --- END SECTION 17 ---

# ==============================================================================
# 18. CYCLE ORCHESTRATION
# ==============================================================================

def run_cycle(conn: sqlite3.Connection, engine: TrailingStopEngine,
              *, mode: str = "once", no_notify: bool = False
              ) -> Optional[Dict[str, Any]]:
    open_ok, reason = session_governor()
    if not open_ok:
        log.info("Session closed: %s", reason)
        forensic_audit_log(conn, "SESSION_CLOSED", {"reason": reason})
        return None

    try:
        df, source, degraded = MARKET_CACHE.get(force=True)
    except Exception as exc:
        log.error("Feed failure: %s", exc)
        forensic_audit_log(conn, "FEED_FAILURE",
                           {"error": str(exc)}, level="ERROR")
        raise

    optimization = grid_search_optimize(df, conn)
    forensic_audit_log(conn, "GRID_SEARCH_COMPLETE", {
        "optimal_atr_period": optimization["optimal_atr_period"],
        "optimal_z_threshold": optimization["optimal_z_threshold"],
        "profit_factor": optimization["profit_factor"],
        "win_rate": optimization["win_rate"],
        "trades": optimization["trades"],
        "cells_evaluated": optimization["cells_evaluated"],
    })

    closed = engine.refresh(df)
    if closed:
        log.info("Trailing engine closed %d position(s)", len(closed))
        forensic_audit_log(conn, "TRAILING_CLOSE", {"closed": closed})

    sig = analyze_market(df, source, degraded, optimization)
    forensic_audit_log(conn, "PILLAR_ANALYSIS", {
        "decision": sig["decision"],
        "confluence": sig["confluence"],
        "pillars": sig["pillars"],
    })

    if sig["decision"] in ("BUY", "SELL"):
        try:
            sig["signal_id"] = persist_signal(conn, sig)
            engine.register(sig)
            log.info("Registered signal #%s (%s)",
                     sig["signal_id"], sig["decision"])
        except sqlite3.Error as exc:
            log.error("Signal persistence failed: %s", exc)

    should_notify = (
        not no_notify
        and (FORCE_NOTIFY
             or sig["decision"] != "NEUTRAL"
             or NOTIFY_NEUTRAL)
    )
    if should_notify:
        alert = build_institutional_alert(sig, optimization)
        send_telegram_html(alert)
        send_discord(alert)

    if optimization.get("curve") and len(optimization["curve"]) >= 2:
        path = plot_backtest(optimization["curve"])
        if path:
            log.info("Chart written -> %s", path)

    forensic_audit_log(conn, "CYCLE_COMPLETE", {
        "decision": sig["decision"],
        "confluence": sig["confluence"],
        "notified": should_notify,
    })
    return sig

# --- END SECTION 18 ---

# ==============================================================================
# 19. MAIN ENTRYPOINT
# ==============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description=f"{SYSTEM_NAME} v{VERSION} — XAUUSD Hybrid Signal Engine")
    parser.add_argument(
        "--mode", choices=["check", "once", "listen"], default="once",
        help="check = dry-run printout; once = single cycle; listen = cycle + poll")
    parser.add_argument(
        "--no-notify", action="store_true",
        help="Suppress Telegram/Discord delivery (stdout only)")
    args = parser.parse_args()

    conn = db_connect()
    init_db(conn)
    engine = TrailingStopEngine(conn)

    try:
        if args.mode == "check":
            df, source, degraded = MARKET_CACHE.get(force=True)
            optimization = grid_search_optimize(df, conn=None)
            sig = analyze_market(df, source, degraded, optimization)
            print(json.dumps({"signal": sig, "optimization": {
                "atr_period": optimization["optimal_atr_period"],
                "z_threshold": optimization["optimal_z_threshold"],
                "profit_factor": optimization["profit_factor"],
                "win_rate": optimization["win_rate"],
                "trades": optimization["trades"],
                "cells_evaluated": optimization["cells_evaluated"],
            }}, indent=2, default=str))

        elif args.mode == "once":
            run_cycle(conn, engine, mode="once", no_notify=args.no_notify)

        elif args.mode == "listen":
            tg_register_commands()
            run_cycle(conn, engine, mode="listen", no_notify=args.no_notify)
            run_command_listener(conn, engine, budget_seconds=POLL_SECONDS)

    except Exception:
        tb = traceback.format_exc()
        log.error("Fatal: %s", tb)
        if not args.no_notify:
            send_emergency_alert(tb)
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()

# --- END SECTION 19 ---
