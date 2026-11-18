#!/usr/bin/env python3
"""
APEX-QUANT-OMNI SOVEREIGN v75.1 (Forensic Hardened & 2-Way Command Edition)
Institutional-Grade Automated XAUUSD (Spot Gold) Signal Generation,
Multi-Source Failover + Live Swissquote BBO Spread Calibration,
Grid-Search Optimization, Auto-Trailing Stop, SQLite Forensic Audit,
and Interactive 2-Way Telegram Command Engine.
"""

# CRITICAL SERVERLESS SAFEGUARD: Headless Matplotlib Backend MUST be set first
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

import argparse
import json
import logging
import os
import sqlite3
import sys
import time
import traceback
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import requests
import yfinance as yf

# ==============================================================================
# GLOBAL CONSTANTS & CONFIGURATION
# ==============================================================================
ENGINE_VERSION = "v75.1"
SYMBOL_PRIMARY = "XAUUSD=X"  # Spot Gold (Explicitly avoiding futures GC=F)
ASSET_LABEL = "XAUUSD (SPOT GOLD)"
DB_PATH = os.getenv("SQLITE_DB_PATH", "trading_signals.db")
CHART_PATH = os.getenv("BACKTEST_CHART_PATH", "backtest_performance.png")
EPSILON = 1e-8

# Account & Risk Defaults
DEFAULT_ACCOUNT_BALANCE = float(os.getenv("ACCOUNT_BALANCE", "10000.0"))
DEFAULT_RISK_PERCENT = float(os.getenv("RISK_PERCENT", "1.0"))
CONTRACT_OUNCES_PER_LOT = 100.0  # Standard 1.00 XAUUSD Lot = 100 Troy Ounces
MIN_LOT_SIZE = 0.01
MAX_LOT_SIZE = 50.00
CONFLUENCE_THRESHOLD = int(os.getenv("CONFLUENCE_THRESHOLD", "65"))
MAX_ALLOWED_SPREAD_USD = float(os.getenv("MAX_ALLOWED_SPREAD_USD", "3.00"))

# Secrets & Webhook Endpoints
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "").strip()

# Grid Search Parameter Space
ATR_PERIOD_GRID = [10, 14, 20]
Z_THRESHOLD_GRID = [1.5, 2.0, 2.5]

# Logging Setup (Silence noisy internal yfinance error dumps during clean failover)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("APEX_QUANT_OMNI")
logging.getLogger("yfinance").setLevel(logging.CRITICAL)


# ==============================================================================
# DATA STRUCTURES
# ==============================================================================
@dataclass
class SpotQuote:
    bid: float
    ask: float
    mid: float
    spread: float
    provider: str


@dataclass
class SessionStatus:
    is_allowed: bool
    session_name: str
    reason: str
    utc_timestamp: str


@dataclass
class GridSearchResult:
    optimal_atr_period: int
    optimal_z_threshold: float
    win_rate: float
    profit_factor: float
    max_drawdown: float
    expectancy_r: float
    total_trades: int
    net_return_pct: float
    equity_curve: List[float]
    timestamps: List[pd.Timestamp]
    trade_log: List[Dict[str, Any]]


@dataclass
class SignalReport:
    timestamp_utc: str
    candle_timestamp_utc: str
    asset: str
    data_source: str
    spot_quote: Optional[SpotQuote]
    session_status: SessionStatus
    signal_decision: str  # "VERIFIED BUY", "VERIFIED SELL", or "NEUTRAL / HOLD"
    entry_price: float
    stop_loss: float
    tp1: float
    tp2: float
    tp3: float
    trailing_stop_protocol: str
    confluence_score: int
    z_score: float
    smc_state: str
    order_flow_state: str
    mtf_harmony_state: str
    atr_value: float
    optimal_atr_period: int
    optimal_z_threshold: float
    recommended_lot: float
    risk_amount_usd: float
    win_rate: float
    profit_factor: float
    max_drawdown: float
    expectancy_r: float
    command_listener_status: str
    webhook_status: str
    pillar_breakdown: Dict[str, Any]


# ==============================================================================
# EXPONENTIAL BACKOFF HTTP & WEBHOOK DELIVERY ENGINE
# ==============================================================================
def request_with_exponential_backoff(
    method: str,
    url: str,
    max_retries: int = 3,
    backoff_delays: Tuple[int, ...] = (2, 4, 8),
    timeout: int = 15,
    **kwargs: Any,
) -> Optional[requests.Response]:
    """
    Executes an HTTP request with strict exponential backoff (2s, 4s, 8s)
    to handle HTTP 429 Rate Limits and transient network/5xx failures.
    """
    attempt = 0
    while attempt <= max_retries:
        try:
            resp = requests.request(method=method, url=url, timeout=timeout, **kwargs)
            if resp.status_code in (200, 201, 204):
                return resp

            # Do not waste retries on hard 404 Not Found
            if resp.status_code == 404:
                logger.warning(f"HTTP 404 on {url}; failing over immediately.")
                return resp

            if resp.status_code == 429 or 500 <= resp.status_code < 600:
                if attempt < max_retries:
                    delay = backoff_delays[min(attempt, len(backoff_delays) - 1)]
                    retry_after = resp.headers.get("Retry-After")
                    if retry_after and retry_after.isdigit():
                        delay = min(15, max(delay, int(retry_after)))
                    logger.warning(
                        f"HTTP {resp.status_code} on {url}. Retrying in {delay}s "
                        f"(attempt {attempt + 1}/{max_retries})..."
                    )
                    time.sleep(delay)
                    attempt += 1
                    continue

            logger.warning(
                f"HTTP request returned status {resp.status_code} on {url}: {resp.text[:200]}"
            )
            return resp
        except requests.RequestException as exc:
            if attempt < max_retries:
                delay = backoff_delays[min(attempt, len(backoff_delays) - 1)]
                logger.warning(
                    f"Request exception ({exc.__class__.__name__}): {exc}. "
                    f"Retrying in {delay}s (attempt {attempt + 1}/{max_retries})..."
                )
                time.sleep(delay)
                attempt += 1
            else:
                logger.error(f"Request exhausted all {max_retries} retries: {exc}")
                return None
    return None


def send_telegram_message(
    text: str,
    chat_id: Optional[str] = None,
    parse_mode: str = "HTML",
) -> bool:
    """Sends a formatted Telegram message using Exponential Backoff (2s, 4s, 8s)."""
    target_chat = (chat_id or TELEGRAM_CHAT_ID).strip()
    if not TELEGRAM_BOT_TOKEN or not target_chat:
        logger.warning("Telegram credentials not configured; skipping Telegram delivery.")
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": target_chat,
        "text": text,
        "parse_mode": parse_mode,
        "disable_web_page_preview": True,
    }
    resp = request_with_exponential_backoff(
        "POST", url, max_retries=3, backoff_delays=(2, 4, 8), json=payload
    )
    return resp is not None and resp.status_code == 200


def send_telegram_photo(
    image_path: str,
    caption: str = "",
    chat_id: Optional[str] = None,
) -> bool:
    """Uploads an image artifact to Telegram using Exponential Backoff (2s, 4s, 8s)."""
    target_chat = (chat_id or TELEGRAM_CHAT_ID).strip()
    if not TELEGRAM_BOT_TOKEN or not target_chat or not os.path.exists(image_path):
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
    delays = (2, 4, 8)
    for attempt in range(4):
        try:
            with open(image_path, "rb") as img_file:
                files = {"photo": img_file}
                data = {"chat_id": target_chat, "caption": caption[:1024], "parse_mode": "HTML"}
                resp = requests.post(url, data=data, files=files, timeout=20)
            if resp.status_code == 200:
                return True
            if (resp.status_code == 429 or 500 <= resp.status_code < 600) and attempt < 3:
                time.sleep(delays[attempt])
                continue
            logger.error(f"Telegram sendPhoto error {resp.status_code}: {resp.text[:200]}")
            return False
        except requests.RequestException as exc:
            if attempt < 3:
                time.sleep(delays[attempt])
            else:
                logger.error(f"Telegram sendPhoto failed: {exc}")
                return False
    return False


def send_discord_webhook(report: SignalReport) -> bool:
    """Sends a structured institutional embed to Discord using Exponential Backoff."""
    if not DISCORD_WEBHOOK_URL:
        logger.info("DISCORD_WEBHOOK_URL not configured; skipping Discord delivery.")
        return False

    if "BUY" in report.signal_decision:
        color = 0x10B981
    elif "SELL" in report.signal_decision:
        color = 0xEF4444
    else:
        color = 0xF59E0B

    spread_str = (
        f"Bid `{report.spot_quote.bid:.2f}` / Ask `{report.spot_quote.ask:.2f}` "
        f"(Spread `${report.spot_quote.spread:.2f}`)"
        if report.spot_quote
        else "N/A"
    )

    embed = {
        "title": f"APEX-QUANT-OMNI SOVEREIGN {ENGINE_VERSION} | {report.asset}",
        "description": (
            f"**Signal Decision:** `{report.signal_decision}`\n"
            f"**Trailing Stop Protocol:** `{report.trailing_stop_protocol}`\n"
            f"**Session Governor:** `{report.session_status.session_name}`\n"
            f"**Live Spot BBO:** {spread_str}"
        ),
        "color": color,
        "fields": [
            {"name": "Precision Entry", "value": f"`{report.entry_price:.2f}`", "inline": True},
            {"name": "Forensic Stop Loss", "value": f"`{report.stop_loss:.2f}`", "inline": True},
            {"name": "Confluence Score", "value": f"`{report.confluence_score}/100`", "inline": True},
            {"name": "TP1 (1.5R)", "value": f"`{report.tp1:.2f}`", "inline": True},
            {"name": "TP2 (2.5R)", "value": f"`{report.tp2:.2f}`", "inline": True},
            {"name": "TP3 (4.0R)", "value": f"`{report.tp3:.2f}`", "inline": True},
            {
                "name": "Risk & Sizing",
                "value": f"Lot: `{report.recommended_lot:.2f}` | Risk: `${report.risk_amount_usd:.2f}`",
                "inline": True,
            },
            {
                "name": "Grid Search Optimal",
                "value": f"ATR: `{report.optimal_atr_period}` | Z: `{report.optimal_z_threshold:.1f}`",
                "inline": True,
            },
            {
                "name": "Backtest Metrics",
                "value": (
                    f"WR: `{report.win_rate:.1f}%` | PF: `{report.profit_factor:.2f}` | "
                    f"MDD: `{report.max_drawdown:.1f}%` | Exp: `{report.expectancy_r:+.2f}R`"
                ),
                "inline": True,
            },
        ],
        "footer": {"text": f"Feed: {report.data_source} | UTC: {report.timestamp_utc}"},
    }

    payload = {"username": f"APEX-QUANT-OMNI {ENGINE_VERSION}", "embeds": [embed]}
    resp = request_with_exponential_backoff(
        "POST", DISCORD_WEBHOOK_URL, max_retries=3, backoff_delays=(2, 4, 8), json=payload
    )
    return resp is not None and resp.status_code in (200, 204)


def send_emergency_telegram_alert(context: str, error: Exception) -> None:
    """Dispatches critical exception telemetry to Telegram so silent failures never occur."""
    tb_snippet = traceback.format_exc()[-900:]
    msg = (
        f"🚨 <b>APEX-QUANT-OMNI {ENGINE_VERSION} — EMERGENCY EXCEPTION ALERT</b> 🚨\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚠️ <b>Module:</b> {context}\n"
        f"❌ <b>Error:</b> <code>{str(error)}</code>\n"
        f"🕒 <b>UTC:</b> {datetime.now(timezone.utc).isoformat()}\n"
        f"<pre>{tb_snippet}</pre>"
    )
    send_telegram_message(msg)


# ==============================================================================
# SQLITE ARTIFACT PERSISTENCE & FORENSIC AUDIT LOGGER
# ==============================================================================
def init_sqlite_db(db_path: str = DB_PATH) -> None:
    """Initializes the SQLite database schema for forensic audit, state, and trailing stops."""
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS signals_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp_utc TEXT NOT NULL,
                candle_timestamp_utc TEXT NOT NULL,
                asset TEXT NOT NULL,
                data_source TEXT NOT NULL,
                session_name TEXT NOT NULL,
                session_allowed INTEGER NOT NULL,
                signal_decision TEXT NOT NULL,
                entry_price REAL NOT NULL,
                stop_loss REAL NOT NULL,
                tp1 REAL NOT NULL,
                tp2 REAL NOT NULL,
                tp3 REAL NOT NULL,
                trailing_stop_protocol TEXT NOT NULL,
                confluence_score INTEGER NOT NULL,
                z_score REAL NOT NULL,
                smc_state TEXT NOT NULL,
                order_flow_state TEXT NOT NULL,
                mtf_harmony_state TEXT NOT NULL,
                atr_value REAL NOT NULL,
                optimal_atr_period INTEGER NOT NULL,
                optimal_z_threshold REAL NOT NULL,
                recommended_lot REAL NOT NULL,
                risk_amount_usd REAL NOT NULL,
                win_rate REAL NOT NULL,
                profit_factor REAL NOT NULL,
                max_drawdown REAL NOT NULL,
                webhook_status TEXT NOT NULL,
                raw_metrics_json TEXT NOT NULL
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS active_positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                opened_utc TEXT NOT NULL,
                direction TEXT NOT NULL,
                entry_price REAL NOT NULL,
                initial_sl REAL NOT NULL,
                current_sl REAL NOT NULL,
                tp1 REAL NOT NULL,
                tp2 REAL NOT NULL,
                tp3 REAL NOT NULL,
                atr_at_entry REAL NOT NULL,
                tp1_hit INTEGER NOT NULL DEFAULT 0,
                peak_favorable_price REAL NOT NULL,
                status TEXT NOT NULL DEFAULT 'OPEN',
                updated_utc TEXT NOT NULL
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS bot_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


def get_bot_state(key: str, default: str = "", db_path: str = DB_PATH) -> str:
    init_sqlite_db(db_path)
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT value FROM bot_state WHERE key = ?", (key,))
        row = cur.fetchone()
        return row[0] if row else default
    finally:
        conn.close()


def set_bot_state(key: str, value: str, db_path: str = DB_PATH) -> None:
    init_sqlite_db(db_path)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO bot_state (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )
        conn.commit()
    finally:
        conn.close()


def forensic_audit_log(report: SignalReport, db_path: str = DB_PATH) -> None:
    """
    Outputs raw quantitative metrics to console AND records the full signal
    event into the local SQLite database (`trading_signals.db`).
    """
    init_sqlite_db(db_path)

    raw_payload = {
        "pillar_breakdown": report.pillar_breakdown,
        "session": asdict(report.session_status),
        "spot_quote": asdict(report.spot_quote) if report.spot_quote else None,
        "z_score": report.z_score,
        "atr_value": report.atr_value,
        "expectancy_r": report.expectancy_r,
        "optimal_atr_period": report.optimal_atr_period,
        "optimal_z_threshold": report.optimal_z_threshold,
    }

    spread_log = (
        f"Bid={report.spot_quote.bid:.2f} Ask={report.spot_quote.ask:.2f} Spread=${report.spot_quote.spread:.2f}"
        if report.spot_quote
        else "SpotBBO=Unavailable"
    )

    logger.info("========== FORENSIC AUDIT LOG ==========")
    logger.info(
        f"Asset={report.asset} | Decision={report.signal_decision} | "
        f"Score={report.confluence_score}/100 | Source={report.data_source}"
    )
    logger.info(
        f"Entry={report.entry_price:.2f} | SL={report.stop_loss:.2f} | "
        f"TP1={report.tp1:.2f} | TP2={report.tp2:.2f} | TP3={report.tp3:.2f} | {spread_log}"
    )
    logger.info(
        f"Z-Score={report.z_score:.4f} | ATR={report.atr_value:.4f} | "
        f"SMC={report.smc_state} | OrderFlow={report.order_flow_state} | "
        f"MTF={report.mtf_harmony_state}"
    )
    logger.info(
        f"GridSearch(ATR={report.optimal_atr_period}, Z={report.optimal_z_threshold:.1f}) | "
        f"WR={report.win_rate:.1f}% | PF={report.profit_factor:.2f} | "
        f"MDD={report.max_drawdown:.1f}% | Exp={report.expectancy_r:+.2f}R"
    )
    logger.info(
        f"Lot={report.recommended_lot:.2f} | RiskUSD=${report.risk_amount_usd:.2f} | "
        f"Trailing={report.trailing_stop_protocol} | Webhook={report.webhook_status}"
    )
    logger.info("========================================")

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO signals_audit (
                timestamp_utc, candle_timestamp_utc, asset, data_source,
                session_name, session_allowed, signal_decision, entry_price,
                stop_loss, tp1, tp2, tp3, trailing_stop_protocol,
                confluence_score, z_score, smc_state, order_flow_state,
                mtf_harmony_state, atr_value, optimal_atr_period,
                optimal_z_threshold, recommended_lot, risk_amount_usd,
                win_rate, profit_factor, max_drawdown, webhook_status,
                raw_metrics_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                report.timestamp_utc,
                report.candle_timestamp_utc,
                report.asset,
                report.data_source,
                report.session_status.session_name,
                1 if report.session_status.is_allowed else 0,
                report.signal_decision,
                float(report.entry_price),
                float(report.stop_loss),
                float(report.tp1),
                float(report.tp2),
                float(report.tp3),
                report.trailing_stop_protocol,
                int(report.confluence_score),
                float(report.z_score),
                report.smc_state,
                report.order_flow_state,
                report.mtf_harmony_state,
                float(report.atr_value),
                int(report.optimal_atr_period),
                float(report.optimal_z_threshold),
                float(report.recommended_lot),
                float(report.risk_amount_usd),
                float(report.win_rate),
                float(report.profit_factor),
                float(report.max_drawdown),
                report.webhook_status,
                json.dumps(raw_payload),
            ),
        )
        conn.commit()
    finally:
        conn.close()


# ==============================================================================
# MARKET SESSION GOVERNOR & TIMING SAFETY
# ==============================================================================
def evaluate_market_session(
    now_utc: Optional[datetime] = None,
    spot_quote: Optional[SpotQuote] = None,
) -> SessionStatus:
    """
    Verifies UTC time, day of week, and live BBO spread. Suppresses signal execution during:
      - Weekend market closure (Fri >= 21:00 UTC through Sun < 23:00 UTC)
      - Daily rollover window (21:00 - 22:00 UTC)
      - Low-liquidity Asian open transition noise (22:00 - 23:00 UTC)
      - Abnormal live spread spike (> MAX_ALLOWED_SPREAD_USD)
    """
    now = now_utc or datetime.now(timezone.utc)
    weekday = now.weekday()  # 0=Mon ... 4=Fri, 5=Sat, 6=Sun
    hour = now.hour
    ts_str = now.isoformat()

    if weekday == 4 and hour >= 21:
        return SessionStatus(
            is_allowed=False,
            session_name="WEEKEND_CLOSE_GUARD",
            reason="Friday close window (>=21:00 UTC) suppressed to prevent weekend gap & spread slippage.",
            utc_timestamp=ts_str,
        )
    if weekday == 5:
        return SessionStatus(
            is_allowed=False,
            session_name="WEEKEND_CLOSED",
            reason="Saturday spot gold market closed.",
            utc_timestamp=ts_str,
        )
    if weekday == 6 and hour < 23:
        return SessionStatus(
            is_allowed=False,
            session_name="SUNDAY_OPEN_GUARD",
            reason="Sunday pre-open / illiquid initial open (<23:00 UTC) suppressed.",
            utc_timestamp=ts_str,
        )

    if 21 <= hour < 22:
        return SessionStatus(
            is_allowed=False,
            session_name="DAILY_ROLLOVER_GUARD",
            reason="Daily FX/Gold rollover hour (21:00-22:00 UTC) suppressed due to spread expansion.",
            utc_timestamp=ts_str,
        )
    if 22 <= hour < 23:
        return SessionStatus(
            is_allowed=False,
            session_name="ASIAN_OPEN_NOISE_GUARD",
            reason="Low-liquidity Asian open transition (22:00-23:00 UTC) suppressed.",
            utc_timestamp=ts_str,
        )

    if spot_quote is not None and spot_quote.spread > MAX_ALLOWED_SPREAD_USD:
        return SessionStatus(
            is_allowed=False,
            session_name="SPREAD_SPIKE_GUARD",
            reason=(
                f"Live spot spread ${spot_quote.spread:.2f} exceeds guard "
                f"limit ${MAX_ALLOWED_SPREAD_USD:.2f}."
            ),
            utc_timestamp=ts_str,
        )

    if 0 <= hour < 7:
        session_name = "ASIAN_SESSION_ACTIVE"
    elif 7 <= hour < 12:
        session_name = "LONDON_SESSION_PRIME"
    elif 12 <= hour < 16:
        session_name = "LONDON_NY_OVERLAP_PEAK"
    else:
        session_name = "NEW_YORK_SESSION_ACTIVE"

    return SessionStatus(
        is_allowed=True,
        session_name=session_name,
        reason="Active institutional liquidity window & tight spread verified.",
        utc_timestamp=ts_str,
    )


# ==============================================================================
# MULTI-SOURCE FREE FALLBACK DATA PIPELINE (MT5 SPOT GOLD SYNC & FAILOVER)
# ==============================================================================
def _normalize_ohlcv_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Flattens yfinance MultiIndex columns, enforces float dtypes, normalizes UTC
    DatetimeIndex, and applies EffectiveVolume safeguard when spot volume is 0.
    Uses immutable Pandas operations (zero Pandas4Warning deprecations).
    """
    if df is None or df.empty:
        return pd.DataFrame()

    df = df.copy()
    # MANDATORY SAFEGUARD: Flatten MultiIndex columns automatically
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    df = df.loc[:, ~df.columns.duplicated()]
    rename_map = {c: str(c).strip().title() for c in df.columns}
    df = df.rename(columns=rename_map)

    required = ["Open", "High", "Low", "Close"]
    for col in required:
        if col not in df.columns:
            return pd.DataFrame()

    if "Volume" not in df.columns:
        df["Volume"] = 0.0

    df = df[["Open", "High", "Low", "Close", "Volume"]].copy()
    for col in ["Open", "High", "Low", "Close", "Volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["Open", "High", "Low", "Close"])
    df["Volume"] = df["Volume"].fillna(0.0)

    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index, utc=True)
    else:
        if df.index.tz is None:
            df.index = df.index.tz_localize("UTC")
        else:
            df.index = df.index.tz_convert("UTC")

    df = df.sort_index()
    df = df[~df.index.duplicated(keep="last")]

    # Spot Gold feeds without tick volume use range-intensity proxy
    range_proxy = ((df["High"] - df["Low"]).abs() + (df["Close"] - df["Open"]).abs() + EPSILON) * 1000.0
    df["EffectiveVolume"] = np.where(df["Volume"] > 0.0, df["Volume"], range_proxy)
    return df


def fetch_swissquote_live_spot_quote() -> Optional[SpotQuote]:
    """Queries Swissquote free public BBO feed for live MT5-grade XAU/USD Bid/Ask/Spread."""
    url = "https://forex-data-feed.swissquote.com/public-quotes/bboquotes/instrument/XAU/USD"
    resp = request_with_exponential_backoff(
        "GET", url, max_retries=1, backoff_delays=(2,), timeout=8
    )
    if resp is None or resp.status_code != 200:
        return None
    try:
        data = resp.json()
        if isinstance(data, list) and len(data) > 0:
            spreads = data[0].get("spreadProfilePrices", [])
            if spreads:
                bid = float(spreads[0]["bid"])
                ask = float(spreads[0]["ask"])
                if bid > 100.0 and ask >= bid:
                    return SpotQuote(
                        bid=round(bid, 2),
                        ask=round(ask, 2),
                        mid=round((bid + ask) / 2.0, 2),
                        spread=round(ask - bid, 2),
                        provider="Swissquote Institutional BBO",
                    )
    except (ValueError, KeyError, IndexError, TypeError):
        return None
    return None


def _calibrate_dataframe_to_live_spot(df: pd.DataFrame, spot_quote: Optional[SpotQuote]) -> pd.DataFrame:
    """Applies exact basis calibration to align proxy/feed closing price with live XAU/USD spot mid."""
    if df.empty or spot_quote is None:
        return df
    last_close = float(df["Close"].iloc[-1])
    basis_ratio = spot_quote.mid / (last_close + EPSILON)
    if 0.95 <= basis_ratio <= 1.05:
        df = df.copy()
        for col in ("Open", "High", "Low", "Close"):
            df[col] = df[col] * basis_ratio
    return df


def _fetch_from_yfinance_pair(ticker: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Primary Probe: yfinance official Spot Gold ticker XAUUSD=X (fail-fast on 15M)."""
    try:
        raw_15m = yf.download(
            tickers=ticker, period="30d", interval="15m", progress=False, auto_adjust=False, threads=False
        )
        df_15m = _normalize_ohlcv_dataframe(raw_15m)
        if len(df_15m) < 120:
            return pd.DataFrame(), pd.DataFrame()
        raw_1h = yf.download(
            tickers=ticker, period="60d", interval="1h", progress=False, auto_adjust=False, threads=False
        )
        df_1h = _normalize_ohlcv_dataframe(raw_1h)
        return df_15m, df_1h
    except Exception:
        return pd.DataFrame(), pd.DataFrame()


def _fetch_from_yahoo_chart_http(ticker: str, interval: str, range_str: str) -> pd.DataFrame:
    """Secondary Feed: Fast Yahoo Finance v8 HTTP Chart API probe (1 retry max to avoid 25s stalls)."""
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
        )
    }
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
    params = {"interval": interval, "range": range_str, "includePrePost": "false"}
    resp = request_with_exponential_backoff(
        "GET", url, max_retries=1, backoff_delays=(2,), timeout=10, headers=headers, params=params
    )
    if resp is None or resp.status_code != 200:
        return pd.DataFrame()
    try:
        payload = resp.json()
        result = payload["chart"]["result"][0]
        timestamps = result["timestamp"]
        quote = result["indicators"]["quote"][0]
        df = pd.DataFrame(
            {
                "Open": quote["open"],
                "High": quote["high"],
                "Low": quote["low"],
                "Close": quote["close"],
                "Volume": quote.get("volume", [0.0] * len(timestamps)),
            },
            index=pd.to_datetime(timestamps, unit="s", utc=True),
        )
        return _normalize_ohlcv_dataframe(df)
    except (KeyError, IndexError, TypeError, ValueError):
        return pd.DataFrame()


def _fetch_from_yahoo_http_pair(ticker: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    df_15m = _fetch_from_yahoo_chart_http(ticker, "15m", "1mo")
    if len(df_15m) < 120:
        return pd.DataFrame(), pd.DataFrame()
    df_1h = _fetch_from_yahoo_chart_http(ticker, "1h", "3mo")
    return df_15m, df_1h


def _fetch_from_binance_spot_gold(interval: str, limit: int, spot_quote: Optional[SpotQuote]) -> pd.DataFrame:
    """
    Tertiary Feed: Binance Vision public spot gold PAXGUSDT (1:1 troy oz),
    calibrated to live Swissquote XAU/USD spot mid-price.
    """
    url = "https://data-api.binance.vision/api/v3/klines"
    params = {"symbol": "PAXGUSDT", "interval": interval, "limit": limit}
    resp = request_with_exponential_backoff(
        "GET", url, max_retries=2, backoff_delays=(2, 4), timeout=12, params=params
    )
    if resp is None or resp.status_code != 200:
        return pd.DataFrame()
    try:
        rows = resp.json()
        if not isinstance(rows, list) or len(rows) < 50:
            return pd.DataFrame()
        df = pd.DataFrame(
            {
                "Open": [float(r[1]) for r in rows],
                "High": [float(r[2]) for r in rows],
                "Low": [float(r[3]) for r in rows],
                "Close": [float(r[4]) for r in rows],
                "Volume": [float(r[5]) for r in rows],
            },
            index=pd.to_datetime([int(r[0]) for r in rows], unit="ms", utc=True),
        )
        return _calibrate_dataframe_to_live_spot(_normalize_ohlcv_dataframe(df), spot_quote)
    except (ValueError, KeyError, IndexError, TypeError):
        return pd.DataFrame()


def _fetch_from_kraken_spot_gold(interval_min: int, spot_quote: Optional[SpotQuote]) -> pd.DataFrame:
    """Quaternary Failover Feed: Kraken public OHLC API for PAXGUSD."""
    url = "https://api.kraken.com/0/public/OHLC"
    params = {"pair": "PAXGUSD", "interval": interval_min}
    resp = request_with_exponential_backoff(
        "GET", url, max_retries=2, backoff_delays=(2, 4), timeout=12, params=params
    )
    if resp is None or resp.status_code != 200:
        return pd.DataFrame()
    try:
        result = resp.json().get("result", {})
        pair_key = next((k for k in result.keys() if k != "last"), None)
        if not pair_key:
            return pd.DataFrame()
        rows = result[pair_key]
        df = pd.DataFrame(
            {
                "Open": [float(r[1]) for r in rows],
                "High": [float(r[2]) for r in rows],
                "Low": [float(r[3]) for r in rows],
                "Close": [float(r[4]) for r in rows],
                "Volume": [float(r[6]) for r in rows],
            },
            index=pd.to_datetime([int(r[0]) for r in rows], unit="s", utc=True),
        )
        return _calibrate_dataframe_to_live_spot(_normalize_ohlcv_dataframe(df), spot_quote)
    except (ValueError, KeyError, IndexError, TypeError):
        return pd.DataFrame()


def fetch_xauusd_mtf_data() -> Tuple[pd.DataFrame, pd.DataFrame, str, Optional[SpotQuote]]:
    """
    Fault-tolerant Multi-Source Data Ingestion for 15M execution and 1H macro data.
    Sequentially queries:
      1. yfinance Spot Gold (XAUUSD=X)
      2. Yahoo v8 HTTP Chart API (XAUUSD=X)
      3. Binance Vision Spot Gold (PAXGUSDT) + Swissquote BBO Spot Calibration
      4. Kraken Public Spot Gold (PAXGUSD) + Swissquote BBO Spot Calibration
    """
    spot_quote = fetch_swissquote_live_spot_quote()
    if spot_quote:
        logger.info(
            f"Live Spot BBO Verified [{spot_quote.provider}]: "
            f"Bid={spot_quote.bid:.2f} | Ask={spot_quote.ask:.2f} | Spread=${spot_quote.spread:.2f}"
        )

    sources = [
        (
            "Nominal (Primary: yfinance XAUUSD=X)",
            lambda: _fetch_from_yfinance_pair(SYMBOL_PRIMARY),
        ),
        (
            "Failover Active (Secondary: Yahoo v8 HTTP XAUUSD=X)",
            lambda: _fetch_from_yahoo_http_pair(SYMBOL_PRIMARY),
        ),
        (
            "Failover Active (Tertiary: Spot PAXG/XAUUSD + Swissquote BBO)",
            lambda: (
                _fetch_from_binance_spot_gold("15m", 1000, spot_quote),
                _fetch_from_binance_spot_gold("1h", 750, spot_quote),
            ),
        ),
        (
            "Failover Active (Quaternary: Kraken PAXGUSD + Swissquote BBO)",
            lambda: (
                _fetch_from_kraken_spot_gold(15, spot_quote),
                _fetch_from_kraken_spot_gold(60, spot_quote),
            ),
        ),
    ]

    for source_label, loader in sources:
        try:
            df_15m, df_1h = loader()
            if len(df_15m) >= 120 and len(df_1h) >= 60:
                logger.info(
                    f"Data ingestion succeeded via [{source_label}] | "
                    f"15M bars={len(df_15m)} | 1H bars={len(df_1h)}"
                )
                return df_15m, df_1h, source_label, spot_quote
            logger.info(f"Feed [{source_label}] unavailable or rate-limited; advancing failover...")
        except Exception as exc:
            logger.warning(f"Feed [{source_label}] raised exception: {exc}. Advancing failover...")

    raise RuntimeError(
        "CRITICAL DATA OUTAGE: All multi-source Spot Gold (XAUUSD) feeds failed."
    )


# ==============================================================================
# TECHNICAL INDICATORS & 5-PILLAR HYBRID QUANTITATIVE ENGINE
# ==============================================================================
def compute_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """Computes Wilder-smoothed Average True Range with zero-division guard."""
    high = df["High"]
    low = df["Low"]
    prev_close = df["Close"].shift(1)
    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    true_range = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = true_range.ewm(alpha=1.0 / max(period, 1), adjust=False).mean()
    return atr.fillna(true_range).clip(lower=EPSILON)


def compute_rolling_zscore(series: pd.Series, window: int = 20) -> pd.Series:
    """Computes rolling Z-Score with mandatory epsilon protection against zero std."""
    rolling_mean = series.rolling(window=window, min_periods=max(5, window // 2)).mean()
    rolling_std = series.rolling(window=window, min_periods=max(5, window // 2)).std()
    z_score = (series - rolling_mean) / (rolling_std + EPSILON)
    return z_score.fillna(0.0)


def analyze_pillar_1_smc(df: pd.DataFrame, lookback: int = 80) -> Dict[str, Any]:
    """
    Pillar I (Smart Money Concepts):
    Detects confirmed Swing Highs / Swing Lows (fractal k=3), Break of Structure (BOS),
    and Change of Character (ChoCh). Max 25 points.
    """
    sub = df.tail(lookback).copy()
    highs = sub["High"].values
    lows = sub["Low"].values
    closes = sub["Close"].values
    n = len(sub)

    swing_highs: List[Tuple[int, float]] = []
    swing_lows: List[Tuple[int, float]] = []
    k = 3
    for i in range(k, n - k):
        if highs[i] == np.max(highs[i - k : i + k + 1]):
            swing_highs.append((i, float(highs[i])))
        if lows[i] == np.min(lows[i - k : i + k + 1]):
            swing_lows.append((i, float(lows[i])))

    last_close = float(closes[-1])
    prev_close = float(closes[-2]) if n >= 2 else last_close

    if len(swing_highs) < 2 or len(swing_lows) < 2:
        return {"bias": "NEUTRAL", "points": 8, "state": "SMC_CONSOLIDATION", "event": "NONE"}

    sh_prev, sh_last = swing_highs[-2][1], swing_highs[-1][1]
    sl_prev, sl_last = swing_lows[-2][1], swing_lows[-1][1]

    higher_highs = sh_last > sh_prev
    higher_lows = sl_last > sl_prev
    lower_highs = sh_last < sh_prev
    lower_lows = sl_last < sl_prev

    bull_bos = last_close > sh_last and prev_close <= sh_last
    bear_bos = last_close < sl_last and prev_close >= sl_last
    bull_choch = lower_lows and last_close > sh_last
    bear_choch = higher_highs and last_close < sl_last

    if bull_choch:
        return {"bias": "BULLISH", "points": 25, "state": "BULLISH_CHOCH_REVERSAL", "event": "CHOCH_UP"}
    if bear_choch:
        return {"bias": "BEARISH", "points": 25, "state": "BEARISH_CHOCH_REVERSAL", "event": "CHOCH_DOWN"}
    if bull_bos or (higher_highs and higher_lows and last_close > sl_last):
        pts = 24 if bull_bos else 19
        ev = "BOS_UP" if bull_bos else "HH_HL_STRUCTURE"
        return {"bias": "BULLISH", "points": pts, "state": f"BULLISH_{ev}", "event": ev}
    if bear_bos or (lower_highs and lower_lows and last_close < sh_last):
        pts = 24 if bear_bos else 19
        ev = "BOS_DOWN" if bear_bos else "LH_LL_STRUCTURE"
        return {"bias": "BEARISH", "points": pts, "state": f"BEARISH_{ev}", "event": ev}

    if last_close > (sh_last + sl_last) / 2.0:
        return {"bias": "BULLISH", "points": 12, "state": "UPPER_RANGE_PREMIUM", "event": "RANGE_BULL"}
    return {"bias": "BEARISH", "points": 12, "state": "LOWER_RANGE_DISCOUNT", "event": "RANGE_BEAR"}


def analyze_pillar_2_order_flow(df: pd.DataFrame, window: int = 10) -> Dict[str, Any]:
    """
    Pillar II (Order Flow):
    Computes volume-weighted body-to-wick ratios and directional pressure delta.
    Max 20 points.
    """
    sub = df.tail(window).copy()
    body = sub["Close"] - sub["Open"]
    abs_body = body.abs()
    upper_wick = sub["High"] - sub[["Open", "Close"]].max(axis=1)
    lower_wick = sub[["Open", "Close"]].min(axis=1) - sub["Low"]
    total_range = (sub["High"] - sub["Low"]).clip(lower=EPSILON)

    vol = sub["EffectiveVolume"]
    vol_weights = vol / (vol.sum() + EPSILON)

    bar_pressure = (body + 0.5 * (lower_wick - upper_wick)) / (total_range + EPSILON)
    vw_pressure = float((bar_pressure * vol_weights).sum())
    body_efficiency = float(((abs_body / (total_range + EPSILON)) * vol_weights).sum())

    if vw_pressure > 0.15:
        pts = min(20, int(12 + vw_pressure * 14 + body_efficiency * 4))
        return {
            "bias": "BULLISH",
            "points": pts,
            "state": f"BULL_IMBALANCE ({vw_pressure:+.2f})",
            "vw_pressure": vw_pressure,
        }
    if vw_pressure < -0.15:
        pts = min(20, int(12 + abs(vw_pressure) * 14 + body_efficiency * 4))
        return {
            "bias": "BEARISH",
            "points": pts,
            "state": f"BEAR_IMBALANCE ({vw_pressure:+.2f})",
            "vw_pressure": vw_pressure,
        }
    return {
        "bias": "NEUTRAL",
        "points": 8,
        "state": f"BALANCED_ABSORPTION ({vw_pressure:+.2f})",
        "vw_pressure": vw_pressure,
    }


def analyze_pillar_3_zscore(
    df: pd.DataFrame,
    z_threshold: float,
    window: int = 20,
) -> Dict[str, Any]:
    """
    Pillar III (Z-Score Mean Reversion & Statistical Regime):
    Evaluates rolling Z-Score with division-by-zero guard against optimal threshold.
    Max 20 points.
    """
    z_series = compute_rolling_zscore(df["Close"], window=window)
    z_now = float(z_series.iloc[-1])
    z_prev = float(z_series.iloc[-2]) if len(z_series) >= 2 else z_now

    if z_now <= -z_threshold:
        pts = 20 if z_now > z_prev else 17
        return {"bias": "BULLISH", "points": pts, "z_score": z_now, "state": "OVERSOLD_REVERSION"}
    if z_now >= z_threshold:
        pts = 20 if z_now < z_prev else 17
        return {"bias": "BEARISH", "points": pts, "z_score": z_now, "state": "OVERBOUGHT_REVERSION"}
    if -z_threshold < z_now <= -0.5 and z_now > z_prev:
        return {"bias": "BULLISH", "points": 15, "z_score": z_now, "state": "DISCOUNT_RECOVERY"}
    if 0.5 <= z_now < z_threshold and z_now < z_prev:
        return {"bias": "BEARISH", "points": 15, "z_score": z_now, "state": "PREMIUM_FADE"}
    if z_now > 0.0:
        return {"bias": "BULLISH", "points": 10, "z_score": z_now, "state": "POSITIVE_DRIFT"}
    return {"bias": "BEARISH", "points": 10, "z_score": z_now, "state": "NEGATIVE_DRIFT"}


def analyze_pillar_4_mtf_harmony(df_15m: pd.DataFrame, df_1h: pd.DataFrame) -> Dict[str, Any]:
    """
    Pillar IV (Multi-Timeframe Fractal Harmony):
    Synchronizes 1-Hour macro trend (EMA 20/50 + 1H SMC) with 15-Minute execution flow.
    Max 20 points.
    """
    ema20_1h = df_1h["Close"].ewm(span=20, adjust=False).mean()
    ema50_1h = df_1h["Close"].ewm(span=50, adjust=False).mean()
    ema20_15m = df_15m["Close"].ewm(span=20, adjust=False).mean()
    ema50_15m = df_15m["Close"].ewm(span=50, adjust=False).mean()

    macro_bull = float(ema20_1h.iloc[-1]) > float(ema50_1h.iloc[-1]) and float(df_1h["Close"].iloc[-1]) > float(ema20_1h.iloc[-1])
    macro_bear = float(ema20_1h.iloc[-1]) < float(ema50_1h.iloc[-1]) and float(df_1h["Close"].iloc[-1]) < float(ema20_1h.iloc[-1])

    exec_bull = float(ema20_15m.iloc[-1]) > float(ema50_15m.iloc[-1])
    exec_bear = float(ema20_15m.iloc[-1]) < float(ema50_15m.iloc[-1])

    smc_1h = analyze_pillar_1_smc(df_1h, lookback=60)

    if macro_bull and exec_bull and smc_1h["bias"] == "BULLISH":
        return {"bias": "BULLISH", "points": 20, "state": "1H_15M_FULL_BULL_HARMONY"}
    if macro_bear and exec_bear and smc_1h["bias"] == "BEARISH":
        return {"bias": "BEARISH", "points": 20, "state": "1H_15M_FULL_BEAR_HARMONY"}
    if (macro_bull and exec_bull) or (smc_1h["bias"] == "BULLISH" and exec_bull):
        return {"bias": "BULLISH", "points": 15, "state": "1H_15M_PARTIAL_BULL_ALIGN"}
    if (macro_bear and exec_bear) or (smc_1h["bias"] == "BEARISH" and exec_bear):
        return {"bias": "BEARISH", "points": 15, "state": "1H_15M_PARTIAL_BEAR_ALIGN"}

    return {"bias": "NEUTRAL", "points": 6, "state": "MTF_DIVERGENCE"}


def analyze_pillar_5_atr_risk(
    df_15m: pd.DataFrame,
    direction: str,
    optimal_atr_period: int,
    spot_quote: Optional[SpotQuote] = None,
    account_balance: float = DEFAULT_ACCOUNT_BALANCE,
    risk_percent: float = DEFAULT_RISK_PERCENT,
) -> Dict[str, Any]:
    """
    Pillar V (ATR Risk & Dynamic Sizing):
    Uses ATR(14) blended with Grid Search optimal ATR period to compute forensic SL,
    TP1 (1.5R), TP2 (2.5R), TP3 (4.0R, min RRR >= 1:3), and dynamic lot sizing.
    Uses live Swissquote Ask/Bid entry when available for institutional realism.
    Max 15 points.
    """
    atr_14 = float(compute_atr(df_15m, period=14).iloc[-1])
    atr_opt = float(compute_atr(df_15m, period=optimal_atr_period).iloc[-1])
    atr_exec = (atr_14 + atr_opt) / 2.0

    close_last = float(df_15m["Close"].iloc[-1])
    if spot_quote is not None:
        entry = spot_quote.ask if direction == "BULLISH" else spot_quote.bid
    else:
        entry = close_last

    atr_series = compute_atr(df_15m, period=14)
    median_atr = float(atr_series.tail(100).median()) + EPSILON
    vol_ratio = atr_14 / median_atr

    risk_dist = max(atr_exec * 1.5, 1.50)
    if direction == "BEARISH":
        sl = entry + risk_dist
        tp1 = entry - (1.5 * risk_dist)
        tp2 = entry - (2.5 * risk_dist)
        tp3 = entry - (4.0 * risk_dist)
    else:
        sl = entry - risk_dist
        tp1 = entry + (1.5 * risk_dist)
        tp2 = entry + (2.5 * risk_dist)
        tp3 = entry + (4.0 * risk_dist)

    risk_usd = account_balance * (risk_percent / 100.0)
    raw_lots = risk_usd / ((risk_dist + EPSILON) * CONTRACT_OUNCES_PER_LOT)
    recommended_lot = round(min(MAX_LOT_SIZE, max(MIN_LOT_SIZE, raw_lots)), 2)

    if 0.6 <= vol_ratio <= 2.2:
        pts = 15
        vol_state = "OPTIMAL_ATR_REGIME"
    elif 0.35 <= vol_ratio <= 3.0:
        pts = 11
        vol_state = "MODERATE_ATR_REGIME"
    else:
        pts = 5
        vol_state = "EXTREME_VOLATILITY_GUARD"

    return {
        "entry": round(entry, 2),
        "sl": round(sl, 2),
        "tp1": round(tp1, 2),
        "tp2": round(tp2, 2),
        "tp3": round(tp3, 2),
        "atr_14": round(atr_14, 4),
        "atr_exec": round(atr_exec, 4),
        "risk_distance": round(risk_dist, 2),
        "recommended_lot": recommended_lot,
        "risk_amount_usd": round(risk_usd, 2),
        "points": pts,
        "vol_state": vol_state,
    }


# ==============================================================================
# GRID SEARCH PARAMETER OPTIMIZATION & MULTI-TIER BACKTESTER
# ==============================================================================
def _simulate_strategy_backtest(
    df_15m: pd.DataFrame,
    df_1h: pd.DataFrame,
    atr_period: int,
    z_threshold: float,
    initial_capital: float = DEFAULT_ACCOUNT_BALANCE,
    risk_pct: float = DEFAULT_RISK_PERCENT,
) -> GridSearchResult:
    """
    Institutional multi-tier exit simulator:
      - 50% scaled out at TP1 (1.5R) -> SL immediately shifted to Breakeven
      - 25% scaled out at TP2 (2.5R) -> SL trails behind TP1 / ATR step
      - 25% runner targeted at TP3 (4.0R) with fractional ATR trailing stop
    """
    df = df_15m.copy()
    df["ATR"] = compute_atr(df, period=atr_period)
    df["Z"] = compute_rolling_zscore(df["Close"], window=20)
    df["EMA20"] = df["Close"].ewm(span=20, adjust=False).mean()
    df["EMA50"] = df["Close"].ewm(span=50, adjust=False).mean()

    h1 = df_1h[["Close"]].copy()
    h1["H1_EMA20"] = h1["Close"].ewm(span=20, adjust=False).mean()
    h1["H1_EMA50"] = h1["Close"].ewm(span=50, adjust=False).mean()
    h1["H1_BIAS"] = np.where(h1["H1_EMA20"] > h1["H1_EMA50"], 1, -1)
    h1_shifted = h1[["H1_BIAS"]].shift(1).reindex(df.index, method="ffill").fillna(0)
    df["H1_BIAS"] = h1_shifted["H1_BIAS"]

    total_range = (df["High"] - df["Low"]).clip(lower=EPSILON)
    df["BAR_FLOW"] = (df["Close"] - df["Open"]) / total_range

    equity = initial_capital
    peak_equity = initial_capital
    max_drawdown_pct = 0.0
    equity_curve: List[float] = [initial_capital]
    timestamps: List[pd.Timestamp] = [df.index[0]]
    trade_log: List[Dict[str, Any]] = []

    gross_profit = 0.0
    gross_loss = 0.0
    wins = 0
    losses = 0
    total_r = 0.0

    in_pos = False
    pos_dir = 0
    entry_p = 0.0
    sl_p = 0.0
    tp1_p = 0.0
    tp2_p = 0.0
    tp3_p = 0.0
    risk_dist = 1.0
    pos_atr = 1.0
    remaining_weight = 1.0
    realized_r = 0.0
    tp1_hit = False
    tp2_hit = False
    risk_usd = 0.0
    entry_time: Optional[pd.Timestamp] = None

    opens = df["Open"].values
    highs = df["High"].values
    lows = df["Low"].values
    closes = df["Close"].values
    atrs = df["ATR"].values
    zs = df["Z"].values
    ema20s = df["EMA20"].values
    ema50s = df["EMA50"].values
    h1_biases = df["H1_BIAS"].values
    flows = df["BAR_FLOW"].values
    idx_list = df.index

    start_bar = max(60, atr_period + 5)
    for i in range(start_bar, len(df)):
        if in_pos:
            trade_closed = False
            exit_p = closes[i]

            if pos_dir == 1:
                if lows[i] <= sl_p:
                    rem_r = (sl_p - entry_p) / (risk_dist + EPSILON)
                    realized_r += remaining_weight * rem_r
                    exit_p = sl_p
                    trade_closed = True
                else:
                    if not tp1_hit and highs[i] >= tp1_p:
                        tp1_hit = True
                        realized_r += 0.50 * 1.5
                        remaining_weight -= 0.50
                        sl_p = max(sl_p, entry_p)
                    if not tp2_hit and highs[i] >= tp2_p:
                        tp2_hit = True
                        realized_r += 0.25 * 2.5
                        remaining_weight -= 0.25
                        sl_p = max(sl_p, tp1_p)
                    if highs[i] >= tp3_p:
                        realized_r += remaining_weight * 4.0
                        exit_p = tp3_p
                        trade_closed = True
                    elif tp1_hit:
                        trail_step = highs[i] - (1.0 * pos_atr)
                        sl_p = max(sl_p, trail_step)
            else:
                if highs[i] >= sl_p:
                    rem_r = (entry_p - sl_p) / (risk_dist + EPSILON)
                    realized_r += remaining_weight * rem_r
                    exit_p = sl_p
                    trade_closed = True
                else:
                    if not tp1_hit and lows[i] <= tp1_p:
                        tp1_hit = True
                        realized_r += 0.50 * 1.5
                        remaining_weight -= 0.50
                        sl_p = min(sl_p, entry_p)
                    if not tp2_hit and lows[i] <= tp2_p:
                        tp2_hit = True
                        realized_r += 0.25 * 2.5
                        remaining_weight -= 0.25
                        sl_p = min(sl_p, tp1_p)
                    if lows[i] <= tp3_p:
                        realized_r += remaining_weight * 4.0
                        exit_p = tp3_p
                        trade_closed = True
                    elif tp1_hit:
                        trail_step = lows[i] + (1.0 * pos_atr)
                        sl_p = min(sl_p, trail_step)

            if trade_closed:
                pnl = risk_usd * realized_r
                equity += pnl
                total_r += realized_r
                if pnl >= 0:
                    gross_profit += pnl
                    wins += 1
                else:
                    gross_loss += abs(pnl)
                    losses += 1
                peak_equity = max(peak_equity, equity)
                dd_pct = ((peak_equity - equity) / (peak_equity + EPSILON)) * 100.0
                max_drawdown_pct = max(max_drawdown_pct, dd_pct)
                equity_curve.append(equity)
                timestamps.append(idx_list[i])
                trade_log.append(
                    {
                        "entry_time": str(entry_time),
                        "exit_time": str(idx_list[i]),
                        "direction": "BUY" if pos_dir == 1 else "SELL",
                        "entry": round(entry_p, 2),
                        "exit": round(exit_p, 2),
                        "r_multiple": round(realized_r, 2),
                        "pnl_usd": round(pnl, 2),
                    }
                )
                in_pos = False
                continue

        else:
            bar_dt = idx_list[i].to_pydatetime()
            if 21 <= bar_dt.hour < 23:
                continue

            z_val = zs[i]
            z_prev = zs[i - 1]
            c_val = closes[i]
            atr_val = max(atrs[i], 0.5)
            bull_trend = ema20s[i] > ema50s[i] and h1_biases[i] >= 0
            bear_trend = ema20s[i] < ema50s[i] and h1_biases[i] <= 0

            bull_signal = (
                (z_val <= -z_threshold or (z_val < -0.6 and z_val > z_prev and bull_trend))
                and flows[i] > 0.1
                and c_val > opens[i]
            )
            bear_signal = (
                (z_val >= z_threshold or (z_val > 0.6 and z_val < z_prev and bear_trend))
                and flows[i] < -0.1
                and c_val < opens[i]
            )

            if bull_signal and not bear_signal:
                in_pos = True
                pos_dir = 1
                entry_p = c_val
                pos_atr = atr_val
                risk_dist = max(1.5 * pos_atr, 1.50)
                sl_p = entry_p - risk_dist
                tp1_p = entry_p + 1.5 * risk_dist
                tp2_p = entry_p + 2.5 * risk_dist
                tp3_p = entry_p + 4.0 * risk_dist
                remaining_weight = 1.0
                realized_r = 0.0
                tp1_hit = False
                tp2_hit = False
                risk_usd = equity * (risk_pct / 100.0)
                entry_time = idx_list[i]
            elif bear_signal and not bull_signal:
                in_pos = True
                pos_dir = -1
                entry_p = c_val
                pos_atr = atr_val
                risk_dist = max(1.5 * pos_atr, 1.50)
                sl_p = entry_p + risk_dist
                tp1_p = entry_p - 1.5 * risk_dist
                tp2_p = entry_p - 2.5 * risk_dist
                tp3_p = entry_p - 4.0 * risk_dist
                remaining_weight = 1.0
                realized_r = 0.0
                tp1_hit = False
                tp2_hit = False
                risk_usd = equity * (risk_pct / 100.0)
                entry_time = idx_list[i]

    total_trades = wins + losses
    win_rate = (wins / (total_trades + EPSILON)) * 100.0 if total_trades > 0 else 0.0
    expectancy_r = (total_r / (total_trades + EPSILON)) if total_trades > 0 else 0.0

    if total_trades == 0:
        profit_factor = 0.0
    elif gross_loss < 1e-6:
        profit_factor = min(9.99, gross_profit / (1.0 + EPSILON)) if gross_profit > 0 else 0.0
    else:
        profit_factor = gross_profit / (gross_loss + EPSILON)

    net_ret = ((equity - initial_capital) / (initial_capital + EPSILON)) * 100.0

    return GridSearchResult(
        optimal_atr_period=atr_period,
        optimal_z_threshold=z_threshold,
        win_rate=round(win_rate, 1),
        profit_factor=round(profit_factor, 2),
        max_drawdown=round(max_drawdown_pct, 2),
        expectancy_r=round(expectancy_r, 2),
        total_trades=total_trades,
        net_return_pct=round(net_ret, 2),
        equity_curve=equity_curve,
        timestamps=timestamps,
        trade_log=trade_log,
    )


def run_grid_search_optimization(df_15m: pd.DataFrame, df_1h: pd.DataFrame) -> GridSearchResult:
    """
    Programmatically evaluates ATR_PERIOD_GRID x Z_THRESHOLD_GRID and selects
    the parameter combination yielding the highest Profit Factor.
    """
    candidates: List[GridSearchResult] = []
    for atr_p in ATR_PERIOD_GRID:
        for z_thresh in Z_THRESHOLD_GRID:
            candidates.append(_simulate_strategy_backtest(df_15m, df_1h, atr_p, z_thresh))

    viable = [c for c in candidates if c.total_trades >= 5]
    pool = viable if viable else candidates

    best = max(
        pool,
        key=lambda c: (c.profit_factor, c.expectancy_r, c.win_rate, -c.max_drawdown),
    )
    logger.info(
        f"Grid Search Optimal Selected: ATR={best.optimal_atr_period}, "
        f"Z={best.optimal_z_threshold:.1f} | Trades={best.total_trades} | "
        f"WR={best.win_rate:.1f}% | PF={best.profit_factor:.2f} | "
        f"MDD={best.max_drawdown:.1f}% | Exp={best.expectancy_r:+.2f}R"
    )
    return best


# ==============================================================================
# MATPLOTLIB BACKTEST CHART GENERATION (HEADLESS AGG)
# ==============================================================================
def generate_backtest_chart(
    df_15m: pd.DataFrame,
    grid_res: GridSearchResult,
    chart_path: str = CHART_PATH,
) -> str:
    """
    Renders institutional multi-panel backtest performance chart safely on headless
    Linux runners via Agg backend and saves to `backtest_performance.png`.
    """
    fig, (ax1, ax2) = plt.subplots(
        2, 1, figsize=(12, 7.8), gridspec_kw={"height_ratios": [2.2, 1.0]}, facecolor="#0B0F19"
    )

    # Panel 1: Equity Curve & High-Water Mark
    ax1.set_facecolor("#111827")
    eq_times = pd.to_datetime(grid_res.timestamps)
    eq_vals = np.array(grid_res.equity_curve, dtype=float)
    hwm_vals = np.maximum.accumulate(eq_vals)

    ax1.plot(eq_times, eq_vals, color="#10B981", linewidth=2.2, label="Simulated Equity ($)")
    ax1.plot(eq_times, hwm_vals, color="#6EE7B7", linestyle=":", linewidth=1.1, alpha=0.7, label="High-Water Mark")
    ax1.fill_between(eq_times, eq_vals, hwm_vals, color="#EF4444", alpha=0.12, label="Drawdown Zone")
    ax1.axhline(DEFAULT_ACCOUNT_BALANCE, color="#6B7280", linestyle="--", linewidth=1.0, alpha=0.7)

    ax1.set_title(
        f"APEX-QUANT-OMNI {ENGINE_VERSION} | XAUUSD Grid-Optimized Performance\n"
        f"Optimal ATR={grid_res.optimal_atr_period} | Z-Thresh={grid_res.optimal_z_threshold:.1f} | "
        f"Trades={grid_res.total_trades} | Win Rate={grid_res.win_rate:.1f}% | "
        f"PF={grid_res.profit_factor:.2f} | Max DD={grid_res.max_drawdown:.1f}% | Exp={grid_res.expectancy_r:+.2f}R",
        color="#F9FAFB",
        fontsize=10.5,
        fontweight="bold",
        pad=12,
    )
    ax1.set_ylabel("Account Equity (USD)", color="#D1D5DB", fontsize=10)
    ax1.tick_params(colors="#9CA3AF", labelsize=9)
    ax1.grid(True, color="#1F2937", linestyle=":", linewidth=0.8)
    ax1.legend(loc="upper left", ncol=3, facecolor="#1F2937", edgecolor="#374151", labelcolor="#F9FAFB", fontsize=8)

    # Panel 2: Rolling Z-Score & Threshold Bands
    ax2.set_facecolor("#111827")
    z_tail = compute_rolling_zscore(df_15m["Close"], window=20).tail(250)
    ax2.plot(z_tail.index, z_tail.values, color="#38BDF8", linewidth=1.5, label="Rolling Z-Score (20)")
    ax2.axhline(
        grid_res.optimal_z_threshold,
        color="#EF4444",
        linestyle="--",
        linewidth=1.1,
        label=f"+Z Threshold ({grid_res.optimal_z_threshold:.1f})",
    )
    ax2.axhline(
        -grid_res.optimal_z_threshold,
        color="#10B981",
        linestyle="--",
        linewidth=1.1,
        label=f"-Z Threshold (-{grid_res.optimal_z_threshold:.1f})",
    )
    ax2.axhline(0.0, color="#6B7280", linestyle="-", linewidth=0.7, alpha=0.6)
    ax2.set_ylabel("Z-Score", color="#D1D5DB", fontsize=10)
    ax2.tick_params(colors="#9CA3AF", labelsize=9)
    ax2.grid(True, color="#1F2937", linestyle=":", linewidth=0.8)
    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d %H:%M"))
    ax2.legend(loc="upper left", ncol=3, facecolor="#1F2937", edgecolor="#374151", labelcolor="#F9FAFB", fontsize=8)

    fig.autofmt_xdate()
    plt.tight_layout()
    plt.savefig(chart_path, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    logger.info(f"Saved backtest performance chart to [{chart_path}].")
    return chart_path


# ==============================================================================
# AUTO-TRAILING STOP ENGINE (LIVE SQLITE STATE MACHINE)
# ==============================================================================
def update_auto_trailing_stop_engine(
    current_price: float,
    current_atr: float,
    new_signal_decision: str,
    entry_price: float,
    sl: float,
    tp1: float,
    tp2: float,
    tp3: float,
    db_path: str = DB_PATH,
) -> str:
    """
    Monitors active open positions in SQLite. Once price breaches TP1, shifts
    Stop Loss to Breakeven and engages fractional ATR trailing steps.
    """
    init_sqlite_db(db_path)
    now_iso = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(db_path)
    protocol_status = "Active @ TP1 Breakeven"

    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, direction, entry_price, current_sl, tp1, tp2, tp3, atr_at_entry, tp1_hit, peak_favorable_price "
            "FROM active_positions WHERE status = 'OPEN' ORDER BY id DESC LIMIT 1"
        )
        row = cur.fetchone()

        if row:
            pos_id, direction, pos_entry, curr_sl, pos_tp1, pos_tp2, pos_tp3, pos_atr, tp1_hit, peak_fav = row
            atr_step = max(0.5 * max(current_atr, pos_atr), 0.50)

            if direction == "BUY":
                peak_fav = max(peak_fav, current_price)
                if current_price <= curr_sl:
                    cur.execute(
                        "UPDATE active_positions SET status = 'CLOSED_SL', updated_utc = ? WHERE id = ?",
                        (now_iso, pos_id),
                    )
                    protocol_status = f"Prior BUY Closed @ SL {curr_sl:.2f} | Ready @ TP1 Breakeven"
                elif current_price >= pos_tp3:
                    cur.execute(
                        "UPDATE active_positions SET status = 'CLOSED_TP3', updated_utc = ? WHERE id = ?",
                        (now_iso, pos_id),
                    )
                    protocol_status = f"Prior BUY Hit TP3 {pos_tp3:.2f} (4.0R)"
                else:
                    if current_price >= pos_tp1 and tp1_hit == 0:
                        tp1_hit = 1
                        curr_sl = max(curr_sl, pos_entry)
                    if tp1_hit == 1:
                        new_trail = max(pos_entry, peak_fav - atr_step)
                        if new_trail > curr_sl:
                            curr_sl = round(new_trail, 2)
                        protocol_status = f"Breakeven+ATR Trailing Active (SL @ {curr_sl:.2f})"
                    cur.execute(
                        "UPDATE active_positions SET current_sl = ?, tp1_hit = ?, peak_favorable_price = ?, updated_utc = ? WHERE id = ?",
                        (curr_sl, tp1_hit, peak_fav, now_iso, pos_id),
                    )
            else:
                peak_fav = min(peak_fav, current_price)
                if current_price >= curr_sl:
                    cur.execute(
                        "UPDATE active_positions SET status = 'CLOSED_SL', updated_utc = ? WHERE id = ?",
                        (now_iso, pos_id),
                    )
                    protocol_status = f"Prior SELL Closed @ SL {curr_sl:.2f} | Ready @ TP1 Breakeven"
                elif current_price <= pos_tp3:
                    cur.execute(
                        "UPDATE active_positions SET status = 'CLOSED_TP3', updated_utc = ? WHERE id = ?",
                        (now_iso, pos_id),
                    )
                    protocol_status = f"Prior SELL Hit TP3 {pos_tp3:.2f} (4.0R)"
                else:
                    if current_price <= pos_tp1 and tp1_hit == 0:
                        tp1_hit = 1
                        curr_sl = min(curr_sl, pos_entry)
                    if tp1_hit == 1:
                        new_trail = min(pos_entry, peak_fav + atr_step)
                        if new_trail < curr_sl:
                            curr_sl = round(new_trail, 2)
                        protocol_status = f"Breakeven+ATR Trailing Active (SL @ {curr_sl:.2f})"
                    cur.execute(
                        "UPDATE active_positions SET current_sl = ?, tp1_hit = ?, peak_favorable_price = ?, updated_utc = ? WHERE id = ?",
                        (curr_sl, tp1_hit, peak_fav, now_iso, pos_id),
                    )

        if "VERIFIED" in new_signal_decision:
            new_dir = "BUY" if "BUY" in new_signal_decision else "SELL"
            cur.execute("SELECT id, direction FROM active_positions WHERE status = 'OPEN'")
            existing_open = cur.fetchall()
            if not any(r[1] == new_dir for r in existing_open):
                cur.execute(
                    """
                    INSERT INTO active_positions (
                        opened_utc, direction, entry_price, initial_sl, current_sl,
                        tp1, tp2, tp3, atr_at_entry, tp1_hit, peak_favorable_price,
                        status, updated_utc
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, 'OPEN', ?)
                    """,
                    (
                        now_iso, new_dir, entry_price, sl, sl,
                        tp1, tp2, tp3, current_atr, entry_price, now_iso,
                    ),
                )
                protocol_status = "Active @ TP1 Breakeven"

        conn.commit()
    finally:
        conn.close()

    return protocol_status


# ==============================================================================
# MASTER SIGNAL SYNTHESIS & INSTITUTIONAL WEBHOOK FORMATTING
# ==============================================================================
def run_full_quant_evaluation(command_listener_status: str = "Active / Ready for /check") -> SignalReport:
    """
    Executes the end-to-end 5-Pillar + Grid Search + Chart + Trailing Stop pipeline.
    """
    df_15m, df_1h, data_source, spot_quote = fetch_xauusd_mtf_data()
    session_status = evaluate_market_session(spot_quote=spot_quote)

    grid_res = run_grid_search_optimization(df_15m, df_1h)
    generate_backtest_chart(df_15m, grid_res, CHART_PATH)

    p1 = analyze_pillar_1_smc(df_15m)
    p2 = analyze_pillar_2_order_flow(df_15m)
    p3 = analyze_pillar_3_zscore(df_15m, z_threshold=grid_res.optimal_z_threshold)
    p4 = analyze_pillar_4_mtf_harmony(df_15m, df_1h)

    bull_score = 0
    bear_score = 0
    for pillar in (p1, p2, p3, p4):
        if pillar["bias"] == "BULLISH":
            bull_score += pillar["points"]
        elif pillar["bias"] == "BEARISH":
            bear_score += pillar["points"]
        else:
            bull_score += pillar["points"] // 2
            bear_score += pillar["points"] // 2

    dominant_dir = "BULLISH" if bull_score >= bear_score else "BEARISH"
    p5 = analyze_pillar_5_atr_risk(
        df_15m=df_15m,
        direction=dominant_dir,
        optimal_atr_period=grid_res.optimal_atr_period,
        spot_quote=spot_quote,
    )

    total_bull = min(100, bull_score + (p5["points"] if dominant_dir == "BULLISH" else 0))
    total_bear = min(100, bear_score + (p5["points"] if dominant_dir == "BEARISH" else 0))
    confluence_score = max(total_bull, total_bear)

    if total_bull >= CONFLUENCE_THRESHOLD and total_bull > total_bear + 8:
        signal_decision = "VERIFIED BUY"
    elif total_bear >= CONFLUENCE_THRESHOLD and total_bear > total_bull + 8:
        signal_decision = "VERIFIED SELL"
    else:
        signal_decision = "NEUTRAL / HOLD"

    trailing_status = update_auto_trailing_stop_engine(
        current_price=p5["entry"],
        current_atr=p5["atr_exec"],
        new_signal_decision=signal_decision,
        entry_price=p5["entry"],
        sl=p5["sl"],
        tp1=p5["tp1"],
        tp2=p5["tp2"],
        tp3=p5["tp3"],
    )

    now_utc = datetime.now(timezone.utc).isoformat()
    candle_ts = df_15m.index[-1].isoformat()

    return SignalReport(
        timestamp_utc=now_utc,
        candle_timestamp_utc=candle_ts,
        asset=ASSET_LABEL,
        data_source=data_source,
        spot_quote=spot_quote,
        session_status=session_status,
        signal_decision=signal_decision,
        entry_price=p5["entry"],
        stop_loss=p5["sl"],
        tp1=p5["tp1"],
        tp2=p5["tp2"],
        tp3=p5["tp3"],
        trailing_stop_protocol=trailing_status,
        confluence_score=int(confluence_score),
        z_score=round(p3["z_score"], 4),
        smc_state=p1["state"],
        order_flow_state=p2["state"],
        mtf_harmony_state=p4["state"],
        atr_value=p5["atr_exec"],
        optimal_atr_period=grid_res.optimal_atr_period,
        optimal_z_threshold=grid_res.optimal_z_threshold,
        recommended_lot=p5["recommended_lot"],
        risk_amount_usd=p5["risk_amount_usd"],
        win_rate=grid_res.win_rate,
        profit_factor=grid_res.profit_factor,
        max_drawdown=grid_res.max_drawdown,
        expectancy_r=grid_res.expectancy_r,
        command_listener_status=command_listener_status,
        webhook_status="Delivered via Exponential Backoff",
        pillar_breakdown={
            "P1_SMC": p1,
            "P2_OrderFlow": p2,
            "P3_ZScore": p3,
            "P4_MTFHarmony": p4,
            "P5_ATRRisk": p5,
        },
    )


def format_institutional_telegram_report(report: SignalReport) -> str:
    """Formats the institutional forensic HTML template for Telegram."""
    bbo_line = (
        f"• <i>Live Spot BBO:</i> Bid {report.spot_quote.bid:.2f} / "
        f"Ask {report.spot_quote.ask:.2f} (Spread ${report.spot_quote.spread:.2f})\n"
        if report.spot_quote
        else "• <i>Live Spot BBO:</i> Feed Close Sync\n"
    )
    return (
        f"🛡️ <b>APEX-QUANT-OMNI SOVEREIGN {ENGINE_VERSION} (2-WAY COMMAND EDITION)</b> 🛡️\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🌟 <b>Asset:</b> {report.asset}\n"
        f"📊 <b>Signal Decision:</b> {report.signal_decision}\n"
        f"🎯 <b>Precision Entry:</b> {report.entry_price:.2f}\n"
        f"🛑 <b>Forensic Stop Loss:</b> {report.stop_loss:.2f}\n"
        f"📈 <b>Take Profit 1 (1.5R):</b> {report.tp1:.2f}\n"
        f"📈 <b>Take Profit 2 (2.5R):</b> {report.tp2:.2f}\n"
        f"🚀 <b>Take Profit 3 (4.0R):</b> {report.tp3:.2f}\n"
        f"🔄 <b>Trailing Stop Protocol:</b> {report.trailing_stop_protocol}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🤖 <b>Interactive 2-Way Command Status:</b>\n"
        f"• <i>Command Listener:</i> {report.command_listener_status}\n"
        f"• <i>Multi-Source Feed:</i> {report.data_source}\n"
        f"{bbo_line}"
        f"• <i>Session Governor:</i> {report.session_status.session_name} "
        f"({'OPEN' if report.session_status.is_allowed else 'SUPPRESSED'})\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚙️ <b>Grid Search &amp; Exponential Backoff:</b>\n"
        f"• <i>Optimal ATR Period:</i> {report.optimal_atr_period}\n"
        f"• <i>Optimal Z-Threshold:</i> {report.optimal_z_threshold:.1f}\n"
        f"• <i>Live Z-Score / SMC:</i> {report.z_score:+.2f} | {report.smc_state}\n"
        f"• <i>Order Flow / MTF:</i> {report.order_flow_state} | {report.mtf_harmony_state}\n"
        f"• <i>Webhook Status:</i> {report.webhook_status}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 <b>Risk &amp; Position Sizing:</b>\n"
        f"• <i>Recommended Lot:</i> {report.recommended_lot:.2f} Lots ({DEFAULT_RISK_PERCENT:.1f}% Risk)\n"
        f"• <i>Risk Amount (USD):</i> ${report.risk_amount_usd:.2f}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔬 <b>Forensic Audit, DB Artifact &amp; Backtest:</b>\n"
        f"• <i>Confluence Score:</i> {report.confluence_score}/100\n"
        f"• <i>SQLite Artifact:</i> Saved &amp; Uploaded\n"
        f"• <i>Optimized Win Rate:</i> {report.win_rate:.1f}%\n"
        f"• <i>Profit Factor:</i> {report.profit_factor:.2f} (Exp: {report.expectancy_r:+.2f}R)\n"
        f"• <i>Max Drawdown:</i> {report.max_drawdown:.1f}%\n"
        f"• <i>Timestamp (UTC):</i> {report.timestamp_utc}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━"
    )


# ==============================================================================
# INTERACTIVE 2-WAY TELEGRAM COMMAND HANDLER (/check, /status, /chart, /audit, /help)
# ==============================================================================
def _build_status_message() -> str:
    spot_quote = fetch_swissquote_live_spot_quote()
    session = evaluate_market_session(spot_quote=spot_quote)
    init_sqlite_db(DB_PATH)
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM signals_audit")
        total_rows = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM active_positions WHERE status = 'OPEN'")
        open_pos = cur.fetchone()[0]
        cur.execute(
            "SELECT timestamp_utc, signal_decision, entry_price, confluence_score "
            "FROM signals_audit ORDER BY id DESC LIMIT 1"
        )
        last_row = cur.fetchone()
    finally:
        conn.close()

    last_summary = (
        f"{last_row[1]} @ {last_row[2]:.2f} ({last_row[3]}/100) [{last_row[0]}]"
        if last_row
        else "No signals logged yet"
    )
    bbo_txt = (
        f"{spot_quote.bid:.2f} / {spot_quote.ask:.2f} (Spread ${spot_quote.spread:.2f})"
        if spot_quote
        else "Unavailable"
    )
    return (
        f"🩺 <b>APEX-QUANT-OMNI {ENGINE_VERSION} — SYSTEM HEALTH STATUS</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"• <b>Engine State:</b> ONLINE &amp; OPERATIONAL\n"
        f"• <b>Live Spot XAU/USD:</b> {bbo_txt}\n"
        f"• <b>Session Governor:</b> {session.session_name} "
        f"({'ACTIVE' if session.is_allowed else 'SUPPRESSED'})\n"
        f"• <b>Session Note:</b> {session.reason}\n"
        f"• <b>SQLite Audit Records:</b> {total_rows} (Open Trailing Pos: {open_pos})\n"
        f"• <b>Latest Audit Entry:</b> {last_summary}\n"
        f"• <b>UTC Clock:</b> {session.utc_timestamp}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Commands: <code>/check</code> | <code>/status</code> | <code>/chart</code> | <code>/audit</code> | <code>/help</code>"
    )


def _build_audit_history_message() -> str:
    init_sqlite_db(DB_PATH)
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT timestamp_utc, signal_decision, entry_price, confluence_score, z_score "
            "FROM signals_audit ORDER BY id DESC LIMIT 5"
        )
        rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        return "📂 <b>SQLite Forensic Audit:</b> No historical entries recorded yet."

    lines = [f"🔬 <b>APEX-QUANT-OMNI {ENGINE_VERSION} — LAST 5 FORENSIC AUDITS</b>", "━━━━━━━━━━━━━━━━━━━━━━"]
    for ts, dec, entry, score, z in rows:
        lines.append(f"• <code>{ts[:19]}Z</code> | <b>{dec}</b> @ {entry:.2f} | {score}/100 | Z={z:+.2f}")
    return "\n".join(lines)


def process_telegram_commands(poll_timeout: int = 0) -> int:
    """
    Polls Telegram Bot API `getUpdates` for interactive 2-way user commands:
      - /check  : Runs on-demand live XAUUSD quantitative analysis & replies immediately
      - /status : Reports system health, live Spot BBO, SQLite state, and Session Governor
      - /chart  : Sends the latest backtest_performance.png equity curve
      - /audit  : Displays last 5 forensic audit entries from SQLite
      - /help   : Lists available interactive commands
    """
    if not TELEGRAM_BOT_TOKEN:
        logger.info("TELEGRAM_BOT_TOKEN missing; skipping 2-way command polling.")
        return 0

    last_offset_str = get_bot_state("telegram_update_offset", "0")
    offset = int(last_offset_str) if last_offset_str.isdigit() else 0

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
    params: Dict[str, Any] = {"offset": offset, "timeout": poll_timeout, "allowed_updates": ["message"]}
    resp = request_with_exponential_backoff(
        "GET", url, max_retries=2, backoff_delays=(2, 4), timeout=poll_timeout + 10, params=params
    )
    if resp is None or resp.status_code != 200:
        return 0

    try:
        updates = resp.json().get("result", [])
    except (ValueError, AttributeError):
        return 0

    processed = 0
    for upd in updates:
        upd_id = int(upd.get("update_id", 0))
        if upd_id >= offset:
            offset = upd_id + 1
            set_bot_state("telegram_update_offset", str(offset))

        msg = upd.get("message") or {}
        text = str(msg.get("text", "")).strip()
        chat_id = str((msg.get("chat") or {}).get("id", "")).strip()
        if not text or not chat_id:
            continue

        if TELEGRAM_CHAT_ID and chat_id != TELEGRAM_CHAT_ID:
            logger.warning(f"Ignored unauthorized Telegram command from chat_id={chat_id}")
            continue

        cmd = text.split()[0].split("@")[0].lower()
        logger.info(f"Received 2-Way Telegram command [{cmd}] from chat_id={chat_id}")

        if cmd == "/check":
            send_telegram_message("⏳ <i>Executing live XAUUSD 5-Pillar + Grid Search scan...</i>", chat_id=chat_id)
            report = run_full_quant_evaluation(command_listener_status="Active (On-Demand /check Executed)")
            report.webhook_status = "On-Demand /check Reply (Exponential Backoff)"
            forensic_audit_log(report)
            send_telegram_message(format_institutional_telegram_report(report), chat_id=chat_id)
            if os.path.exists(CHART_PATH):
                send_telegram_photo(CHART_PATH, caption="📊 Live Grid-Optimized Backtest Curve", chat_id=chat_id)
            processed += 1

        elif cmd == "/status":
            send_telegram_message(_build_status_message(), chat_id=chat_id)
            processed += 1

        elif cmd == "/audit":
            send_telegram_message(_build_audit_history_message(), chat_id=chat_id)
            processed += 1

        elif cmd == "/chart":
            if not os.path.exists(CHART_PATH):
                df_15m, df_1h, _, _ = fetch_xauusd_mtf_data()
                grid_res = run_grid_search_optimization(df_15m, df_1h)
                generate_backtest_chart(df_15m, grid_res, CHART_PATH)
            send_telegram_photo(CHART_PATH, caption="📈 APEX-QUANT-OMNI Backtest Performance", chat_id=chat_id)
            processed += 1

        elif cmd in ("/help", "/start"):
            help_msg = (
                f"🤖 <b>APEX-QUANT-OMNI {ENGINE_VERSION} Interactive Commands</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"• <code>/check</code> — Live XAUUSD Signal, Z-Score, SMC &amp; Confluence Scan\n"
                f"• <code>/status</code> — System Health, Live Spread &amp; Session Governor\n"
                f"• <code>/chart</code> — Latest Grid-Optimized Equity Curve Chart\n"
                f"• <code>/audit</code> — Last 5 SQLite Forensic Audit Records\n"
                f"• <code>/help</code> — Show this command menu"
            )
            send_telegram_message(help_msg, chat_id=chat_id)
            processed += 1

    return processed


# ==============================================================================
# EXECUTION ORCHESTRATION (ONESHOT SERVERLESS & CONTINUOUS 2-WAY POLLING)
# ==============================================================================
def execute_scheduled_cycle(force_notify: bool = False) -> None:
    """
    Runs one complete serverless cycle:
      1. Handles any pending 2-way Telegram user commands (/check, /status, /chart, /audit)
      2. Runs 5-Pillar Quant Engine + Grid Search + Live Spread Check + Chart
      3. Evaluates Market Session Governor
      4. Dispatches webhooks via Exponential Backoff on verified signals (deduped per candle)
    """
    init_sqlite_db(DB_PATH)
    process_telegram_commands(poll_timeout=0)

    report = run_full_quant_evaluation(command_listener_status="Active / Ready for /check")
    session = report.session_status

    last_notified_candle = get_bot_state("last_notified_candle_utc", "")
    is_new_candle = report.candle_timestamp_utc != last_notified_candle
    is_verified = "VERIFIED" in report.signal_decision

    if not session.is_allowed and not force_notify:
        report.webhook_status = f"Suppressed by Session Governor ({session.session_name})"
        forensic_audit_log(report)
        logger.info(f"Automated alert suppressed: {session.reason}")
        return

    if (is_verified and is_new_candle) or force_notify:
        tg_ok = send_telegram_message(format_institutional_telegram_report(report))
        if tg_ok and os.path.exists(CHART_PATH):
            send_telegram_photo(CHART_PATH, caption=f"📊 {report.asset} | {report.signal_decision}")
        dc_ok = send_discord_webhook(report)

        if tg_ok or dc_ok:
            report.webhook_status = "Delivered via Exponential Backoff"
            set_bot_state("last_notified_candle_utc", report.candle_timestamp_utc)
        else:
            report.webhook_status = "Logged Locally (No Webhook Target Configured/Reached)"
    else:
        report.webhook_status = "Standby (Neutral Confluence or Already Notified)"

    forensic_audit_log(report)


def run_continuous_polling_loop(scan_interval_sec: int = 900) -> None:
    """
    Runs a persistent 2-way Telegram command listener with periodic 15-minute
    scheduled quantitative scans (for VPS / long-running container deployments).
    """
    logger.info(f"Starting continuous 2-Way Telegram Listener + {scan_interval_sec}s scan loop...")
    last_scan_ts = 0.0
    while True:
        try:
            process_telegram_commands(poll_timeout=15)
            now_ts = time.time()
            if now_ts - last_scan_ts >= scan_interval_sec:
                execute_scheduled_cycle(force_notify=False)
                last_scan_ts = now_ts
        except KeyboardInterrupt:
            logger.info("Graceful shutdown requested.")
            break
        except Exception as exc:
            logger.error(f"Error inside continuous loop: {exc}")
            send_emergency_telegram_alert("Continuous Polling Loop", exc)
            time.sleep(10)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=f"APEX-QUANT-OMNI SOVEREIGN {ENGINE_VERSION} XAUUSD Engine"
    )
    parser.add_argument(
        "--mode",
        choices=["oneshot", "poll", "commands_only"],
        default=os.getenv("BOT_MODE", "oneshot"),
        help="Execution mode: oneshot (GitHub Actions), poll (24/7 listener), commands_only",
    )
    parser.add_argument(
        "--force-notify",
        action="store_true",
        default=os.getenv("FORCE_NOTIFY", "false").lower() == "true",
        help="Force webhook delivery even on NEUTRAL or suppressed session (manual diagnostic).",
    )
    args = parser.parse_args()

    try:
        if args.mode == "poll":
            run_continuous_polling_loop()
        elif args.mode == "commands_only":
            count = process_telegram_commands(poll_timeout=2)
            logger.info(f"Processed {count} interactive Telegram command(s).")
        else:
            execute_scheduled_cycle(force_notify=args.force_notify)
    except Exception as exc:
        logger.critical(f"Fatal unhandled exception in APEX-QUANT-OMNI: {exc}", exc_info=True)
        send_emergency_telegram_alert("Top-Level Engine Execution", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
