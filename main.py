#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
 APEX-QUANT-OMNI ENTERPRISE v40.0  (ULTIMATE EDITION)
 Institutional-Grade XAUUSD (SPOT GOLD) Signal Generation Engine
--------------------------------------------------------------------------------
 MODULES
   • Grid Search Parameter Optimization (ATR period x Z-score threshold)
   • 5-Pillar Hybrid Engine (SMC / Order Flow / Z-Score / MTF / ATR Regime)
   • SQLite Forensic Persistence (trading_signals.db)
   • Auto-Trailing Stop Engine (breakeven @ TP1 -> ATR-fraction trailing)
   • Telegram Error Handler (network / API / data anomaly alerts)
   • Matplotlib Backtest Charts (Agg backend -> backtest_performance.png)
   • Dual-Channel Delivery (Telegram + Discord) with Circuit Breaker
================================================================================
"""

from __future__ import annotations

import html as html_lib
import json
import math
import os
import re
import sqlite3
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import matplotlib

matplotlib.use("Agg")  # MANDATORY: headless backend for GitHub Actions
import matplotlib.pyplot as plt  # noqa: E402

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import requests  # noqa: E402
import yfinance as yf  # noqa: E402

# ==============================================================================
# SECTION 1 — GLOBAL CONFIGURATION
# ==============================================================================

ENGINE_NAME = "APEX-QUANT-OMNI ENTERPRISE v40.0"
ENGINE_EDITION = "ULTIMATE EDITION"

SYMBOL = "XAUUSD=X"          # Spot gold — NEVER GC=F (futures basis mismatch)
EXEC_INTERVAL = "15m"        # Execution timeframe
MACRO_INTERVAL = "1h"        # Macro fractal timeframe
EXEC_PERIOD = "60d"          # yfinance hard limit for intraday 15m
MACRO_PERIOD = "730d"        # yfinance hard limit for 1h

DB_PATH = "trading_signals.db"
CHART_PATH = "backtest_performance.png"

EPS = 1e-8                   # Division-by-zero protection epsilon
CONTRACT_SIZE = 100.0        # Ounces per 1.00 lot of XAUUSD
SPREAD_USD = 0.30            # 30 cents institutional spread
SLIPPAGE_USD = 0.05          # 5 cents execution slippage
ATR_MULT = 1.0               # Stop-loss distance = 1.0 x ATR
RR_TARGET = 4.0              # Backtest final target (min RRR 1:3 satisfied)
TP1_R, TP2_R, TP3_R = 1.5, 2.5, 4.0
TRAIL_ATR_FRACTION = 0.50    # Trailing step = 0.5 x ATR
REGIME_PCT_FLOOR = 0.20      # ATR percentile floor (below => CHOPPY filter)
MACRO_SLOPE_THRESHOLD = 0.15 # EMA20/EMA50 spread in ATR units
FLOW_STRONG = 1.00           # Order-flow displacement strength threshold
MIN_CONFLUENCE = 60.0        # Confluence gate for verified signals
MIN_TRADES_FOR_OPTIMUM = 8   # Minimum sample size for grid-search eligibility

GRID_ATR_PERIODS = [10, 14, 20]
GRID_Z_THRESHOLDS = [1.5, 2.0, 2.5]
Z_WINDOW = 20                # Rolling Z-score window (fixed)
WARMUP_BARS = 120            # Backtest warmup

ACCOUNT_BALANCE = float(os.getenv("ACCOUNT_BALANCE", "10000") or 10000)
RISK_PERCENTAGE = float(os.getenv("RISK_PERCENTAGE", "1.0") or 1.0)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
SEND_NO_TRADE_ALERT = os.getenv("SEND_NO_TRADE_ALERT", "0").strip() == "1"

HTTP_TIMEOUT = 15
MAX_RETRIES = 3


# ==============================================================================
# SECTION 2 — LOGGING & FORENSIC AUDIT TRAIL
# ==============================================================================

def utc_stamp() -> str:
    """ISO-8601 UTC timestamp, second resolution."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def log(message: str, level: str = "INFO") -> None:
    """Structured console logger (GitHub Actions friendly)."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts} UTC] [{level:<9}] {message}", flush=True)


def forensic_audit_log(section: str, payload: dict) -> None:
    """
    Emit RAW mathematical values to the GitHub Actions console.
    Every value used in a decision must be visible for forensic replay.
    """
    log(f"AUDIT >> {section}", "FORENSIC")
    if not payload:
        print("    |- (empty payload)", flush=True)
        print("    +- end of section", flush=True)
        return
    for key, value in payload.items():
        if isinstance(value, bool):
            rendered = str(value)
        elif isinstance(value, (int, np.integer)):
            rendered = str(int(value))
        elif isinstance(value, (float, np.floating)):
            rendered = f"{float(value):.6f}"
        elif isinstance(value, (list, tuple)) and len(value) > 12:
            rendered = f"{list(value[:12])} ... (n={len(value)})"
        else:
            rendered = str(value)
        print(f"    |- {key:<30}: {rendered}", flush=True)
    print("    +- end of section", flush=True)


def safe_float(value, default: float = 0.0) -> float:
    """NaN/inf/None-safe float coercion."""
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    if not np.isfinite(result):
        return default
    return result


# ==============================================================================
# SECTION 3 — EXCEPTIONS & CIRCUIT BREAKER
# ==============================================================================

class DataAnomalyError(RuntimeError):
    """Raised when the market data payload fails structural validation."""


class CircuitBreakerOpen(RuntimeError):
    """Raised when the circuit breaker trips and halts execution."""


class CircuitBreaker:
    """
    Halts execution when consecutive critical failures exceed the threshold.
    Prevents burning API quota / dispatching signals on corrupt data.
    """

    def __init__(self, threshold: int = 3) -> None:
        self.threshold = threshold
        self.failures = 0
        self.reasons: list[str] = []

    def record_failure(self, reason: str) -> None:
        self.failures += 1
        self.reasons.append(reason)
        log(f"CircuitBreaker failure {self.failures}/{self.threshold}: {reason}", "WARNING")
        if self.failures >= self.threshold:
            raise CircuitBreakerOpen(
                f"Circuit breaker tripped after {self.failures} failures: {self.reasons}"
            )

    def reset(self) -> None:
        self.failures = 0
        self.reasons = []


# ==============================================================================
# SECTION 4 — SQLITE FORENSIC PERSISTENCE LAYER
# ==============================================================================

def db_connect() -> sqlite3.Connection:
    """Open a resilient SQLite connection with row access by column name."""
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=DELETE;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def init_db() -> None:
    """Create the forensic schema if it does not already exist."""
    conn = db_connect()
    try:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS signals (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp       TEXT    NOT NULL,
                symbol          TEXT    NOT NULL,
                direction       TEXT    NOT NULL,
                entry           REAL    NOT NULL,
                stop_loss       REAL    NOT NULL,
                tp1             REAL    NOT NULL,
                tp2             REAL    NOT NULL,
                tp3             REAL    NOT NULL,
                lot             REAL    NOT NULL,
                risk_usd        REAL    NOT NULL,
                confluence      REAL    NOT NULL,
                regime          TEXT    NOT NULL,
                atr_period      INTEGER NOT NULL,
                z_threshold     REAL    NOT NULL,
                win_rate        REAL    NOT NULL,
                profit_factor   REAL    NOT NULL,
                max_drawdown    REAL    NOT NULL,
                status          TEXT    NOT NULL
            );

            CREATE TABLE IF NOT EXISTS active_positions (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_id       INTEGER NOT NULL,
                timestamp       TEXT    NOT NULL,
                direction       TEXT    NOT NULL,
                entry           REAL    NOT NULL,
                stop_loss       REAL    NOT NULL,
                tp1             REAL    NOT NULL,
                tp2             REAL    NOT NULL,
                tp3             REAL    NOT NULL,
                extreme_price   REAL    NOT NULL,
                trailing_stage  INTEGER NOT NULL DEFAULT 0,
                last_update     TEXT    NOT NULL,
                status          TEXT    NOT NULL,
                FOREIGN KEY (signal_id) REFERENCES signals (id)
            );

            CREATE TABLE IF NOT EXISTS audit_events (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp       TEXT    NOT NULL,
                level           TEXT    NOT NULL,
                section         TEXT    NOT NULL,
                message         TEXT    NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_signals_ts  ON signals (timestamp);
            CREATE INDEX IF NOT EXISTS idx_active_stat ON active_positions (status);
            """
        )
        conn.commit()
        log(f"SQLite schema verified at '{DB_PATH}'.")
    finally:
        conn.close()


def db_log_event(level: str, section: str, message: str) -> None:
    """Append an audit event row (never raises — persistence must not break flow)."""
    try:
        conn = db_connect()
        try:
            conn.execute(
                "INSERT INTO audit_events (timestamp, level, section, message) "
                "VALUES (?, ?, ?, ?);",
                (utc_stamp(), level, section, message[:4000]),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception as exc:  # pragma: no cover - defensive
        log(f"SQLite audit_events write failed: {exc}", "WARNING")


def db_log_signal(ctx: dict) -> int:
    """Persist a verified signal. Returns the inserted row id."""
    conn = db_connect()
    try:
        cursor = conn.execute(
            """
            INSERT INTO signals (
                timestamp, symbol, direction, entry, stop_loss, tp1, tp2, tp3,
                lot, risk_usd, confluence, regime, atr_period, z_threshold,
                win_rate, profit_factor, max_drawdown, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (
                ctx["timestamp"], ctx["symbol"], ctx["decision"], ctx["entry"],
                ctx["stop_loss"], ctx["tp1"], ctx["tp2"], ctx["tp3"], ctx["lot"],
                ctx["risk_usd"], ctx["confluence"], ctx["regime"],
                int(ctx["atr_period"]), float(ctx["z_threshold"]),
                float(ctx["win_rate"]), float(ctx["profit_factor"]),
                float(ctx["max_drawdown"]), "OPEN",
            ),
        )
        conn.commit()
        return int(cursor.lastrowid)
    finally:
        conn.close()


def db_create_active_position(signal_id: int, ctx: dict) -> None:
    """Register the position for auto-trailing management on subsequent runs."""
    conn = db_connect()
    try:
        conn.execute(
            """
            INSERT INTO active_positions (
                signal_id, timestamp, direction, entry, stop_loss, tp1, tp2, tp3,
                extreme_price, trailing_stage, last_update, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (
                signal_id, ctx["timestamp"], ctx["decision"], ctx["entry"],
                ctx["stop_loss"], ctx["tp1"], ctx["tp2"], ctx["tp3"],
                ctx["entry"], 0, ctx["timestamp"], "OPEN",
            ),
        )
        conn.commit()
    finally:
        conn.close()


# ==============================================================================
# SECTION 5 — DATA PIPELINE (100% FREE, REGISTRATION-FREE)
# ==============================================================================

def fetch_ohlcv(symbol: str, interval: str, period: str, label: str) -> pd.DataFrame:
    """
    Download and rigorously sanitise OHLCV data.
    Handles yfinance multi-index columns, duplicates, non-numeric junk,
    impossible candles, and insufficient history.
    """
    log(f"Downloading {label} data: symbol={symbol} interval={interval} period={period}")
    raw = yf.download(
        tickers=symbol,
        interval=interval,
        period=period,
        auto_adjust=False,
        progress=False,
        threads=False,
    )
    if raw is None or len(raw) == 0:
        raise DataAnomalyError(f"{label}: empty payload returned by yfinance")

    df = raw.copy()

    # --- Multi-index flattening safeguard (mandatory) ---
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df = df.loc[:, ~df.columns.duplicated()]

    required = ["Open", "High", "Low", "Close", "Volume"]
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise DataAnomalyError(f"{label}: missing required columns {missing}")

    df = df[required].copy()
    df.index = pd.to_datetime(df.index, utc=True)
    df = df[~df.index.duplicated(keep="last")].sort_index()
    df = df.apply(pd.to_numeric, errors="coerce")
    df["Volume"] = df["Volume"].fillna(0.0)
    df = df.dropna(subset=["Open", "High", "Low", "Close"])

    # Structural integrity filters
    df = df[(df["High"] >= df["Low"]) & (df["Close"] > 0.0) & (df["Open"] > 0.0)]
    df = df[(df["High"] >= df["Close"]) & (df["Low"] <= df["Close"])]

    if len(df) < 250:
        raise DataAnomalyError(f"{label}: insufficient history ({len(df)} bars, need >= 250)")

    log(f"{label} data ready: {len(df)} bars | {df.index[0]} -> {df.index[-1]}")
    return df


# ==============================================================================
# SECTION 6 — VECTORISED INDICATORS & STRUCTURE DETECTION
# ==============================================================================

def true_range(df: pd.DataFrame) -> pd.Series:
    """Wilder true range with previous-close gap inclusion."""
    prev_close = df["Close"].shift(1)
    ranges = pd.concat(
        [
            df["High"] - df["Low"],
            (df["High"] - prev_close).abs(),
            (df["Low"] - prev_close).abs(),
        ],
        axis=1,
    )
    return ranges.max(axis=1)


def atr(df: pd.DataFrame, period: int) -> pd.Series:
    """Average True Range (SMA of true range) — epsilon-safe downstream."""
    return true_range(df).rolling(window=period, min_periods=period).mean()


def zscore(series: pd.Series, window: int) -> pd.Series:
    """Rolling Z-score with explicit division-by-zero protection."""
    mean = series.rolling(window=window, min_periods=window).mean()
    std = series.rolling(window=window, min_periods=window).std(ddof=0)
    return (series - mean) / (std + EPS)


def detect_structure(df: pd.DataFrame, left: int = 3, right: int = 3) -> pd.DataFrame:
    """
    PILLAR I — Smart Money Concepts.
    Detects confirmed swing highs/lows, Break of Structure (BOS),
    and Change of Character (ChoCh) via a deterministic trend state machine.
    """
    out = df.copy()
    window = left + right + 1

    roll_max = out["High"].rolling(window=window, center=True, min_periods=window).max()
    roll_min = out["Low"].rolling(window=window, center=True, min_periods=window).min()

    is_swing_high = (out["High"] >= roll_max).fillna(False).astype(bool)
    is_swing_low = (out["Low"] <= roll_min).fillna(False).astype(bool)

    out["swing_high"] = out["High"].where(is_swing_high)
    out["swing_low"] = out["Low"].where(is_swing_low)

    # Last CONFIRMED swing, shifted by one bar to remove look-ahead bias
    out["last_swing_high"] = out["swing_high"].ffill().shift(1)
    out["last_swing_low"] = out["swing_low"].ffill().shift(1)

    bos_bull = (
        (out["Close"] > out["last_swing_high"]) & out["last_swing_high"].notna()
    ).fillna(False).to_numpy(dtype=bool)

    bos_bear = (
        (out["Close"] < out["last_swing_low"]) & out["last_swing_low"].notna()
    ).fillna(False).to_numpy(dtype=bool)

    choch_bull = np.zeros(len(out), dtype=bool)
    choch_bear = np.zeros(len(out), dtype=bool)

    trend_state = 0  # 1 = bullish, -1 = bearish, 0 = undetermined
    for i in range(len(out)):
        if bos_bull[i]:
            if trend_state == -1:
                choch_bull[i] = True   # bearish structure invalidated
            trend_state = 1
        if bos_bear[i]:
            if trend_state == 1:
                choch_bear[i] = True   # bullish structure invalidated
            trend_state = -1

    out["bos_bull"] = bos_bull
    out["bos_bear"] = bos_bear
    out["choch_bull"] = choch_bull
    out["choch_bear"] = choch_bear
    out["structure_trend"] = trend_state
    return out


def order_flow_metrics(df: pd.DataFrame, lookback: int = 20):
    """
    PILLAR II — Order Flow / Volume-Weighted Body-to-Wick Displacement.
    High displacement with expanding volume = institutional participation.
    """
    body = (df["Close"] - df["Open"]).abs()
    full_range = (df["High"] - df["Low"]).abs() + EPS
    wick = (full_range - body).clip(lower=0.0)
    displacement = (body / (wick + EPS)).clip(lower=0.0, upper=5.0)

    volume = df["Volume"].replace(0.0, np.nan)
    volume_ma = volume.rolling(window=lookback, min_periods=lookback).mean()
    volume_factor = (volume / (volume_ma + EPS)).fillna(1.0).clip(lower=0.2, upper=3.0)

    flow = (displacement * volume_factor).fillna(0.0)
    direction = np.sign(df["Close"] - df["Open"]).fillna(0.0)
    return flow, direction


def macro_bias(df_macro: pd.DataFrame) -> dict:
    """
    PILLAR IV — Multi-Timeframe Fractal Harmony (1H macro governor).
    Normalised EMA spread expressed in ATR units for scale invariance.
    """
    close = df_macro["Close"]
    ema_fast = close.ewm(span=20, adjust=False).mean()
    ema_slow = close.ewm(span=50, adjust=False).mean()
    macro_atr = atr(df_macro, 14)

    denom = (macro_atr + EPS).replace(0.0, EPS)
    slope_series = (ema_fast - ema_slow) / denom
    slope = safe_float(slope_series.iloc[-1], 0.0)

    if slope > MACRO_SLOPE_THRESHOLD:
        bias = 1
    elif slope < -MACRO_SLOPE_THRESHOLD:
        bias = -1
    else:
        bias = 0

    return {
        "slope": slope,
        "bias": bias,
        "ema_fast": safe_float(ema_fast.iloc[-1], 0.0),
        "ema_slow": safe_float(ema_slow.iloc[-1], 0.0),
        "macro_atr": safe_float(macro_atr.iloc[-1], 0.0),
        "bars": int(len(df_macro)),
    }


def build_feature_frame(df: pd.DataFrame, atr_period: int, z_window: int = Z_WINDOW) -> pd.DataFrame:
    """Assemble the complete vectorised feature matrix for a given ATR period."""
    feat = detect_structure(df)
    feat["atr"] = atr(feat, atr_period)
    feat["z"] = zscore(feat["Close"], z_window)
    flow, flow_dir = order_flow_metrics(feat)
    feat["flow"] = flow
    feat["flow_dir"] = flow_dir
    feat["ema_fast"] = feat["Close"].ewm(span=20, adjust=False).mean()
    feat["ema_slow"] = feat["Close"].ewm(span=50, adjust=False).mean()

    # PILLAR V — volatility regime percentile (percentile rank of ATR/price)
    atr_ratio = feat["atr"] / (feat["Close"] + EPS)
    feat["atr_ratio_pct"] = atr_ratio.rolling(window=100, min_periods=50).rank(pct=True)
    feat["atr_ratio_pct"] = feat["atr_ratio_pct"].fillna(0.0)
    return feat


# ==============================================================================
# SECTION 7 — SLIPPAGE-AWARE BACKTEST ENGINE
# ==============================================================================

def run_backtest(
    feat: pd.DataFrame,
    z_threshold: float,
    atr_mult: float = ATR_MULT,
    rr: float = RR_TARGET,
    balance: float = ACCOUNT_BALANCE,
    risk_pct: float = RISK_PERCENTAGE,
    spread: float = SPREAD_USD,
    slippage: float = SLIPPAGE_USD,
    warmup: int = WARMUP_BARS,
) -> dict:
    """
    Event-driven, spread- and slippage-aware historical simulation.
    Stop-loss is checked BEFORE take-profit on every bar (conservative fill).
    """
    n = len(feat)
    empty = {
        "trades": [], "equity_usd": [balance], "total_trades": 0,
        "wins": 0, "losses": 0, "win_rate": 0.0, "profit_factor": 0.0,
        "max_drawdown": 0.0, "net_r": 0.0, "avg_r": 0.0,
    }
    if n <= warmup + 5:
        return empty

    high = feat["High"].to_numpy(dtype=float)
    low = feat["Low"].to_numpy(dtype=float)
    close = feat["Close"].to_numpy(dtype=float)
    atr_arr = feat["atr"].to_numpy(dtype=float)
    z_arr = feat["z"].to_numpy(dtype=float)
    atr_pct = feat["atr_ratio_pct"].to_numpy(dtype=float)
    ema_slow = feat["ema_slow"].to_numpy(dtype=float)

    cost = (spread / 2.0) + slippage
    risk_usd_per_trade = balance * (risk_pct / 100.0)

    trades: list[dict] = []
    equity_r: list[float] = [0.0]

    i = warmup
    while i < n:
        if not (
            np.isfinite(z_arr[i])
            and np.isfinite(atr_arr[i])
            and np.isfinite(atr_pct[i])
            and np.isfinite(ema_slow[i])
        ) or atr_arr[i] <= EPS:
            i += 1
            continue

        direction = 0
        if z_arr[i] <= -z_threshold and close[i] > ema_slow[i] and atr_pct[i] >= REGIME_PCT_FLOOR:
            direction = 1
        elif z_arr[i] >= z_threshold and close[i] < ema_slow[i] and atr_pct[i] >= REGIME_PCT_FLOOR:
            direction = -1

        if direction == 0:
            i += 1
            continue

        risk_distance = atr_mult * atr_arr[i]
        if risk_distance <= EPS:
            i += 1
            continue

        if direction == 1:
            entry = close[i] + cost
            stop_loss = entry - risk_distance
            take_profit = entry + (rr * risk_distance)
        else:
            entry = close[i] - cost
            stop_loss = entry + risk_distance
            take_profit = entry - (rr * risk_distance)

        exit_price = None
        exit_reason = "EOD"
        j = i + 1
        while j < n:
            if direction == 1:
                if low[j] <= stop_loss:
                    exit_price, exit_reason = stop_loss - cost, "SL"
                    break
                if high[j] >= take_profit:
                    exit_price, exit_reason = take_profit - cost, "TP"
                    break
            else:
                if high[j] >= stop_loss:
                    exit_price, exit_reason = stop_loss + cost, "SL"
                    break
                if low[j] <= take_profit:
                    exit_price, exit_reason = take_profit + cost, "TP"
                    break
            j += 1

        if exit_price is None:
            j = n - 1
            exit_price = close[-1] - cost if direction == 1 else close[-1] + cost
            exit_reason = "EOD"

        if direction == 1:
            r_multiple = (exit_price - entry) / risk_distance
        else:
            r_multiple = (entry - exit_price) / risk_distance

        trades.append(
            {
                "entry_index": int(i),
                "exit_index": int(j),
                "direction": "BUY" if direction == 1 else "SELL",
                "entry": float(entry),
                "exit": float(exit_price),
                "r": float(r_multiple),
                "reason": exit_reason,
            }
        )
        equity_r.append(equity_r[-1] + float(r_multiple))
        i = max(j, i + 1)

    total = len(trades)
    if total == 0:
        return empty

    r_values = np.array([t["r"] for t in trades], dtype=float)
    wins = int(np.sum(r_values > 0))
    losses = int(np.sum(r_values <= 0))
    gross_profit = float(np.sum(r_values[r_values > 0]))
    gross_loss = float(abs(np.sum(r_values[r_values <= 0])))
    profit_factor = gross_profit / (gross_loss + EPS)
    if gross_loss <= EPS:
        profit_factor = min(gross_profit, 99.0)

    equity_usd = balance + np.array(equity_r, dtype=float) * risk_usd_per_trade
    running_peak = np.maximum.accumulate(equity_usd)
    drawdown = (equity_usd - running_peak) / np.where(running_peak <= EPS, EPS, running_peak)
    max_drawdown = float(abs(np.min(drawdown)) * 100.0)

    return {
        "trades": trades,
        "equity_usd": equity_usd.tolist(),
        "total_trades": total,
        "wins": wins,
        "losses": losses,
        "win_rate": round((wins / total) * 100.0, 2),
        "profit_factor": round(profit_factor, 4),
        "max_drawdown": round(max_drawdown, 2),
        "net_r": round(float(equity_r[-1]), 4),
        "avg_r": round(float(np.mean(r_values)), 4),
    }


# ==============================================================================
# SECTION 8 — GRID SEARCH PARAMETER OPTIMIZATION
# ==============================================================================

def grid_search_optimize(df: pd.DataFrame) -> tuple[dict, list[dict]]:
    """
    Exhaustive grid search over (ATR period x Z-score threshold).
    Selection metric: Profit Factor, tie-broken by Win Rate then Net R.
    """
    log("Grid Search optimization started.")
    results: list[dict] = []

    for atr_period in GRID_ATR_PERIODS:
        feat = build_feature_frame(df, atr_period)
        for z_threshold in GRID_Z_THRESHOLDS:
            metrics = run_backtest(feat, z_threshold)
            record = {
                "atr_period": atr_period,
                "z_threshold": z_threshold,
                "total_trades": metrics["total_trades"],
                "win_rate": metrics["win_rate"],
                "profit_factor": metrics["profit_factor"],
                "max_drawdown": metrics["max_drawdown"],
                "net_r": metrics["net_r"],
                "avg_r": metrics["avg_r"],
            }
            results.append(record)
            log(
                f"  ATR={atr_period:<3} Z={z_threshold:<4} | "
                f"Trades={record['total_trades']:<4} WR={record['win_rate']:>6.2f}% "
                f"PF={record['profit_factor']:>7.4f} DD={record['max_drawdown']:>6.2f}% "
                f"NetR={record['net_r']:>8.4f}"
            )

    eligible = [r for r in results if r["total_trades"] >= MIN_TRADES_FOR_OPTIMUM]
    pool = eligible if eligible else results
    best = max(
        pool,
        key=lambda r: (r["profit_factor"], r["win_rate"], r["net_r"]),
    )

    log(
        f"Grid Search winner -> ATR={best['atr_period']} | Z={best['z_threshold']} | "
        f"PF={best['profit_factor']} | WR={best['win_rate']}% | DD={best['max_drawdown']}%"
    )
    forensic_audit_log("GRID SEARCH OPTIMIZATION", {
        "combinations_evaluated": len(results),
        "eligible_combinations": len(eligible),
        "optimal_atr_period": best["atr_period"],
        "optimal_z_threshold": best["z_threshold"],
        "optimal_profit_factor": best["profit_factor"],
        "optimal_win_rate": best["win_rate"],
        "optimal_max_drawdown": best["max_drawdown"],
        "optimal_net_r": best["net_r"],
    })
    return best, results


# ==============================================================================
# SECTION 9 — MATPLOTLIB BACKTEST CHART (AGG BACKEND)
# ==============================================================================

def generate_backtest_chart(equity_usd: list[float], metrics: dict, best: dict) -> str:
    """Render equity curve + drawdown to backtest_performance.png (headless)."""
    equity = np.array(equity_usd, dtype=float)
    if equity.size < 2:
        equity = np.array([ACCOUNT_BALANCE, ACCOUNT_BALANCE], dtype=float)

    x_axis = np.arange(equity.size)
    running_peak = np.maximum.accumulate(equity)
    drawdown_pct = (equity - running_peak) / np.where(running_peak <= EPS, EPS, running_peak) * 100.0

    fig, (ax_equity, ax_dd) = plt.subplots(
        2, 1, figsize=(12, 8), sharex=True,
        gridspec_kw={"height_ratios": [2.2, 1.0]},
    )
    fig.suptitle(
        f"{ENGINE_NAME} - XAUUSD Backtest Performance\n"
        f"Optimal ATR={best['atr_period']} | Z-Threshold={best['z_threshold']} | "
        f"PF={metrics['profit_factor']:.2f} | WR={metrics['win_rate']:.1f}% | "
        f"MaxDD={metrics['max_drawdown']:.1f}%",
        fontsize=12, fontweight="bold",
    )

    ax_equity.plot(x_axis, equity, color="#0F9D58", linewidth=1.8, label="Equity (USD)")
    ax_equity.fill_between(x_axis, equity, float(np.min(equity)), color="#0F9D58", alpha=0.12)
    ax_equity.axhline(ACCOUNT_BALANCE, color="#5F6368", linestyle="--", linewidth=1.0,
                      label="Initial Balance")
    ax_equity.set_ylabel("Equity (USD)", fontweight="bold")
    ax_equity.grid(True, alpha=0.25)
    ax_equity.legend(loc="upper left", fontsize=9)

    ax_dd.fill_between(x_axis, drawdown_pct, 0.0, color="#D93025", alpha=0.35)
    ax_dd.plot(x_axis, drawdown_pct, color="#D93025", linewidth=1.2)
    ax_dd.set_ylabel("Drawdown (%)", fontweight="bold")
    ax_dd.set_xlabel("Trade Sequence", fontweight="bold")
    ax_dd.grid(True, alpha=0.25)

    fig.tight_layout(rect=(0, 0, 1, 0.94))
    fig.savefig(CHART_PATH, dpi=130, bbox_inches="tight")
    plt.close(fig)
    log(f"Backtest chart written -> {CHART_PATH}")
    return CHART_PATH


# ==============================================================================
# SECTION 10 — POSITION SIZING
# ==============================================================================

def calculate_position_size(balance: float, risk_pct: float, entry: float, stop_loss: float) -> dict:
    """Risk-based lot sizing, floored to the 0.01 lot broker increment."""
    risk_usd = balance * (risk_pct / 100.0)
    sl_distance = abs(entry - stop_loss)

    if sl_distance < EPS:
        return {"lot": 0.01, "risk_usd": round(risk_usd, 2), "sl_distance": 0.0, "raw_lot": 0.0}

    raw_lot = risk_usd / (sl_distance * CONTRACT_SIZE)
    lot = math.floor(raw_lot / 0.01) * 0.01
    lot = round(max(0.01, min(100.0, lot)), 2)
    actual_risk = round(lot * sl_distance * CONTRACT_SIZE, 2)

    return {
        "lot": lot,
        "risk_usd": actual_risk,
        "sl_distance": round(sl_distance, 4),
        "raw_lot": round(raw_lot, 4),
    }


# ==============================================================================
# SECTION 11 — 5-PILLAR HYBRID CONFLUENCE ENGINE
# ==============================================================================

def evaluate_pillars(feat: pd.DataFrame, macro: dict, z_threshold: float) -> dict:
    """
    Score both directions across the five institutional pillars.
    Returns a decision dictionary containing every raw input value.
    """
    row = feat.iloc[-1]

    close = safe_float(row["Close"], 0.0)
    atr_value = safe_float(row["atr"], 0.0)
    z_value = safe_float(row["z"], 0.0)
    flow_value = safe_float(row["flow"], 0.0)
    flow_dir = safe_float(row["flow_dir"], 0.0)
    atr_pct = safe_float(row["atr_ratio_pct"], 0.0)
    ema_fast = safe_float(row["ema_fast"], 0.0)
    ema_slow = safe_float(row["ema_slow"], 0.0)
    last_swing_high = safe_float(row["last_swing_high"], np.nan)
    last_swing_low = safe_float(row["last_swing_low"], np.nan)

    bos_bull = bool(row["bos_bull"])
    bos_bear = bool(row["bos_bear"])
    choch_bull = bool(row["choch_bull"])
    choch_bear = bool(row["choch_bear"])

    # ---- PILLAR I: SMC structure bias ----
    structure_bias = 0
    if np.isfinite(last_swing_high) and close > last_swing_high:
        structure_bias = 1
    elif np.isfinite(last_swing_low) and close < last_swing_low:
        structure_bias = -1
    if choch_bull or bos_bull:
        structure_bias = max(structure_bias, 1)
    if choch_bear or bos_bear:
        structure_bias = min(structure_bias, -1)
    if structure_bias == 0:
        if ema_fast > ema_slow:
            structure_bias = 1
        elif ema_fast < ema_slow:
            structure_bias = -1

    # ---- PILLAR IV: macro fractal harmony ----
    mtf_bias = int(macro["bias"])

    # ---- PILLAR V: regime governor ----
    regime = "TRENDING" if atr_pct >= REGIME_PCT_FLOOR else "CHOPPY"

    def score_side(side: int) -> float:
        score = 0.0
        if structure_bias == side:
            score += 25.0
        if int(np.sign(flow_dir)) == side:
            score += 20.0 if flow_value >= FLOW_STRONG else 10.0
        if side == 1:
            if z_value <= -z_threshold:
                score += 20.0
            elif z_value <= -1.0:
                score += 10.0
        else:
            if z_value >= z_threshold:
                score += 20.0
            elif z_value >= 1.0:
                score += 10.0
        if mtf_bias == side:
            score += 20.0
        if regime == "TRENDING":
            score += 15.0
        return round(score, 2)

    long_score = score_side(1)
    short_score = score_side(-1)

    direction = 0
    if long_score >= MIN_CONFLUENCE and long_score > short_score:
        direction = 1
    elif short_score >= MIN_CONFLUENCE and short_score > long_score:
        direction = -1

    confluence = max(long_score, short_score)

    return {
        "direction": direction,
        "confluence": round(confluence, 2),
        "long_score": long_score,
        "short_score": short_score,
        "close": close,
        "atr": atr_value,
        "z": z_value,
        "flow": flow_value,
        "flow_dir": flow_dir,
        "atr_ratio_pct": atr_pct,
        "ema_fast": ema_fast,
        "ema_slow": ema_slow,
        "last_swing_high": last_swing_high,
        "last_swing_low": last_swing_low,
        "bos_bull": bos_bull,
        "bos_bear": bos_bear,
        "choch_bull": choch_bull,
        "choch_bear": choch_bear,
        "structure_bias": structure_bias,
        "mtf_bias": mtf_bias,
        "macro_slope": macro["slope"],
        "regime": regime,
        "z_threshold": z_threshold,
    }


# ==============================================================================
# SECTION 12 — AUTO-TRAILING STOP ENGINE
# ==============================================================================

def manage_trailing_positions(latest_price: float, latest_atr: float) -> list[str]:
    """
    Manage all OPEN positions:
      Stage 1 — TP1 breached  -> Stop Loss to BREAKEVEN
      Stage 2 — Trail active  -> Stop trails at 0.5 x ATR behind the extreme
      Terminal — SL / TP3    -> Position closed and archived
    """
    events: list[str] = []
    if not os.path.exists(DB_PATH):
        return events

    conn = db_connect()
    try:
        rows = conn.execute(
            "SELECT * FROM active_positions WHERE status = 'OPEN';"
        ).fetchall()

        for row in rows:
            pos_id = int(row["id"])
            direction = str(row["direction"])
            entry = float(row["entry"])
            stop_loss = float(row["stop_loss"])
            tp1 = float(row["tp1"])
            tp3 = float(row["tp3"])
            extreme = float(row["extreme_price"])
            stage = int(row["trailing_stage"])

            new_stop = stop_loss
            new_stage = stage
            status = "OPEN"
            note = ""

            if direction == "BUY":
                extreme = max(extreme, latest_price)
                if latest_price >= tp1 and new_stage < 1:
                    new_stop = max(new_stop, entry)
                    new_stage = 1
                    note = f"#{pos_id} TP1 breached -> SL to BREAKEVEN ({entry:.2f})"
                if new_stage >= 1 and latest_atr > 0:
                    trail_candidate = extreme - (TRAIL_ATR_FRACTION * latest_atr)
                    if trail_candidate > new_stop:
                        new_stop = trail_candidate
                        new_stage = max(new_stage, 2)
                        note = f"#{pos_id} ATR trail -> SL {new_stop:.2f}"
                if latest_price <= new_stop:
                    status = "CLOSED_SL"
                    note = f"#{pos_id} closed by trailing stop @ {new_stop:.2f}"
                elif latest_price >= tp3:
                    status = "CLOSED_TP3"
                    note = f"#{pos_id} closed at TP3 @ {tp3:.2f}"
            else:
                extreme = min(extreme, latest_price)
                if latest_price <= tp1 and new_stage < 1:
                    new_stop = min(new_stop, entry)
                    new_stage = 1
                    note = f"#{pos_id} TP1 breached -> SL to BREAKEVEN ({entry:.2f})"
                if new_stage >= 1 and latest_atr > 0:
                    trail_candidate = extreme + (TRAIL_ATR_FRACTION * latest_atr)
                    if trail_candidate < new_stop:
                        new_stop = trail_candidate
                        new_stage = max(new_stage, 2)
                        note = f"#{pos_id} ATR trail -> SL {new_stop:.2f}"
                if latest_price >= new_stop:
                    status = "CLOSED_SL"
                    note = f"#{pos_id} closed by trailing stop @ {new_stop:.2f}"
                elif latest_price <= tp3:
                    status = "CLOSED_TP3"
                    note = f"#{pos_id} closed at TP3 @ {tp3:.2f}"

            conn.execute(
                "UPDATE active_positions SET stop_loss = ?, extreme_price = ?, "
                "trailing_stage = ?, last_update = ?, status = ? WHERE id = ?;",
                (new_stop, extreme, new_stage, utc_stamp(), status, pos_id),
            )
            if note:
                events.append(note)
                log(f"TRAILING :: {note}", "TRAILING")

        conn.commit()
        return events
    finally:
        conn.close()


# ==============================================================================
# SECTION 13 — WEBHOOK DELIVERY (TELEGRAM + DISCORD)
# ==============================================================================

def html_to_plain(text: str) -> str:
    """Strip HTML tags for Discord's non-HTML renderer."""
    text = re.sub(r"<br\s*/?>", "\n", text)
    text = re.sub(r"<[^>]+>", "", text)
    return html_lib.unescape(text)


def send_telegram(message: str) -> bool:
    """Dispatch to the Telegram Bot API with exponential backoff retries."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log("Telegram credentials absent — dispatch skipped.", "WARNING")
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }

    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.post(url, json=payload, timeout=HTTP_TIMEOUT)
            if response.status_code == 200 and response.json().get("ok", False):
                log("Telegram dispatch OK.")
                return True
            last_error = f"HTTP {response.status_code}: {response.text[:300]}"
        except Exception as exc:
            last_error = f"{type(exc).__name__}: {exc}"
        if attempt < MAX_RETRIES:
            time.sleep(2.0 * attempt)
    raise RuntimeError(f"Telegram dispatch failed -> {last_error}")


def send_discord(message: str) -> bool:
    """Dispatch to a Discord webhook (plain text, chunked to 1900 chars)."""
    if not DISCORD_WEBHOOK_URL:
        log("Discord webhook absent — dispatch skipped.", "WARNING")
        return False

    plain = html_to_plain(message)
    chunks = [plain[i:i + 1900] for i in range(0, len(plain), 1900)] or [plain]

    for index, chunk in enumerate(chunks, start=1):
        payload = {"content": chunk, "username": f"{ENGINE_NAME}"}
        last_error = None
        delivered = False
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = requests.post(
                    DISCORD_WEBHOOK_URL, json=payload, timeout=HTTP_TIMEOUT
                )
                if response.status_code in (200, 204):
                    delivered = True
                    break
                last_error = f"HTTP {response.status_code}: {response.text[:300]}"
            except Exception as exc:
                last_error = f"{type(exc).__name__}: {exc}"
            if attempt < MAX_RETRIES:
                time.sleep(2.0 * attempt)
        if not delivered:
            raise RuntimeError(f"Discord chunk {index}/{len(chunks)} failed -> {last_error}")

    log(f"Discord dispatch OK ({len(chunks)} chunk(s)).")
    return True


def notify_error(context: str, exc: Exception) -> None:
    """Telegram Error Handler — fires on any caught critical exception."""
    tb = traceback.format_exc()
    log(f"ERROR HANDLER >> {context} :: {type(exc).__name__}: {exc}", "ERROR")
    print(tb, flush=True)

    message = (
        f"🚨 <b>{ENGINE_NAME} — ERROR HANDLER</b> 🚨\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚠️ <b>Context:</b> {context}\n"
        f"🧨 <b>Exception:</b> {type(exc).__name__}\n"
        f"📝 <b>Detail:</b> {str(exc)[:600]}\n"
        f"🕒 <b>Timestamp (UTC):</b> {utc_stamp()}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━"
    )
    try:
        send_telegram(message)
    except Exception as nested:  # pragma: no cover - defensive
        log(f"Error-handler Telegram fallback failed: {nested}", "ERROR")

    db_log_event("ERROR", context, f"{type(exc).__name__}: {exc}")


def dispatch_alerts(message: str) -> dict:
    """Parallel dual-channel delivery with isolated failure domains."""
    results = {"telegram": False, "discord": False}
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = {
            "telegram": pool.submit(send_telegram, message),
            "discord": pool.submit(send_discord, message),
        }
        for name, future in futures.items():
            try:
                results[name] = bool(future.result(timeout=90))
            except Exception as exc:
                results[name] = False
                notify_error(f"WEBHOOK::{name.upper()}", exc)
    return results


# ==============================================================================
# SECTION 14 — INSTITUTIONAL FORENSIC REPORT TEMPLATE
# ==============================================================================

def build_signal_message(ctx: dict) -> str:
    """Render the mandated institutional forensic report."""
    lines = [
        f"🛡️ <b>{ENGINE_NAME} ({ENGINE_EDITION})</b> 🛡️",
        "━━━━━━━━━━━━━━━━━━━━━━",
        "🌟 <b>Asset:</b> XAUUSD (SPOT GOLD)",
        f"📊 <b>Signal Decision:</b> {ctx['decision']}",
        f"🎯 <b>Precision Entry:</b> {ctx['entry']:.2f}",
        f"🛑 <b>Forensic Stop Loss:</b> {ctx['stop_loss']:.2f}",
        f"📈 <b>Take Profit 1 (1.5R):</b> {ctx['tp1']:.2f}",
        f"📈 <b>Take Profit 2 (2.5R):</b> {ctx['tp2']:.2f}",
        f"🚀 <b>Take Profit 3 (4.0R):</b> {ctx['tp3']:.2f}",
        f"🔄 <b>Trailing Stop Protocol:</b> {ctx['trailing_protocol']}",
        "━━━━━━━━━━━━━━━━━━━━━━",
        "⚙️ <b>Grid Search &amp; Visualization:</b>",
        f"• <i>Optimal ATR Period:</i> {ctx['atr_period']}",
        f"• <i>Optimal Z-Threshold:</i> {ctx['z_threshold']:.1f}",
        f"• <i>Backtest Chart:</i> Generated &amp; Saved as PNG ({CHART_PATH})",
        "━━━━━━━━━━━━━━━━━━━━━━",
        "💰 <b>Risk &amp; Position Sizing:</b>",
        f"• <i>Recommended Lot:</i> {ctx['lot']:.2f} Lots ({RISK_PERCENTAGE:.2f}% Risk)",
        f"• <i>Risk Amount (USD):</i> ${ctx['risk_usd']:.2f}",
        "━━━━━━━━━━━━━━━━━━━━━━",
        "🔬 <b>Forensic Audit, Database &amp; Backtest:</b>",
        f"• <i>Market Regime:</i> {ctx['regime']}",
        f"• <i>Confluence Score:</i> {ctx['confluence']:.0f}/100",
        f"• <i>SQLite DB Status:</i> {ctx['db_status']}",
        f"• <i>Error Handler Status:</i> {ctx['error_handler_status']}",
        f"• <i>Optimized Win Rate:</i> {ctx['win_rate']:.1f}%",
        f"• <i>Profit Factor:</i> {ctx['profit_factor']:.2f}",
        f"• <i>Max Drawdown:</i> {ctx['max_drawdown']:.1f}%",
        f"• <i>Timestamp (UTC):</i> {ctx['timestamp']}",
        "━━━━━━━━━━━━━━━━━━━━━━",
    ]
    return "\n".join(lines)


def build_no_trade_message(ctx: dict) -> str:
    """Compact status report when the confluence gate rejects the setup."""
    return "\n".join(
        [
            f"🛡️ <b>{ENGINE_NAME}</b> 🛡️",
            "━━━━━━━━━━━━━━━━━━━━━━",
            "🌟 <b>Asset:</b> XAUUSD (SPOT GOLD)",
            "📊 <b>Signal Decision:</b> NO TRADE (CONFLUENCE BELOW GATE)",
            f"⚖️ <b>Long Score:</b> {ctx['long_score']:.0f}/100 | "
            f"<b>Short Score:</b> {ctx['short_score']:.0f}/100",
            f"🔬 <b>Market Regime:</b> {ctx['regime']}",
            f"📉 <b>Z-Score:</b> {ctx['z']:.3f} (Threshold {ctx['z_threshold']:.1f})",
            f"💨 <b>Order Flow:</b> {ctx['flow']:.3f} (Direction {ctx['flow_dir']:.0f})",
            f"🧭 <b>Macro Slope:</b> {ctx['macro_slope']:.4f}",
            f"🕒 <b>Timestamp (UTC):</b> {ctx['timestamp']}",
            "━━━━━━━━━━━━━━━━━━━━━━",
        ]
    )


# ==============================================================================
# SECTION 15 — ORCHESTRATION
# ==============================================================================

def main() -> int:
    log("=" * 78)
    log(f"{ENGINE_NAME} ({ENGINE_EDITION}) — INITIALISING")
    log("=" * 78)

    breaker = CircuitBreaker(threshold=3)
    init_db()

    # ------------------------------------------------------------------
    # PHASE 1 — DATA ACQUISITION (circuit-breaker protected)
    # ------------------------------------------------------------------
    try:
        df_exec = fetch_ohlcv(SYMBOL, EXEC_INTERVAL, EXEC_PERIOD, "EXECUTION-15M")
        df_macro = fetch_ohlcv(SYMBOL, MACRO_INTERVAL, MACRO_PERIOD, "MACRO-1H")
    except Exception as exc:
        notify_error("DATA PIPELINE", exc)
        return 1

    try:
        macro = macro_bias(df_macro)
        forensic_audit_log("MACRO FRACTAL (1H)", macro)
    except Exception as exc:
        notify_error("MACRO BIAS MODULE", exc)
        return 1

    # ------------------------------------------------------------------
    # PHASE 2 — GRID SEARCH OPTIMIZATION
    # ------------------------------------------------------------------
    try:
        best, grid_results = grid_search_optimize(df_exec)
    except Exception as exc:
        notify_error("GRID SEARCH OPTIMIZER", exc)
        return 1

    atr_period = int(best["atr_period"])
    z_threshold = float(best["z_threshold"])

    # ------------------------------------------------------------------
    # PHASE 3 — FEATURE ENGINE + OPTIMISED BACKTEST + CHART
    # ------------------------------------------------------------------
    try:
        feat = build_feature_frame(df_exec, atr_period)
        metrics = run_backtest(feat, z_threshold)
        generate_backtest_chart(metrics["equity_usd"], metrics, best)
    except Exception as exc:
        notify_error("BACKTEST / VISUALIZATION", exc)
        return 1

    forensic_audit_log("OPTIMISED BACKTEST METRICS", {
        "atr_period": atr_period,
        "z_threshold": z_threshold,
        "total_trades": metrics["total_trades"],
        "wins": metrics["wins"],
        "losses": metrics["losses"],
        "win_rate_pct": metrics["win_rate"],
        "profit_factor": metrics["profit_factor"],
        "max_drawdown_pct": metrics["max_drawdown"],
        "net_r": metrics["net_r"],
        "avg_r": metrics["avg_r"],
    })

    # ------------------------------------------------------------------
    # PHASE 4 — AUTO-TRAILING STOP MANAGEMENT
    # ------------------------------------------------------------------
    latest_price = safe_float(df_exec["Close"].iloc[-1], 0.0)
    latest_atr = safe_float(feat["atr"].iloc[-1], 0.0)
    try:
        trail_events = manage_trailing_positions(latest_price, latest_atr)
        if trail_events:
            forensic_audit_log("AUTO-TRAILING STOP EVENTS", {
                f"event_{i + 1}": e for i, e in enumerate(trail_events)
            })
        else:
            log("Auto-trailing engine: no open positions to manage.")
    except Exception as exc:
        notify_error("AUTO-TRAILING ENGINE", exc)

    # ------------------------------------------------------------------
    # PHASE 5 — 5-PILLAR CONFLUENCE EVALUATION
    # ------------------------------------------------------------------
    try:
        pillars = evaluate_pillars(feat, macro, z_threshold)
    except Exception as exc:
        notify_error("5-PILLAR EVALUATION", exc)
        return 1

    forensic_audit_log("5-PILLAR RAW MATHEMATICS", {
        "close": pillars["close"],
        "atr_14_optimised": pillars["atr"],
        "z_score": pillars["z"],
        "z_threshold": pillars["z_threshold"],
        "order_flow_displacement": pillars["flow"],
        "order_flow_direction": pillars["flow_dir"],
        "atr_regime_percentile": pillars["atr_ratio_pct"],
        "ema_fast": pillars["ema_fast"],
        "ema_slow": pillars["ema_slow"],
        "last_swing_high": pillars["last_swing_high"],
        "last_swing_low": pillars["last_swing_low"],
        "bos_bull": pillars["bos_bull"],
        "bos_bear": pillars["bos_bear"],
        "choch_bull": pillars["choch_bull"],
        "choch_bear": pillars["choch_bear"],
        "structure_bias": pillars["structure_bias"],
        "mtf_bias": pillars["mtf_bias"],
        "macro_slope": pillars["macro_slope"],
        "market_regime": pillars["regime"],
        "long_score": pillars["long_score"],
        "short_score": pillars["short_score"],
    })

    if pillars["direction"] == 0:
        log(
            f"No verified signal. Long={pillars['long_score']:.0f} "
            f"Short={pillars['short_score']:.0f} Gate={MIN_CONFLUENCE:.0f}",
            "INFO",
        )
        db_log_event(
            "INFO", "DECISION",
            f"NO_TRADE long={pillars['long_score']} short={pillars['short_score']} "
            f"regime={pillars['regime']}",
        )
        if SEND_NO_TRADE_ALERT:
            try:
                dispatch_alerts(build_no_trade_message({
                    **pillars, "timestamp": utc_stamp(),
                }))
            except Exception as exc:
                notify_error("NO-TRADE NOTIFICATION", exc)
        return 0

    # ------------------------------------------------------------------
    # PHASE 6 — LEVEL CONSTRUCTION & POSITION SIZING
    # ------------------------------------------------------------------
    direction = int(pillars["direction"])
    entry = round(pillars["close"], 2)
    risk_distance = max(pillars["atr"] * ATR_MULT, 0.10)

    if direction == 1:
        decision_label = "VERIFIED BUY"
        stop_loss = round(entry - risk_distance, 2)
        tp1 = round(entry + (TP1_R * risk_distance), 2)
        tp2 = round(entry + (TP2_R * risk_distance), 2)
        tp3 = round(entry + (TP3_R * risk_distance), 2)
    else:
        decision_label = "VERIFIED SELL"
        stop_loss = round(entry + risk_distance, 2)
        tp1 = round(entry - (TP1_R * risk_distance), 2)
        tp2 = round(entry - (TP2_R * risk_distance), 2)
        tp3 = round(entry - (TP3_R * risk_distance), 2)

    sizing = calculate_position_size(ACCOUNT_BALANCE, RISK_PERCENTAGE, entry, stop_loss)

    forensic_audit_log("RISK & POSITION SIZING", {
        "account_balance": ACCOUNT_BALANCE,
        "risk_percentage": RISK_PERCENTAGE,
        "entry": entry,
        "stop_loss": stop_loss,
        "risk_distance": sizing["sl_distance"],
        "raw_lot": sizing["raw_lot"],
        "final_lot": sizing["lot"],
        "risk_usd": sizing["risk_usd"],
        "tp1": tp1, "tp2": tp2, "tp3": tp3,
        "rrr_tp1": TP1_R, "rrr_tp2": TP2_R, "rrr_tp3": TP3_R,
    })

    ctx = {
        "timestamp": utc_stamp(),
        "symbol": SYMBOL,
        "decision": decision_label,
        "entry": entry,
        "stop_loss": stop_loss,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "lot": sizing["lot"],
        "risk_usd": sizing["risk_usd"],
        "confluence": pillars["confluence"],
        "regime": pillars["regime"],
        "atr_period": atr_period,
        "z_threshold": z_threshold,
        "win_rate": metrics["win_rate"],
        "profit_factor": metrics["profit_factor"],
        "max_drawdown": metrics["max_drawdown"],
        "trailing_protocol": "ARMED — Auto-shift to BREAKEVEN @ TP1, then 0.5xATR trail",
        "db_status": "Pending",
        "error_handler_status": "Active / Nominal",
    }

    # ------------------------------------------------------------------
    # PHASE 7 — SQLITE PERSISTENCE (BEFORE ANY DISPATCH)
    # ------------------------------------------------------------------
    signal_id = None
    try:
        signal_id = db_log_signal(ctx)
        db_create_active_position(signal_id, ctx)
        ctx["db_status"] = f"Logged Successfully (ID={signal_id})"
        log(f"Signal persisted to SQLite -> signals.id={signal_id}")
        forensic_audit_log("SQLITE PERSISTENCE", {
            "signal_id": signal_id,
            "active_position": "registered",
            "db_path": DB_PATH,
        })
    except Exception as exc:
        ctx["db_status"] = "PERSISTENCE FAILURE"
        notify_error("SQLITE PERSISTENCE", exc)
        breaker.record_failure("sqlite_persistence")

    # ------------------------------------------------------------------
    # PHASE 8 — DUAL-CHANNEL DISPATCH
    # ------------------------------------------------------------------
    message = build_signal_message(ctx)
    log("Dispatching institutional forensic report (Telegram + Discord in parallel)...")
    try:
        delivery = dispatch_alerts(message)
        log(f"Delivery report: Telegram={delivery['telegram']} Discord={delivery['discord']}")
        db_log_event(
            "INFO", "DISPATCH",
            f"telegram={delivery['telegram']} discord={delivery['discord']} signal_id={signal_id}",
        )
    except Exception as exc:
        notify_error("DUAL-CHANNEL DISPATCH", exc)

    log("=" * 78)
    log(f"{ENGINE_NAME} — CYCLE COMPLETE")
    log("=" * 78)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except CircuitBreakerOpen as breaker_exc:
        log(f"CIRCUIT BREAKER TRIPPED: {breaker_exc}", "CRITICAL")
        notify_error("CIRCUIT BREAKER", breaker_exc)
        sys.exit(2)
    except Exception as fatal_exc:  # pragma: no cover - last-resort guard
        notify_error("FATAL UNHANDLED EXCEPTION", fatal_exc)
        sys.exit(1)
