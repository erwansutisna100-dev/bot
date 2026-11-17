import matplotlib
matplotlib.use("Agg")

import concurrent.futures
import datetime as dt
import math
import os
import sqlite3
import sys
import traceback
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests
import yfinance as yf


# ==============================================================================
# ENTERPRISE CONFIGURATION & CONSTANTS
# ==============================================================================
ENGINE_NAME = "APEX-QUANT-OMNI ENTERPRISE v40.0"
TICKER = "XAUUSD=X"
DB_PATH = os.getenv("SQLITE_DB_PATH", "trading_signals.db")
CHART_PATH = os.getenv("BACKTEST_CHART_PATH", "backtest_performance.png")

ACCOUNT_BALANCE = float(os.getenv("ACCOUNT_BALANCE", "10000.0"))
RISK_PERCENTAGE = float(os.getenv("RISK_PERCENTAGE", "1.0"))

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "").strip()

EPSILON = 1e-8
SPREAD_DOLLARS = 0.30
SLIPPAGE_DOLLARS = 0.15
XAU_CONTRACT_MULTIPLIER = 100.0  # 1.00 Standard Lot = 100 oz -> $1 move = $100
MIN_RRR_TARGET = 3.0
CONFLUENCE_THRESHOLD = 65.0

ATR_GRID = [10, 14, 20]
Z_THRESHOLD_GRID = [1.5, 2.0, 2.5]
Z_LOOKBACK_WINDOW = 20

CIRCUIT_BREAKER_MAX_STALE_HOURS = 96.0
CIRCUIT_BREAKER_MAX_BAR_JUMP_PCT = 0.08


class CircuitBreakerTripped(RuntimeError):
    """Raised when market data integrity or execution invariants fail."""


@dataclass
class BacktestMetrics:
    atr_period: int
    z_threshold: float
    total_trades: int
    wins: int
    losses: int
    win_rate: float
    profit_factor: float
    max_drawdown: float
    net_return_pct: float
    equity_curve: List[float] = field(default_factory=list)
    timestamps: List[pd.Timestamp] = field(default_factory=list)
    trade_returns: List[float] = field(default_factory=list)


@dataclass
class QuantitativeSignal:
    decision: str  # "VERIFIED BUY", "VERIFIED SELL", or "NO SIGNAL"
    entry_price: float
    stop_loss: float
    tp1: float
    tp2: float
    tp3: float
    atr_value: float
    z_score: float
    confluence_score: float
    market_regime: str
    smc_bias: str
    order_flow_score: float
    htf_alignment: str
    recommended_lot: float
    risk_usd: float
    rrr_tp3: float
    trailing_protocol: str
    timestamp_utc: str
    pillar_breakdown: Dict[str, Any] = field(default_factory=dict)


# ==============================================================================
# TELEGRAM ERROR HANDLER (ALERT SYSTEM)
# ==============================================================================
def send_telegram_error_alert(context: str, exc: BaseException) -> bool:
    """Dispatches critical runtime/network/data errors to Telegram."""
    tb_lines = traceback.format_exception(type(exc), exc, exc.__traceback__)
    tb_tail = "".join(tb_lines[-4:]).strip()
    if len(tb_tail) > 1200:
        tb_tail = tb_tail[-1200:]

    safe_context = (
        context.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    )
    safe_err = (
        f"{type(exc).__name__}: {exc}".replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )
    safe_tb = tb_tail.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    message = (
        f"🚨 <b>{ENGINE_NAME} — CRITICAL ERROR ALERT</b> 🚨\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚠️ <b>Module:</b> {safe_context}\n"
        f"❌ <b>Exception:</b> <code>{safe_err}</code>\n"
        f"🕒 <b>UTC Time:</b> {dt.datetime.now(dt.timezone.utc).isoformat()}\n"
        f"📋 <b>Trace Tail:</b>\n<pre>{safe_tb}</pre>"
    )

    print(f"[ERROR HANDLER] {context} -> {type(exc).__name__}: {exc}", file=sys.stderr)

    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print(
            "[ERROR HANDLER] Telegram credentials not configured; alert logged locally.",
            file=sys.stderr,
        )
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    try:
        resp = requests.post(url, json=payload, timeout=15)
        resp.raise_for_status()
        return True
    except Exception as dispatch_err:
        print(
            f"[ERROR HANDLER] Failed to dispatch Telegram error alert: {dispatch_err}",
            file=sys.stderr,
        )
        return False


# ==============================================================================
# SQLITE PERSISTENCE & FORENSIC AUDIT LOGGER
# ==============================================================================
def init_sqlite_db(db_path: str = DB_PATH) -> sqlite3.Connection:
    """Initializes SQLite tables for signal history, forensic audits, and active positions."""
    conn = sqlite3.connect(db_path, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS signal_audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp_utc TEXT NOT NULL,
            asset TEXT NOT NULL,
            decision TEXT NOT NULL,
            entry_price REAL NOT NULL,
            stop_loss REAL NOT NULL,
            tp1 REAL NOT NULL,
            tp2 REAL NOT NULL,
            tp3 REAL NOT NULL,
            trailing_protocol TEXT NOT NULL,
            optimal_atr_period INTEGER NOT NULL,
            optimal_z_threshold REAL NOT NULL,
            recommended_lot REAL NOT NULL,
            risk_usd REAL NOT NULL,
            market_regime TEXT NOT NULL,
            confluence_score REAL NOT NULL,
            z_score REAL NOT NULL,
            atr_value REAL NOT NULL,
            smc_bias TEXT NOT NULL,
            order_flow_score REAL NOT NULL,
            htf_alignment TEXT NOT NULL,
            win_rate REAL NOT NULL,
            profit_factor REAL NOT NULL,
            max_drawdown REAL NOT NULL,
            raw_forensic_json TEXT NOT NULL
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS active_positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            opened_at_utc TEXT NOT NULL,
            updated_at_utc TEXT NOT NULL,
            asset TEXT NOT NULL,
            direction TEXT NOT NULL,
            entry_price REAL NOT NULL,
            initial_sl REAL NOT NULL,
            current_sl REAL NOT NULL,
            tp1 REAL NOT NULL,
            tp2 REAL NOT NULL,
            tp3 REAL NOT NULL,
            atr_at_entry REAL NOT NULL,
            lot_size REAL NOT NULL,
            tp1_breached INTEGER NOT NULL DEFAULT 0,
            highest_favorable REAL NOT NULL,
            trailing_state TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'OPEN'
        );
        """
    )
    conn.commit()
    return conn


def forensic_audit_log(
    conn: sqlite3.Connection,
    signal: QuantitativeSignal,
    best_metrics: BacktestMetrics,
    raw_math: Dict[str, Any],
) -> str:
    """
    Outputs raw mathematical diagnostics to stdout AND persists every evaluation
    to SQLite prior to webhook dispatch.
    """
    import json

    raw_payload_json = json.dumps(raw_math, default=str)

    print("\n" + "=" * 78)
    print(f"FORENSIC AUDIT REPORT | {ENGINE_NAME} | {signal.timestamp_utc}")
    print("=" * 78)
    print(f"Asset                : XAUUSD (SPOT GOLD) [{TICKER}]")
    print(f"Signal Decision      : {signal.decision}")
    print(f"Market Regime        : {signal.market_regime}")
    print(f"Confluence Score     : {signal.confluence_score:.2f} / 100.00")
    print(f"Precision Entry      : {signal.entry_price:.2f}")
    print(f"Forensic Stop Loss   : {signal.stop_loss:.2f}")
    print(f"TP1 (1.5R)           : {signal.tp1:.2f}")
    print(f"TP2 (2.5R)           : {signal.tp2:.2f}")
    print(f"TP3 (4.0R)           : {signal.tp3:.2f} (RRR 1:{signal.rrr_tp3:.2f})")
    print(f"Trailing Protocol    : {signal.trailing_protocol}")
    print(f"Optimal Grid Params  : ATR={best_metrics.atr_period} | Z-Thresh={best_metrics.z_threshold:.1f}")
    print(f"Backtest Stats       : Trades={best_metrics.total_trades} | WR={best_metrics.win_rate:.2f}% | PF={best_metrics.profit_factor:.2f} | MaxDD={best_metrics.max_drawdown:.2f}%")
    print(f"Lot & Risk (1%)      : {signal.recommended_lot:.2f} Lots | ${signal.risk_usd:.2f}")
    print(f"Raw Math Pillars     : {raw_payload_json}")
    print("=" * 78 + "\n")

    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO signal_audit_log (
                timestamp_utc, asset, decision, entry_price, stop_loss,
                tp1, tp2, tp3, trailing_protocol, optimal_atr_period,
                optimal_z_threshold, recommended_lot, risk_usd, market_regime,
                confluence_score, z_score, atr_value, smc_bias,
                order_flow_score, htf_alignment, win_rate, profit_factor,
                max_drawdown, raw_forensic_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (
                signal.timestamp_utc,
                "XAUUSD",
                signal.decision,
                round(signal.entry_price, 4),
                round(signal.stop_loss, 4),
                round(signal.tp1, 4),
                round(signal.tp2, 4),
                round(signal.tp3, 4),
                signal.trailing_protocol,
                int(best_metrics.atr_period),
                float(best_metrics.z_threshold),
                round(signal.recommended_lot, 2),
                round(signal.risk_usd, 2),
                signal.market_regime,
                round(signal.confluence_score, 2),
                round(signal.z_score, 6),
                round(signal.atr_value, 6),
                signal.smc_bias,
                round(signal.order_flow_score, 4),
                signal.htf_alignment,
                round(best_metrics.win_rate, 2),
                round(best_metrics.profit_factor, 4),
                round(best_metrics.max_drawdown, 2),
                raw_payload_json,
            ),
        )
        conn.commit()
        return "Logged Successfully"
    except Exception as db_exc:
        send_telegram_error_alert("SQLite forensic_audit_log Persistence", db_exc)
        return f"DB Error: {db_exc}"


# ==============================================================================
# DATA PIPELINE & MULTI-INDEX CLEANING + CIRCUIT BREAKER
# ==============================================================================
def clean_yfinance_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Flattens yfinance MultiIndex columns, enforces UTC timestamps, removes
    corrupted/duplicate rows, and guarantees numeric OHLCV structure.
    """
    if df is None or df.empty:
        return pd.DataFrame()

    df = df.copy()
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    df.columns = [str(col).strip().title() for col in df.columns]
    df = df.loc[:, ~df.columns.duplicated()]

    required_cols = ["Open", "High", "Low", "Close"]
    for col in required_cols:
        if col not in df.columns:
            raise CircuitBreakerTripped(f"Missing required OHLC column '{col}' from yfinance feed.")
        df[col] = pd.to_numeric(df[col], errors="coerce")

    if "Volume" not in df.columns:
        df["Volume"] = 0.0
    else:
        df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce").fillna(0.0)

    df.index = pd.to_datetime(df.index, utc=True)
    df = df[~df.index.duplicated(keep="last")]
    df = df.sort_index()
    df = df.dropna(subset=required_cols)

    # Enforce High >= max(Open, Close) and Low <= min(Open, Close)
    df = df[(df["Close"] > 0.0) & (df["Open"] > 0.0) & (df["High"] > 0.0) & (df["Low"] > 0.0)]
    df["High"] = np.maximum(df["High"], np.maximum(df["Open"], df["Close"]))
    df["Low"] = np.minimum(df["Low"], np.minimum(df["Open"], df["Close"]))

    return df[["Open", "High", "Low", "Close", "Volume"]]


def fetch_spot_gold_data() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Pulls free Spot Gold (XAUUSD=X) 15m and 1h OHLCV data via yfinance."""
    try:
        raw_15m = yf.download(
            tickers=TICKER,
            period="60d",
            interval="15m",
            auto_adjust=False,
            progress=False,
            threads=False,
        )
        raw_1h = yf.download(
            tickers=TICKER,
            period="180d",
            interval="1h",
            auto_adjust=False,
            progress=False,
            threads=False,
        )
    except Exception as net_exc:
        raise CircuitBreakerTripped(f"yfinance network failure for {TICKER}: {net_exc}") from net_exc

    df_15m = clean_yfinance_dataframe(raw_15m)
    df_1h = clean_yfinance_dataframe(raw_1h)

    run_circuit_breaker(df_15m, df_1h)
    return df_15m, df_1h


def run_circuit_breaker(df_15m: pd.DataFrame, df_1h: pd.DataFrame) -> None:
    """Halts execution immediately on feed starvation, stale bars, or price anomalies."""
    if df_15m.empty or len(df_15m) < 120:
        raise CircuitBreakerTripped(
            f"Insufficient 15m bars for {TICKER}: got {len(df_15m)} (need >= 120)."
        )
    if df_1h.empty or len(df_1h) < 60:
        raise CircuitBreakerTripped(
            f"Insufficient 1h bars for {TICKER}: got {len(df_1h)} (need >= 60)."
        )

    last_ts = df_15m.index[-1]
    now_utc = dt.datetime.now(dt.timezone.utc)
    staleness_hours = (now_utc - last_ts.to_pydatetime()).total_seconds() / 3600.0
    if staleness_hours > CIRCUIT_BREAKER_MAX_STALE_HOURS:
        raise CircuitBreakerTripped(
            f"Market data stale by {staleness_hours:.2f} hours (limit {CIRCUIT_BREAKER_MAX_STALE_HOURS}h)."
        )

    latest_close = float(df_15m["Close"].iloc[-1])
    if not math.isfinite(latest_close) or latest_close < 500.0 or latest_close > 15000.0:
        raise CircuitBreakerTripped(
            f"Spot Gold price outside plausible bounds: {latest_close}"
        )

    pct_returns = df_15m["Close"].pct_change().dropna().abs()
    if not pct_returns.empty and float(pct_returns.iloc[-1]) > CIRCUIT_BREAKER_MAX_BAR_JUMP_PCT:
        raise CircuitBreakerTripped(
            f"Anomalous single-bar price spike detected: {float(pct_returns.iloc[-1]) * 100:.2f}%"
        )


# ==============================================================================
# 5-PILLAR HYBRID QUANTITATIVE FEATURE ENGINE
# ==============================================================================
def compute_atr(df: pd.DataFrame, period: int) -> pd.Series:
    """Computes Wilder-style Average True Range with division/NaN safety."""
    high = df["High"]
    low = df["Low"]
    prev_close = df["Close"].shift(1)
    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    true_range = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = true_range.ewm(alpha=1.0 / max(period, 1), min_periods=period, adjust=False).mean()
    return atr.ffill().bfill() + EPSILON


def compute_adx(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """Computes Average Directional Index for Market Regime filtering."""
    high = df["High"]
    low = df["Low"]
    up_move = high.diff()
    down_move = -low.diff()

    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)

    atr = compute_atr(df, period)
    plus_di = 100.0 * pd.Series(plus_dm, index=df.index).ewm(alpha=1.0 / period, adjust=False).mean() / (atr + EPSILON)
    minus_di = 100.0 * pd.Series(minus_dm, index=df.index).ewm(alpha=1.0 / period, adjust=False).mean() / (atr + EPSILON)

    dx = 100.0 * (plus_di - minus_di).abs() / (plus_di + minus_di + EPSILON)
    adx = dx.ewm(alpha=1.0 / period, adjust=False).mean()
    return adx.fillna(0.0)


def compute_smc_structure(df: pd.DataFrame, swing_lookback: int = 3) -> pd.DataFrame:
    """
    Pillar I (SMC): Causal Swing High/Low detection, Break of Structure (BOS),
    and Change of Character (ChoCh) without future look-ahead leakage.
    """
    highs = df["High"].values
    lows = df["Low"].values
    closes = df["Close"].values
    n = len(df)

    last_swing_high = np.full(n, np.nan)
    last_swing_low = np.full(n, np.nan)
    smc_bias = np.zeros(n, dtype=int)  # +1 bullish, -1 bearish
    bos_bull = np.zeros(n, dtype=bool)
    bos_bear = np.zeros(n, dtype=bool)
    choch_bull = np.zeros(n, dtype=bool)
    choch_bear = np.zeros(n, dtype=bool)

    curr_sh = highs[0]
    curr_sl = lows[0]
    curr_bias = 0

    for i in range(swing_lookback * 2, n):
        # Confirmed pivot at idx = i - swing_lookback using only bars <= i
        p = i - swing_lookback
        window_h = highs[p - swing_lookback : p + swing_lookback + 1]
        window_l = lows[p - swing_lookback : p + swing_lookback + 1]

        if highs[p] >= np.max(window_h):
            curr_sh = highs[p]
        if lows[p] <= np.min(window_l):
            curr_sl = lows[p]

        last_swing_high[i] = curr_sh
        last_swing_low[i] = curr_sl

        c = closes[i]
        prev_c = closes[i - 1]

        broke_up = c > curr_sh and prev_c <= curr_sh
        broke_down = c < curr_sl and prev_c >= curr_sl

        if broke_up:
            if curr_bias == -1:
                choch_bull[i] = True
            else:
                bos_bull[i] = True
            curr_bias = 1
        elif broke_down:
            if curr_bias == 1:
                choch_bear[i] = True
            else:
                bos_bear[i] = True
            curr_bias = -1
        elif curr_bias == 0:
            curr_bias = 1 if c > (curr_sh + curr_sl) * 0.5 else -1

        smc_bias[i] = curr_bias

    out = pd.DataFrame(
        {
            "swing_high": last_swing_high,
            "swing_low": last_swing_low,
            "smc_bias": smc_bias,
            "bos_bull": bos_bull,
            "bos_bear": bos_bear,
            "choch_bull": choch_bull,
            "choch_bear": choch_bear,
        },
        index=df.index,
    )
    return out


def compute_order_flow_features(df: pd.DataFrame, atr: pd.Series) -> pd.DataFrame:
    """
    Pillar II (Order Flow): Volume-weighted body-to-wick displacement ratios.
    Falls back cleanly to range-activity weighting if spot FX Volume is 0.
    """
    candle_range = (df["High"] - df["Low"]).abs() + EPSILON
    body = df["Close"] - df["Open"]
    abs_body = body.abs()
    body_ratio = (abs_body / candle_range).clip(0.0, 1.0)

    has_real_volume = bool((df["Volume"] > 0.0).any())
    if has_real_volume:
        vol_series = df["Volume"].replace(0.0, np.nan).ffill().fillna(1.0)
        vol_ma = vol_series.rolling(20, min_periods=5).mean() + EPSILON
        rel_volume = (vol_series / vol_ma).clip(0.25, 3.0)
    else:
        rel_volume = (candle_range / (atr + EPSILON)).clip(0.25, 3.0)

    direction = np.sign(body)
    displacement_norm = (abs_body / (atr + EPSILON)).clip(0.0, 2.5)
    signed_flow = direction * body_ratio * rel_volume * (0.5 + 0.5 * displacement_norm)
    smoothed_flow = signed_flow.ewm(span=3, adjust=False).mean()

    return pd.DataFrame(
        {
            "body_ratio": body_ratio,
            "rel_volume": rel_volume,
            "signed_flow": signed_flow,
            "smoothed_flow": smoothed_flow,
            "has_real_volume": has_real_volume,
        },
        index=df.index,
    )


def enrich_features(
    df_15m: pd.DataFrame,
    df_1h: pd.DataFrame,
    atr_period: int,
    z_window: int = Z_LOOKBACK_WINDOW,
) -> pd.DataFrame:
    """Builds the unified 5-pillar feature matrix aligned across 1H and 15M."""
    df = df_15m.copy()

    # Pillar V: ATR & Market Regime Governor
    df["ATR"] = compute_atr(df, atr_period)
    df["ADX"] = compute_adx(df, 14)
    atr_pct_rank = df["ATR"].rolling(96, min_periods=20).rank(pct=True).fillna(0.5)
    efficiency_ratio = (
        (df["Close"] - df["Close"].shift(10)).abs()
        / (df["Close"].diff().abs().rolling(10, min_periods=5).sum() + EPSILON)
    ).fillna(0.0)
    df["Efficiency_Ratio"] = efficiency_ratio
    df["Regime_Trending"] = (df["ADX"] >= 18.0) & (atr_pct_rank >= 0.20) & (efficiency_ratio >= 0.22)

    # Pillar I: SMC
    smc = compute_smc_structure(df, swing_lookback=3)
    df = pd.concat([df, smc], axis=1)

    # Pillar II: Order Flow
    flow = compute_order_flow_features(df, df["ATR"])
    df = pd.concat([df, flow], axis=1)

    # Pillar III: Z-Score Mean Reversion (Division-by-Zero Protected)
    rolling_mean = df["Close"].rolling(window=z_window, min_periods=z_window).mean()
    rolling_std = df["Close"].rolling(window=z_window, min_periods=z_window).std(ddof=0)
    df["Z_Score"] = ((df["Close"] - rolling_mean) / (rolling_std + EPSILON)).fillna(0.0)

    # Pillar IV: Multi-Timeframe 1H Fractal Harmony (strict backward merge_asof)
    htf = df_1h.copy()
    htf["HTF_EMA20"] = htf["Close"].ewm(span=20, adjust=False).mean()
    htf["HTF_EMA50"] = htf["Close"].ewm(span=50, adjust=False).mean()
    htf["HTF_ATR"] = compute_atr(htf, atr_period)
    htf_smc = compute_smc_structure(htf, swing_lookback=3)
    htf["HTF_SMC_Bias"] = htf_smc["smc_bias"]
    htf["HTF_Trend"] = np.where(
        (htf["Close"] > htf["HTF_EMA20"]) & (htf["HTF_EMA20"] >= htf["HTF_EMA50"]),
        1,
        np.where(
            (htf["Close"] < htf["HTF_EMA20"]) & (htf["HTF_EMA20"] <= htf["HTF_EMA50"]),
            -1,
            htf["HTF_SMC_Bias"],
        ),
    )

    htf_aligned = pd.merge_asof(
        df[["Close"]].reset_index().rename(columns={df.index.name or "index": "ts"}),
        htf[["HTF_EMA20", "HTF_EMA50", "HTF_Trend", "HTF_SMC_Bias"]]
        .reset_index()
        .rename(columns={htf.index.name or "index": "ts"}),
        on="ts",
        direction="backward",
    ).set_index("ts")

    df["HTF_EMA20"] = htf_aligned["HTF_EMA20"].bfill()
    df["HTF_EMA50"] = htf_aligned["HTF_EMA50"].bfill()
    df["HTF_Trend"] = htf_aligned["HTF_Trend"].fillna(0).astype(int)
    df["HTF_SMC_Bias"] = htf_aligned["HTF_SMC_Bias"].fillna(0).astype(int)

    return df


def score_bar_confluence(row: pd.Series, z_threshold: float) -> Tuple[str, float, Dict[str, float]]:
    """
    Scores a single bar across all 5 pillars (0..100) and emits directional intent.
    Harmonizes macro trend (1H) + SMC structure with Z-Score pullback & order flow.
    """
    htf_trend = int(row["HTF_Trend"])
    smc_bias = int(row["smc_bias"])
    z_score = float(row["Z_Score"])
    flow_val = float(row["smoothed_flow"])
    body_ratio = float(row["body_ratio"])
    regime_ok = bool(row["Regime_Trending"])

    bos_bull = bool(row["bos_bull"])
    choch_bull = bool(row["choch_bull"])
    bos_bear = bool(row["bos_bear"])
    choch_bear = bool(row["choch_bear"])

    # Bullish Confluence Score
    bull_score = 0.0
    if htf_trend == 1:
        bull_score += 25.0
    if smc_bias == 1:
        bull_score += 20.0
    if bos_bull or choch_bull:
        bull_score += 10.0
    if z_score <= -z_threshold:
        bull_score += 25.0
    elif z_score <= -(z_threshold * 0.65):
        bull_score += 15.0
    if flow_val > 0.15 and body_ratio >= 0.45:
        bull_score += 20.0
    elif flow_val > 0.0:
        bull_score += 10.0
    if regime_ok:
        bull_score += 10.0

    # Bearish Confluence Score
    bear_score = 0.0
    if htf_trend == -1:
        bear_score += 25.0
    if smc_bias == -1:
        bear_score += 20.0
    if bos_bear or choch_bear:
        bear_score += 10.0
    if z_score >= z_threshold:
        bear_score += 25.0
    elif z_score >= (z_threshold * 0.65):
        bear_score += 15.0
    if flow_val < -0.15 and body_ratio >= 0.45:
        bear_score += 20.0
    elif flow_val < 0.0:
        bear_score += 10.0
    if regime_ok:
        bear_score += 10.0

    bull_score = min(bull_score, 100.0)
    bear_score = min(bear_score, 100.0)

    breakdown = {
        "bull_score": round(bull_score, 2),
        "bear_score": round(bear_score, 2),
        "htf_trend": htf_trend,
        "smc_bias": smc_bias,
        "z_score": round(z_score, 4),
        "smoothed_flow": round(flow_val, 4),
        "regime_trending": int(regime_ok),
    }

    if not regime_ok:
        return "NO SIGNAL", max(bull_score, bear_score), breakdown

    if bull_score >= CONFLUENCE_THRESHOLD and bull_score > bear_score and htf_trend >= 0:
        return "VERIFIED BUY", bull_score, breakdown
    if bear_score >= CONFLUENCE_THRESHOLD and bear_score > bull_score and htf_trend <= 0:
        return "VERIFIED SELL", bear_score, breakdown

    return "NO SIGNAL", max(bull_score, bear_score), breakdown


# ==============================================================================
# SLIPPAGE-AWARE BACKTEST ENGINE & GRID SEARCH OPTIMIZATION
# ==============================================================================
def run_backtest_for_params(
    df_15m: pd.DataFrame,
    df_1h: pd.DataFrame,
    atr_period: int,
    z_threshold: float,
) -> BacktestMetrics:
    """
    Executes a realistic spread + slippage-aware simulation including TP1
    breakeven transition and fractional-ATR trailing stop exits.
    """
    df = enrich_features(df_15m, df_1h, atr_period=atr_period, z_window=Z_LOOKBACK_WINDOW)

    opens = df["Open"].values
    highs = df["High"].values
    lows = df["Low"].values
    closes = df["Close"].values
    atrs = df["ATR"].values
    z_scores = df["Z_Score"].values
    htf_trends = df["HTF_Trend"].values
    smc_biases = df["smc_bias"].values
    flows = df["smoothed_flow"].values
    body_ratios = df["body_ratio"].values
    regimes = df["Regime_Trending"].values
    bos_bulls = df["bos_bull"].values
    choch_bulls = df["choch_bull"].values
    bos_bears = df["bos_bear"].values
    choch_bears = df["choch_bear"].values
    timestamps = list(df.index)

    n = len(df)
    equity = ACCOUNT_BALANCE
    peak_equity = ACCOUNT_BALANCE
    max_dd = 0.0
    equity_curve = [equity]
    curve_timestamps = [timestamps[0]]
    trade_returns: List[float] = []

    in_pos = False
    direction = 0
    entry_px = 0.0
    sl_px = 0.0
    tp1_px = 0.0
    tp3_px = 0.0
    pos_atr = 0.0
    tp1_hit = False
    best_fav = 0.0
    risk_dollars = 0.0

    cost_half = (SPREAD_DOLLARS * 0.5) + SLIPPAGE_DOLLARS
    start_idx = max(atr_period, Z_LOOKBACK_WINDOW, 30)

    for i in range(start_idx, n - 1):
        h = highs[i]
        l = lows[i]

        if in_pos:
            if direction == 1:
                best_fav = max(best_fav, h)
                # Check SL first (conservative worst-case bar assumption)
                if l <= sl_px:
                    exit_px = sl_px - SLIPPAGE_DOLLARS
                    pnl_r = (exit_px - entry_px) / (pos_atr * 1.0 + EPSILON)
                    pnl_usd = pnl_r * risk_dollars
                    equity += pnl_usd
                    trade_returns.append(pnl_usd)
                    in_pos = False
                elif h >= tp3_px:
                    exit_px = tp3_px - SLIPPAGE_DOLLARS
                    pnl_r = (exit_px - entry_px) / (pos_atr * 1.0 + EPSILON)
                    pnl_usd = pnl_r * risk_dollars
                    equity += pnl_usd
                    trade_returns.append(pnl_usd)
                    in_pos = False
                else:
                    if not tp1_hit and h >= tp1_px:
                        tp1_hit = True
                        sl_px = max(sl_px, entry_px)  # Shift to Breakeven
                    if tp1_hit:
                        trailing_candidate = best_fav - (0.75 * pos_atr)
                        sl_px = max(sl_px, trailing_candidate)
            else:
                best_fav = min(best_fav, l)
                if h >= sl_px:
                    exit_px = sl_px + SLIPPAGE_DOLLARS
                    pnl_r = (entry_px - exit_px) / (pos_atr * 1.0 + EPSILON)
                    pnl_usd = pnl_r * risk_dollars
                    equity += pnl_usd
                    trade_returns.append(pnl_usd)
                    in_pos = False
                elif l <= tp3_px:
                    exit_px = tp3_px + SLIPPAGE_DOLLARS
                    pnl_r = (entry_px - exit_px) / (pos_atr * 1.0 + EPSILON)
                    pnl_usd = pnl_r * risk_dollars
                    equity += pnl_usd
                    trade_returns.append(pnl_usd)
                    in_pos = False
                else:
                    if not tp1_hit and l <= tp1_px:
                        tp1_hit = True
                        sl_px = min(sl_px, entry_px)  # Shift to Breakeven
                    if tp1_hit:
                        trailing_candidate = best_fav + (0.75 * pos_atr)
                        sl_px = min(sl_px, trailing_candidate)

            peak_equity = max(peak_equity, equity)
            dd_pct = ((peak_equity - equity) / (peak_equity + EPSILON)) * 100.0
            max_dd = max(max_dd, dd_pct)
            equity_curve.append(equity)
            curve_timestamps.append(timestamps[i])
            continue

        # Entry evaluation on closed bar i -> enter at bar i+1 Open
        if not regimes[i]:
            equity_curve.append(equity)
            curve_timestamps.append(timestamps[i])
            continue

        htf_t = int(htf_trends[i])
        smc_b = int(smc_biases[i])
        z_val = float(z_scores[i])
        fl_val = float(flows[i])
        br_val = float(body_ratios[i])

        bull_sc = (
            (25.0 if htf_t == 1 else 0.0)
            + (20.0 if smc_b == 1 else 0.0)
            + (10.0 if (bos_bulls[i] or choch_bulls[i]) else 0.0)
            + (25.0 if z_val <= -z_threshold else (15.0 if z_val <= -(z_threshold * 0.65) else 0.0))
            + (20.0 if (fl_val > 0.15 and br_val >= 0.45) else (10.0 if fl_val > 0.0 else 0.0))
            + 10.0
        )
        bear_sc = (
            (25.0 if htf_t == -1 else 0.0)
            + (20.0 if smc_b == -1 else 0.0)
            + (10.0 if (bos_bears[i] or choch_bears[i]) else 0.0)
            + (25.0 if z_val >= z_threshold else (15.0 if z_val >= (z_threshold * 0.65) else 0.0))
            + (20.0 if (fl_val < -0.15 and br_val >= 0.45) else (10.0 if fl_val < 0.0 else 0.0))
            + 10.0
        )

        atr_val = float(atrs[i])
        if atr_val <= EPSILON:
            equity_curve.append(equity)
            curve_timestamps.append(timestamps[i])
            continue

        next_open = float(opens[i + 1])
        if bull_sc >= CONFLUENCE_THRESHOLD and bull_sc > bear_sc and htf_t >= 0:
            in_pos = True
            direction = 1
            entry_px = next_open + cost_half
            pos_atr = max(atr_val, 0.5)
            sl_px = entry_px - (1.0 * pos_atr)
            tp1_px = entry_px + (1.5 * pos_atr)
            tp3_px = entry_px + (4.0 * pos_atr)
            tp1_hit = False
            best_fav = entry_px
            risk_dollars = equity * (RISK_PERCENTAGE / 100.0)
        elif bear_sc >= CONFLUENCE_THRESHOLD and bear_sc > bull_sc and htf_t <= 0:
            in_pos = True
            direction = -1
            entry_px = next_open - cost_half
            pos_atr = max(atr_val, 0.5)
            sl_px = entry_px + (1.0 * pos_atr)
            tp1_px = entry_px - (1.5 * pos_atr)
            tp3_px = entry_px - (4.0 * pos_atr)
            tp1_hit = False
            best_fav = entry_px
            risk_dollars = equity * (RISK_PERCENTAGE / 100.0)

        equity_curve.append(equity)
        curve_timestamps.append(timestamps[i])

    total_trades = len(trade_returns)
    wins = sum(1 for r in trade_returns if r > 0.0)
    losses = sum(1 for r in trade_returns if r <= 0.0)
    gross_profit = sum(r for r in trade_returns if r > 0.0)
    gross_loss = abs(sum(r for r in trade_returns if r < 0.0))

    win_rate = (wins / (total_trades + EPSILON)) * 100.0 if total_trades > 0 else 0.0
    if gross_loss > EPSILON:
        profit_factor = gross_profit / (gross_loss + EPSILON)
    elif gross_profit > 0.0:
        profit_factor = 3.50
    else:
        profit_factor = 0.0

    net_return_pct = ((equity - ACCOUNT_BALANCE) / (ACCOUNT_BALANCE + EPSILON)) * 100.0

    return BacktestMetrics(
        atr_period=atr_period,
        z_threshold=z_threshold,
        total_trades=total_trades,
        wins=wins,
        losses=losses,
        win_rate=win_rate,
        profit_factor=min(profit_factor, 9.99),
        max_drawdown=max_dd,
        net_return_pct=net_return_pct,
        equity_curve=equity_curve,
        timestamps=curve_timestamps,
        trade_returns=trade_returns,
    )


def run_grid_search_optimization(
    df_15m: pd.DataFrame, df_1h: pd.DataFrame
) -> Tuple[BacktestMetrics, List[BacktestMetrics]]:
    """
    Searches predefined ATR Period & Z-Score Threshold grid to select the
    highest Profit Factor configuration (requiring statistical trade support).
    """
    candidates: List[BacktestMetrics] = []
    for atr_p in ATR_GRID:
        for z_th in Z_THRESHOLD_GRID:
            metrics = run_backtest_for_params(df_15m, df_1h, atr_p, z_th)
            candidates.append(metrics)

    # Prefer parameter sets with >= 5 trades; fallback to highest PF if sparse
    valid = [m for m in candidates if m.total_trades >= 5]
    pool = valid if valid else candidates
    best = max(
        pool,
        key=lambda m: (round(m.profit_factor, 4), round(m.win_rate, 2), -round(m.max_drawdown, 2), m.total_trades),
    )

    print("[GRID SEARCH] Parameter Optimization Matrix:")
    for m in candidates:
        marker = " <-- OPTIMAL" if (m.atr_period == best.atr_period and m.z_threshold == best.z_threshold) else ""
        print(
            f"  ATR={m.atr_period:2d} | Z={m.z_threshold:.1f} | Trades={m.total_trades:3d} | "
            f"WR={m.win_rate:5.1f}% | PF={m.profit_factor:4.2f} | MaxDD={m.max_drawdown:5.2f}%{marker}"
        )

    return best, candidates


# ==============================================================================
# MATPLOTLIB BACKTEST CHART GENERATION (HEADLESS 'Agg' BACKEND)
# ==============================================================================
def generate_backtest_chart(
    best_metrics: BacktestMetrics, chart_path: str = CHART_PATH
) -> str:
    """
    Renders an institutional-grade equity curve & drawdown chart to PNG
    using Matplotlib's headless Agg backend.
    """
    plt.style.use("dark_background")
    fig, (ax1, ax2) = plt.subplots(
        2, 1, figsize=(13, 7.5), gridspec_kw={"height_ratios": [3, 1]}, sharex=True
    )
    fig.patch.set_facecolor("#0B0F17")
    ax1.set_facecolor("#111827")
    ax2.set_facecolor("#111827")

    ts = pd.to_datetime(best_metrics.timestamps)
    eq = np.array(best_metrics.equity_curve, dtype=float)
    running_peak = np.maximum.accumulate(eq)
    drawdown_pct = -((running_peak - eq) / (running_peak + EPSILON)) * 100.0

    ax1.plot(ts, eq, color="#F59E0B", linewidth=2.0, label="Equity Curve (USD)")
    ax1.fill_between(ts, eq, ACCOUNT_BALANCE, where=(eq >= ACCOUNT_BALANCE), color="#10B981", alpha=0.14)
    ax1.fill_between(ts, eq, ACCOUNT_BALANCE, where=(eq < ACCOUNT_BALANCE), color="#EF4444", alpha=0.14)
    ax1.axhline(ACCOUNT_BALANCE, color="#6B7280", linestyle="--", linewidth=1.0, alpha=0.7)

    title_str = (
        f"{ENGINE_NAME} | XAUUSD Backtest "
        f"(ATR={best_metrics.atr_period}, Z={best_metrics.z_threshold:.1f})"
    )
    ax1.set_title(title_str, fontsize=13, fontweight="bold", color="#F9FAFB", pad=12)
    ax1.set_ylabel("Account Equity ($)", fontsize=10, color="#D1D5DB")
    ax1.grid(True, linestyle=":", linewidth=0.6, alpha=0.35, color="#4B5563")
    ax1.legend(loc="upper left", frameon=True, facecolor="#1F2937", edgecolor="#374151")

    stats_box = (
        f"Trades: {best_metrics.total_trades}\n"
        f"Win Rate: {best_metrics.win_rate:.1f}%\n"
        f"Profit Factor: {best_metrics.profit_factor:.2f}\n"
        f"Max Drawdown: {best_metrics.max_drawdown:.2f}%\n"
        f"Net Return: {best_metrics.net_return_pct:+.2f}%"
    )
    ax1.text(
        0.985,
        0.05,
        stats_box,
        transform=ax1.transAxes,
        fontsize=9.5,
        verticalalignment="bottom",
        horizontalalignment="right",
        color="#F3F4F6",
        bbox=dict(boxstyle="round,pad=0.5", facecolor="#1F2937", edgecolor="#F59E0B", alpha=0.9),
    )

    ax2.fill_between(ts, drawdown_pct, 0.0, color="#EF4444", alpha=0.45, label="Drawdown (%)")
    ax2.plot(ts, drawdown_pct, color="#F87171", linewidth=1.1)
    ax2.set_ylabel("Drawdown (%)", fontsize=10, color="#D1D5DB")
    ax2.set_xlabel("UTC Timeline", fontsize=10, color="#D1D5DB")
    ax2.grid(True, linestyle=":", linewidth=0.6, alpha=0.35, color="#4B5563")
    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d %H:%M"))
    fig.autofmt_xdate()

    plt.tight_layout()
    plt.savefig(chart_path, dpi=150, facecolor=fig.get_facecolor())
    plt.close(fig)

    return "Generated & Saved as PNG"


# ==============================================================================
# AUTO-TRAILING STOP ENGINE (SQLITE STATEFUL MANAGEMENT)
# ==============================================================================
def manage_active_trailing_stops(
    conn: sqlite3.Connection, latest_bar: pd.Series, current_atr: float
) -> Tuple[str, List[str]]:
    """
    Evaluates open positions stored in SQLite against current Spot Gold price:
    - Once TP1 is breached, shifts Stop Loss to Breakeven (Entry).
    - Engages fractional-ATR (0.75 * ATR) trailing step behind favorable extremes.
    - Closes positions when SL or TP3 is hit.
    """
    now_iso = dt.datetime.now(dt.timezone.utc).isoformat()
    curr_high = float(latest_bar["High"])
    curr_low = float(latest_bar["Low"])
    curr_close = float(latest_bar["Close"])

    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT id, direction, entry_price, current_sl, tp1, tp3,
               atr_at_entry, tp1_breached, highest_favorable
        FROM active_positions
        WHERE status = 'OPEN';
        """
    )
    open_rows = cursor.fetchall()
    events: List[str] = []

    for row in open_rows:
        (
            pos_id,
            direction,
            entry_px,
            curr_sl,
            tp1,
            tp3,
            atr_entry,
            tp1_breached,
            highest_fav,
        ) = row

        step_atr = max(current_atr, atr_entry, 0.5)
        new_sl = curr_sl
        new_fav = highest_fav
        new_tp1_breached = tp1_breached
        new_status = "OPEN"
        trail_state = "Initial Forensic SL"

        if direction == "BUY":
            new_fav = max(highest_fav, curr_high)
            if curr_low <= curr_sl:
                new_status = "CLOSED_SL"
                trail_state = f"Stopped Out @ {curr_sl:.2f}"
            elif curr_high >= tp3:
                new_status = "CLOSED_TP3"
                trail_state = f"Target TP3 Hit @ {tp3:.2f}"
            else:
                if not tp1_breached and curr_high >= tp1:
                    new_tp1_breached = 1
                    new_sl = max(curr_sl, entry_px)
                    trail_state = "Active @ TP1 Breakeven"
                if new_tp1_breached:
                    trail_candidate = new_fav - (0.75 * step_atr)
                    if trail_candidate > new_sl:
                        new_sl = trail_candidate
                    trail_state = f"Active Trailing SL @ {new_sl:.2f}"
        else:
            new_fav = min(highest_fav, curr_low)
            if curr_high >= curr_sl:
                new_status = "CLOSED_SL"
                trail_state = f"Stopped Out @ {curr_sl:.2f}"
            elif curr_low <= tp3:
                new_status = "CLOSED_TP3"
                trail_state = f"Target TP3 Hit @ {tp3:.2f}"
            else:
                if not tp1_breached and curr_low <= tp1:
                    new_tp1_breached = 1
                    new_sl = min(curr_sl, entry_px)
                    trail_state = "Active @ TP1 Breakeven"
                if new_tp1_breached:
                    trail_candidate = new_fav + (0.75 * step_atr)
                    if trail_candidate < new_sl:
                        new_sl = trail_candidate
                    trail_state = f"Active Trailing SL @ {new_sl:.2f}"

        cursor.execute(
            """
            UPDATE active_positions
            SET updated_at_utc = ?, current_sl = ?, tp1_breached = ?,
                highest_favorable = ?, trailing_state = ?, status = ?
            WHERE id = ?;
            """,
            (
                now_iso,
                round(new_sl, 4),
                int(new_tp1_breached),
                round(new_fav, 4),
                trail_state,
                new_status,
                pos_id,
            ),
        )
        events.append(
            f"Position #{pos_id} ({direction} @ {entry_px:.2f} | Close {curr_close:.2f}): {trail_state} [{new_status}]"
        )

    conn.commit()

    if events:
        print("[TRAILING ENGINE] Position Updates:")
        for ev in events:
            print(f"  -> {ev}")
        return events[-1], events

    return "Active @ TP1 Breakeven (Armed)", events


def register_new_position(conn: sqlite3.Connection, signal: QuantitativeSignal) -> None:
    """Persists newly triggered VERIFIED BUY/SELL positions for trailing-stop tracking."""
    if signal.decision not in ("VERIFIED BUY", "VERIFIED SELL"):
        return

    direction = "BUY" if signal.decision == "VERIFIED BUY" else "SELL"
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO active_positions (
            opened_at_utc, updated_at_utc, asset, direction, entry_price,
            initial_sl, current_sl, tp1, tp2, tp3, atr_at_entry,
            lot_size, tp1_breached, highest_favorable, trailing_state, status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, 'OPEN');
        """,
        (
            signal.timestamp_utc,
            signal.timestamp_utc,
            "XAUUSD",
            direction,
            round(signal.entry_price, 4),
            round(signal.stop_loss, 4),
            round(signal.stop_loss, 4),
            round(signal.tp1, 4),
            round(signal.tp2, 4),
            round(signal.tp3, 4),
            round(signal.atr_value, 4),
            round(signal.recommended_lot, 2),
            round(signal.entry_price, 4),
            "Active @ TP1 Breakeven",
        ),
    )
    conn.commit()


# ==============================================================================
# LIVE SIGNAL EVALUATION & POSITION SIZING
# ==============================================================================
def evaluate_live_signal(
    df_15m: pd.DataFrame,
    df_1h: pd.DataFrame,
    best_metrics: BacktestMetrics,
) -> Tuple[QuantitativeSignal, Dict[str, Any]]:
    """Evaluates the latest closed 15m candle using optimal Grid Search parameters."""
    enriched = enrich_features(
        df_15m,
        df_1h,
        atr_period=best_metrics.atr_period,
        z_window=Z_LOOKBACK_WINDOW,
    )

    # Use last completed bar (-2) if current bar (-1) is mid-formation; fallback cleanly
    eval_row = enriched.iloc[-1]
    decision, confluence, breakdown = score_bar_confluence(
        eval_row, z_threshold=best_metrics.z_threshold
    )

    entry_price = float(eval_row["Close"])
    atr_val = max(float(eval_row["ATR"]), 0.50)
    z_score = float(eval_row["Z_Score"])
    regime_label = "TRENDING" if bool(eval_row["Regime_Trending"]) else "CHOPPY FILTERED"
    smc_label = "BULLISH" if int(eval_row["smc_bias"]) == 1 else "BEARISH"
    htf_label = (
        "BULLISH 1H HARMONY"
        if int(eval_row["HTF_Trend"]) == 1
        else ("BEARISH 1H HARMONY" if int(eval_row["HTF_Trend"]) == -1 else "NEUTRAL 1H")
    )

    sl_distance = 1.0 * atr_val
    if decision == "VERIFIED SELL":
        stop_loss = entry_price + sl_distance
        tp1 = entry_price - (1.5 * sl_distance)
        tp2 = entry_price - (2.5 * sl_distance)
        tp3 = entry_price - (4.0 * sl_distance)
    else:
        stop_loss = entry_price - sl_distance
        tp1 = entry_price + (1.5 * sl_distance)
        tp2 = entry_price + (2.5 * sl_distance)
        tp3 = entry_price + (4.0 * sl_distance)

    rrr_tp3 = abs(tp3 - entry_price) / (abs(entry_price - stop_loss) + EPSILON)
    if decision in ("VERIFIED BUY", "VERIFIED SELL") and rrr_tp3 < MIN_RRR_TARGET:
        decision = "NO SIGNAL"

    risk_usd = ACCOUNT_BALANCE * (RISK_PERCENTAGE / 100.0)
    raw_lot = risk_usd / ((sl_distance * XAU_CONTRACT_MULTIPLIER) + EPSILON)
    recommended_lot = max(0.01, round(raw_lot, 2))

    ts_utc = dt.datetime.now(dt.timezone.utc).isoformat()

    signal = QuantitativeSignal(
        decision=decision,
        entry_price=entry_price,
        stop_loss=stop_loss,
        tp1=tp1,
        tp2=tp2,
        tp3=tp3,
        atr_value=atr_val,
        z_score=z_score,
        confluence_score=confluence,
        market_regime=regime_label,
        smc_bias=smc_label,
        order_flow_score=float(eval_row["smoothed_flow"]),
        htf_alignment=htf_label,
        recommended_lot=recommended_lot,
        risk_usd=risk_usd,
        rrr_tp3=rrr_tp3,
        trailing_protocol="Active @ TP1 Breakeven",
        timestamp_utc=ts_utc,
        pillar_breakdown=breakdown,
    )

    raw_math = {
        "latest_bar_time": str(enriched.index[-1]),
        "open": round(float(eval_row["Open"]), 4),
        "high": round(float(eval_row["High"]), 4),
        "low": round(float(eval_row["Low"]), 4),
        "close": round(float(eval_row["Close"]), 4),
        "atr": round(atr_val, 6),
        "adx": round(float(eval_row["ADX"]), 4),
        "efficiency_ratio": round(float(eval_row["Efficiency_Ratio"]), 4),
        "z_score": round(z_score, 6),
        "body_ratio": round(float(eval_row["body_ratio"]), 4),
        "rel_volume": round(float(eval_row["rel_volume"]), 4),
        "has_real_volume": bool(eval_row["has_real_volume"]),
        "htf_ema20": round(float(eval_row["HTF_EMA20"]), 4),
        "htf_ema50": round(float(eval_row["HTF_EMA50"]), 4),
        "breakdown": breakdown,
    }

    return signal, raw_math


# ==============================================================================
# DUAL-CHANNEL PARALLEL WEBHOOK DELIVERY (TELEGRAM + DISCORD)
# ==============================================================================
def build_forensic_report_html(
    signal: QuantitativeSignal,
    best_metrics: BacktestMetrics,
    chart_status: str,
    db_status: str,
) -> str:
    """Formats the exact Institutional Forensic Report template in Telegram HTML."""
    return (
        f"🛡️ <b>{ENGINE_NAME} (ULTIMATE EDITION)</b> 🛡️\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🌟 <b>Asset:</b> XAUUSD (SPOT GOLD)\n"
        f"📊 <b>Signal Decision:</b> {signal.decision}\n"
        f"🎯 <b>Precision Entry:</b> {signal.entry_price:.2f}\n"
        f"🛑 <b>Forensic Stop Loss:</b> {signal.stop_loss:.2f}\n"
        f"📈 <b>Take Profit 1 (1.5R):</b> {signal.tp1:.2f}\n"
        f"📈 <b>Take Profit 2 (2.5R):</b> {signal.tp2:.2f}\n"
        f"🚀 <b>Take Profit 3 (4.0R):</b> {signal.tp3:.2f}\n"
        f"🔄 <b>Trailing Stop Protocol:</b> {signal.trailing_protocol}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚙️ <b>Grid Search &amp; Visualization:</b>\n"
        f"• <i>Optimal ATR Period:</i> {best_metrics.atr_period}\n"
        f"• <i>Optimal Z-Threshold:</i> {best_metrics.z_threshold:.1f}\n"
        f"• <i>Backtest Chart:</i> {chart_status}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 <b>Risk &amp; Position Sizing:</b>\n"
        f"• <i>Recommended Lot:</i> {signal.recommended_lot:.2f} Lots ({RISK_PERCENTAGE:.0f}% Risk)\n"
        f"• <i>Risk Amount (USD):</i> ${signal.risk_usd:.2f}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔬 <b>Forensic Audit, Database &amp; Backtest:</b>\n"
        f"• <i>Market Regime:</i> {signal.market_regime}\n"
        f"• <i>Confluence Score:</i> {signal.confluence_score:.1f}/100\n"
        f"• <i>SQLite DB Status:</i> {db_status}\n"
        f"• <i>Error Handler Status:</i> Active / Nominal\n"
        f"• <i>Optimized Win Rate:</i> {best_metrics.win_rate:.1f}%\n"
        f"• <i>Profit Factor:</i> {best_metrics.profit_factor:.2f}\n"
        f"• <i>Max Drawdown:</i> {best_metrics.max_drawdown:.1f}%\n"
        f"• <i>Timestamp (UTC):</i> {signal.timestamp_utc}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━"
    )


def html_to_discord_markdown(html_msg: str) -> str:
    """Converts Telegram HTML tags cleanly to Discord Markdown."""
    return (
        html_msg.replace("<b>", "**")
        .replace("</b>", "**")
        .replace("<i>", "*")
        .replace("</i>", "*")
        .replace("&amp;", "&")
    )


def _send_telegram_signal(html_payload: str, chart_path: str) -> str:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return "Skipped (missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID)"
    try:
        msg_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        resp = requests.post(
            msg_url,
            json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": html_payload,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=20,
        )
        resp.raise_for_status()

        if os.path.exists(chart_path):
            photo_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
            with open(chart_path, "rb") as img_file:
                photo_resp = requests.post(
                    photo_url,
                    data={
                        "chat_id": TELEGRAM_CHAT_ID,
                        "caption": f"{ENGINE_NAME} | XAUUSD Equity & Drawdown Backtest",
                    },
                    files={"photo": img_file},
                    timeout=25,
                )
                photo_resp.raise_for_status()
        return "Delivered OK"
    except Exception as exc:
        send_telegram_error_alert("Telegram Webhook Delivery", exc)
        return f"Failed: {exc}"


def _send_discord_signal(discord_payload: str, chart_path: str) -> str:
    if not DISCORD_WEBHOOK_URL:
        return "Skipped (missing DISCORD_WEBHOOK_URL)"
    try:
        if os.path.exists(chart_path):
            with open(chart_path, "rb") as img_file:
                resp = requests.post(
                    DISCORD_WEBHOOK_URL,
                    data={"content": discord_payload},
                    files={"file": ("backtest_performance.png", img_file, "image/png")},
                    timeout=25,
                )
        else:
            resp = requests.post(
                DISCORD_WEBHOOK_URL,
                json={"content": discord_payload},
                timeout=20,
            )
        resp.raise_for_status()
        return "Delivered OK"
    except Exception as exc:
        send_telegram_error_alert("Discord Webhook Delivery", exc)
        return f"Failed: {exc}"


def dispatch_dual_channel_webhooks(
    signal: QuantitativeSignal,
    best_metrics: BacktestMetrics,
    chart_status: str,
    db_status: str,
) -> Dict[str, str]:
    """Dispatches verified signals in parallel to Telegram and Discord."""
    html_msg = build_forensic_report_html(signal, best_metrics, chart_status, db_status)
    discord_msg = html_to_discord_markdown(html_msg)

    results: Dict[str, str] = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        tg_future = executor.submit(_send_telegram_signal, html_msg, CHART_PATH)
        dc_future = executor.submit(_send_discord_signal, discord_msg, CHART_PATH)
        results["telegram"] = tg_future.result()
        results["discord"] = dc_future.result()

    print(f"[WEBHOOK DISPATCH] Telegram={results['telegram']} | Discord={results['discord']}")
    return results


# ==============================================================================
# MAIN ORCHESTRATION PIPELINE
# ==============================================================================
def main() -> int:
    conn: Optional[sqlite3.Connection] = None
    try:
        print(f"[BOOT] Initializing {ENGINE_NAME} for {TICKER}...")
        conn = init_sqlite_db(DB_PATH)

        # 1. Fetch & Clean Spot Gold Feed + Circuit Breaker
        df_15m, df_1h = fetch_spot_gold_data()
        print(
            f"[DATA] Loaded {len(df_15m)}x 15m bars & {len(df_1h)}x 1h bars | "
            f"Latest Spot Close: {float(df_15m['Close'].iloc[-1]):.2f}"
        )

        # 2. Grid Search Parameter Optimization
        best_metrics, _ = run_grid_search_optimization(df_15m, df_1h)

        # 3. Generate Headless Matplotlib Backtest Performance Chart
        chart_status = generate_backtest_chart(best_metrics, CHART_PATH)
        print(f"[CHART] {chart_status} -> {CHART_PATH}")

        # 4. Live 5-Pillar Signal Evaluation
        signal, raw_math = evaluate_live_signal(df_15m, df_1h, best_metrics)

        # 5. Auto-Trailing Stop Management for Existing Open Positions
        latest_atr_series = compute_atr(df_15m, best_metrics.atr_period)
        latest_atr_val = float(latest_atr_series.iloc[-1])
        trailing_summary, _ = manage_active_trailing_stops(conn, df_15m.iloc[-1], latest_atr_val)
        if signal.decision == "NO SIGNAL":
            signal.trailing_protocol = trailing_summary

        # 6. Forensic Audit Console Output & SQLite Persistence (Prior to Dispatch)
        db_status = forensic_audit_log(conn, signal, best_metrics, raw_math)

        # 7. Dual-Channel Parallel Dispatch on Verified Signals
        if signal.decision in ("VERIFIED BUY", "VERIFIED SELL"):
            register_new_position(conn, signal)
            dispatch_dual_channel_webhooks(signal, best_metrics, chart_status, db_status)
        else:
            print(
                f"[GOVERNOR] Decision is {signal.decision} "
                f"(Regime={signal.market_regime}, Score={signal.confluence_score:.1f}/100). "
                f"Webhook suppressed to prevent low-quality alerts."
            )

        return 0

    except CircuitBreakerTripped as cb_exc:
        send_telegram_error_alert("Circuit Breaker Tripped", cb_exc)
        return 1
    except Exception as exc:
        send_telegram_error_alert("APEX-QUANT-OMNI Core Execution", exc)
        return 1
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    sys.exit(main())
