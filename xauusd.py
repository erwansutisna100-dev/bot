import matplotlib
matplotlib.use('Agg')  # Non-GUI backend strictly enforced for headless GitHub Actions runners

import os
import sys
import time
import sqlite3
import logging
import traceback
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pandas as pd
import requests
import yfinance as yf
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# ==============================================================================
# ENTERPRISE CONFIGURATION & RISK GOVERNANCE
# ==============================================================================
TICKER_PRIMARY = "XAUUSD=X"
TICKER_FALLBACK = "GC=F"  # Automatic failover if Yahoo spot feed experiences transient 404
DB_PATH = "trading_signals.db"
CHART_PATH = "backtest_performance.png"

ACCOUNT_BALANCE = float(os.getenv("ACCOUNT_BALANCE", "10000.0"))
RISK_PERCENTAGE = float(os.getenv("RISK_PERCENTAGE", "0.01"))  # 1.0% Risk per trade
CONTRACT_SIZE = 100.0  # Standard 1.00 Lot XAUUSD = 100 troy ounces
SPREAD_POINTS = 0.25   # $0.25 simulated broker spread
SLIPPAGE_POINTS = 0.15 # $0.15 simulated execution slippage

MIN_CONFLUENCE_SCORE = 65
EPSILON = 1e-8

# Grid Search Parameter Space
ATR_GRID = [10, 14, 20]
Z_THRESH_GRID = [1.5, 2.0, 2.5]

# Webhook Secrets
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("APEX-QUANT-OMNI")


# ==============================================================================
# TELEGRAM ERROR HANDLER & ALERT SYSTEM
# ==============================================================================
def dispatch_telegram_error(error_title: str, error_details: str) -> None:
    """Dispatches critical runtime or network anomalies directly to Telegram."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram credentials missing; skipping error alert dispatch.")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    timestamp_utc = datetime.now(timezone.utc).isoformat()
    safe_details = str(error_details)[:3200]

    payload_text = (
        f"🚨 <b>APEX-QUANT-OMNI v40.0 SYSTEM ALERT</b> 🚨\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚠️ <b>Fault Category:</b> {error_title}\n"
        f"🕒 <b>Timestamp (UTC):</b> {timestamp_utc}\n"
        f"📋 <b>Diagnostic Trace:</b>\n"
        f"<pre>{safe_details}</pre>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━"
    )

    try:
        resp = requests.post(
            url,
            json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": payload_text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True
            },
            timeout=15
        )
        resp.raise_for_status()
        logger.info("Critical error notification dispatched to Telegram.")
    except Exception as exc:
        logger.error(f"Failed to send Telegram error alert: {exc}")


# ==============================================================================
# SQLITE PERSISTENCE LAYER
# ==============================================================================
def init_sqlite_db(db_path: str = DB_PATH) -> None:
    """Initializes the SQLite database with tables for signals and position trailing states."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS signal_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp_utc TEXT NOT NULL,
            asset TEXT NOT NULL,
            signal_decision TEXT NOT NULL,
            entry_price REAL NOT NULL,
            stop_loss REAL NOT NULL,
            tp1 REAL NOT NULL,
            tp2 REAL NOT NULL,
            tp3 REAL NOT NULL,
            trailing_status TEXT NOT NULL,
            optimal_atr_period INTEGER NOT NULL,
            optimal_z_threshold REAL NOT NULL,
            recommended_lot REAL NOT NULL,
            risk_amount_usd REAL NOT NULL,
            market_regime TEXT NOT NULL,
            confluence_score INTEGER NOT NULL,
            smc_status TEXT NOT NULL,
            z_score REAL NOT NULL,
            atr_value REAL NOT NULL,
            order_flow_score REAL NOT NULL,
            win_rate REAL NOT NULL,
            profit_factor REAL NOT NULL,
            max_drawdown REAL NOT NULL
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS active_positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            opened_at_utc TEXT NOT NULL,
            direction TEXT NOT NULL,
            entry_price REAL NOT NULL,
            current_sl REAL NOT NULL,
            tp1 REAL NOT NULL,
            tp2 REAL NOT NULL,
            tp3 REAL NOT NULL,
            atr_at_entry REAL NOT NULL,
            trailing_stage TEXT NOT NULL,
            status TEXT NOT NULL
        )
    """)

    conn.commit()
    conn.close()


def persist_signal_to_db(record: dict, db_path: str = DB_PATH) -> bool:
    """Writes verified signal and forensic metrics to SQLite prior to webhook dispatch."""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO signal_history (
                timestamp_utc, asset, signal_decision, entry_price, stop_loss,
                tp1, tp2, tp3, trailing_status, optimal_atr_period,
                optimal_z_threshold, recommended_lot, risk_amount_usd,
                market_regime, confluence_score, smc_status, z_score,
                atr_value, order_flow_score, win_rate, profit_factor, max_drawdown
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            record["timestamp_utc"],
            record["asset"],
            record["signal_decision"],
            record["entry_price"],
            record["stop_loss"],
            record["tp1"],
            record["tp2"],
            record["tp3"],
            record["trailing_status"],
            record["optimal_atr_period"],
            record["optimal_z_threshold"],
            record["recommended_lot"],
            record["risk_amount_usd"],
            record["market_regime"],
            record["confluence_score"],
            record["smc_status"],
            record["z_score"],
            record["atr_value"],
            record["order_flow_score"],
            record["win_rate"],
            record["profit_factor"],
            record["max_drawdown"]
        ))

        if record["signal_decision"] in ("VERIFIED BUY", "VERIFIED SELL"):
            direction = "BUY" if "BUY" in record["signal_decision"] else "SELL"
            cursor.execute("""
                INSERT INTO active_positions (
                    opened_at_utc, direction, entry_price, current_sl,
                    tp1, tp2, tp3, atr_at_entry, trailing_stage, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                record["timestamp_utc"],
                direction,
                record["entry_price"],
                record["stop_loss"],
                record["tp1"],
                record["tp2"],
                record["tp3"],
                record["atr_value"],
                "INITIAL_SL_ARMED",
                "OPEN"
            ))

        conn.commit()
        conn.close()
        return True
    except Exception as exc:
        logger.error(f"SQLite persistence failure: {exc}")
        return False


# ==============================================================================
# AUTO-TRAILING STOP ENGINE
# ==============================================================================
def manage_auto_trailing_stops(current_price: float, current_atr: float, db_path: str = DB_PATH) -> str:
    """
    Evaluates open positions in SQLite and shifts Stop Loss to Breakeven at TP1
    or steps SL dynamically by 0.5 * ATR increments beyond TP1/TP2.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, direction, entry_price, current_sl, tp1, tp2, tp3, trailing_stage
        FROM active_positions
        WHERE status = 'OPEN'
    """)
    open_trades = cursor.fetchall()

    latest_protocol_status = "Armed @ TP1 Breakeven (0.5x ATR Step)"

    for trade in open_trades:
        trade_id, direction, entry, sl, tp1, tp2, tp3, stage = trade
        new_sl = sl
        new_stage = stage
        new_status = "OPEN"
        atr_step = 0.5 * max(current_atr, EPSILON)

        if direction == "BUY":
            if current_price <= sl:
                new_status = "CLOSED_SL"
                new_stage = "STOPPED_OUT"
            elif current_price >= tp3:
                new_status = "CLOSED_TP3"
                new_stage = "FULL_TP3_CAPTURED"
            elif current_price >= tp2:
                candidate_sl = round(current_price - atr_step, 2)
                if candidate_sl > new_sl:
                    new_sl = candidate_sl
                new_stage = "TRAILING_STEP_TP2_ACTIVE"
            elif current_price >= tp1:
                breakeven_sl = round(entry + 0.15, 2)
                candidate_sl = max(breakeven_sl, round(current_price - atr_step, 2))
                if candidate_sl > new_sl:
                    new_sl = candidate_sl
                new_stage = "BREAKEVEN_LOCKED_TP1"

        elif direction == "SELL":
            if current_price >= sl:
                new_status = "CLOSED_SL"
                new_stage = "STOPPED_OUT"
            elif current_price <= tp3:
                new_status = "CLOSED_TP3"
                new_stage = "FULL_TP3_CAPTURED"
            elif current_price <= tp2:
                candidate_sl = round(current_price + atr_step, 2)
                if candidate_sl < new_sl:
                    new_sl = candidate_sl
                new_stage = "TRAILING_STEP_TP2_ACTIVE"
            elif current_price <= tp1:
                breakeven_sl = round(entry - 0.15, 2)
                candidate_sl = min(breakeven_sl, round(current_price + atr_step, 2))
                if candidate_sl < new_sl:
                    new_sl = candidate_sl
                new_stage = "BREAKEVEN_LOCKED_TP1"

        cursor.execute("""
            UPDATE active_positions
            SET current_sl = ?, trailing_stage = ?, status = ?
            WHERE id = ?
        """, (new_sl, new_stage, new_status, trade_id))

        latest_protocol_status = f"Active @ {new_stage} (SL: {new_sl:.2f})"

    conn.commit()
    conn.close()
    return latest_protocol_status


# ==============================================================================
# DATA ACQUISITION, MULTI-INDEX CLEANING & CIRCUIT BREAKER
# ==============================================================================
def clean_yfinance_df(df: pd.DataFrame) -> pd.DataFrame:
    """Flattens MultiIndex columns and sanitizes OHLCV numeric series."""
    if df is None or df.empty:
        return pd.DataFrame()

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    df = df.loc[:, ~df.columns.duplicated()].copy()

    required_cols = ["Open", "High", "Low", "Close"]
    for col in required_cols:
        if col not in df.columns:
            return pd.DataFrame()
        df[col] = pd.to_numeric(df[col], errors="coerce")

    if "Volume" not in df.columns:
        df["Volume"] = 1.0
    else:
        df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce").fillna(0.0)
        if (df["Volume"] <= 0).all():
            # Synthetic tick-range volume proxy when spot FX feed reports 0 volume
            df["Volume"] = ((df["High"] - df["Low"]).abs() * 1000.0) + 1.0

    df.dropna(subset=required_cols, inplace=True)
    df = df[(df["Close"] > 0) & (df["High"] >= df["Low"])]
    df.sort_index(inplace=True)
    return df


def fetch_market_data(interval: str, period: str, retries: int = 3) -> pd.DataFrame:
    """
    Fetches official XAUUSD=X Spot Gold data via yfinance with retry backoff
    and automatic fallback to GC=F only if XAUUSD=X endpoint returns empty/404.
    """
    tickers_to_try = [TICKER_PRIMARY, TICKER_FALLBACK]

    for ticker in tickers_to_try:
        for attempt in range(1, retries + 1):
            try:
                logger.info(f"Fetching {ticker} [{interval}, {period}] (Attempt {attempt}/{retries})...")
                raw_df = yf.download(
                    tickers=ticker,
                    period=period,
                    interval=interval,
                    progress=False,
                    auto_adjust=False,
                    threads=False
                )
                cleaned = clean_yfinance_df(raw_df)
                if len(cleaned) >= 120:
                    logger.info(f"Successfully acquired {len(cleaned)} clean bars from {ticker} ({interval}).")
                    return cleaned
                logger.warning(f"Insufficient rows ({len(cleaned)}) from {ticker} ({interval}).")
            except Exception as exc:
                logger.warning(f"Fetch error for {ticker} ({interval}) on attempt {attempt}: {exc}")
            time.sleep(1.5 * attempt)

    raise RuntimeError(f"CRITICAL DATA FAILURE: Unable to retrieve valid OHLCV data for interval {interval}.")


def circuit_breaker_check(df_15m: pd.DataFrame, df_1h: pd.DataFrame) -> None:
    """Halts execution immediately if price anomalies, flash-crash spikes, or stale feeds occur."""
    if len(df_15m) < 120 or len(df_1h) < 120:
        raise ValueError("CIRCUIT BREAKER TRIGGERED: Insufficient historical bars for statistical validity.")

    last_close = float(df_15m["Close"].iloc[-1])
    prev_close = float(df_15m["Close"].iloc[-2])

    if not (500.0 <= last_close <= 15000.0):
        raise ValueError(f"CIRCUIT BREAKER TRIGGERED: Out-of-bounds XAUUSD price detected ({last_close}).")

    pct_jump = abs(last_close - prev_close) / max(prev_close, EPSILON)
    if pct_jump > 0.05:
        raise ValueError(f"CIRCUIT BREAKER TRIGGERED: Abnormal single-bar flash spike ({pct_jump*100:.2f}%).")


# ==============================================================================
# 5-PILLAR QUANTITATIVE & MICROSTRUCTURE FEATURE ENGINEERING
# ==============================================================================
def compute_indicators(df: pd.DataFrame, atr_period: int, z_window: int = 20) -> pd.DataFrame:
    """Computes vectorized ATR, Rolling Z-Score, Order Flow Imbalance, SMC swings, and Regime filters."""
    data = df.copy()

    # Pillar V: True Range & Wilder's ATR with division-by-zero protection
    prev_close = data["Close"].shift(1)
    tr1 = data["High"] - data["Low"]
    tr2 = (data["High"] - prev_close).abs()
    tr3 = (data["Low"] - prev_close).abs()
    true_range = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    data["ATR"] = true_range.ewm(alpha=1.0 / atr_period, adjust=False).mean() + EPSILON

    # Pillar III: Statistical Rolling Z-Score with epsilon guard
    rolling_mean = data["Close"].rolling(window=z_window, min_periods=z_window).mean()
    rolling_std = data["Close"].rolling(window=z_window, min_periods=z_window).std() + EPSILON
    data["Z_Score"] = (data["Close"] - rolling_mean) / rolling_std

    # Pillar II: Volume-Weighted Body-to-Wick Order Flow Displacement
    candle_range = (data["High"] - data["Low"]) + EPSILON
    signed_body = data["Close"] - data["Open"]
    body_to_wick = signed_body / candle_range
    vol_ma = data["Volume"].rolling(window=20, min_periods=1).mean() + EPSILON
    rel_volume = data["Volume"] / vol_ma
    data["Order_Flow"] = (body_to_wick * rel_volume).ewm(span=5, adjust=False).mean()

    # Trend & Volatility Regime Governor (Pillar IV & V)
    data["EMA_50"] = data["Close"].ewm(span=50, adjust=False).mean()
    data["EMA_200"] = data["Close"].ewm(span=200, adjust=False).mean()
    data["ATR_Pct"] = data["ATR"] / (data["Close"] + EPSILON)
    data["ATR_Baseline"] = data["ATR"].rolling(window=50, min_periods=10).mean() + EPSILON
    data["Vol_Ratio"] = data["ATR"] / data["ATR_Baseline"]

    # Pillar I: Programmatic SMC Swing High / Swing Low Detection (5-bar fractal window)
    lookback = 5
    data["Swing_High"] = data["High"].rolling(window=lookback * 2 + 1, center=False).max().shift(1)
    data["Swing_Low"] = data["Low"].rolling(window=lookback * 2 + 1, center=False).min().shift(1)

    return data


def detect_smc_structure(df: pd.DataFrame) -> tuple[str, int]:
    """
    Programmatically identifies Break of Structure (BOS) and Change of Character (ChoCh)
    across recent institutional swing boundaries.
    """
    recent = df.iloc[-30:].copy()
    close_now = float(recent["Close"].iloc[-1])
    prev_close = float(recent["Close"].iloc[-2])
    ema50_now = float(recent["EMA_50"].iloc[-1])
    ema200_now = float(recent["EMA_200"].iloc[-1])

    swing_high_prev = float(recent["High"].iloc[-15:-2].max())
    swing_low_prev = float(recent["Low"].iloc[-15:-2].min())

    macro_bullish = ema50_now > ema200_now

    if close_now > swing_high_prev:
        if macro_bullish:
            return "BULLISH BOS (CONTINUATION)", 30
        else:
            return "BULLISH CHOCH (REVERSAL SHIFT)", 28
    elif close_now < swing_low_prev:
        if not macro_bullish:
            return "BEARISH BOS (CONTINUATION)", -30
        else:
            return "BEARISH CHOCH (REVERSAL SHIFT)", -28
    elif prev_close <= swing_low_prev and close_now > swing_low_prev:
        return "BULLISH LIQUIDITY SWEEP / MITIGATION", 24
    elif prev_close >= swing_high_prev and close_now < swing_high_prev:
        return "BEARISH LIQUIDITY SWEEP / MITIGATION", -24
    else:
        return "INTERNAL RANGE CONSOLIDATION", 0


# ==============================================================================
# VECTORISED SLIPPAGE-AWARE BACKTEST & GRID SEARCH OPTIMIZER
# ==============================================================================
def run_vectorised_backtest(
    df_15m: pd.DataFrame,
    macro_trend_series: pd.Series,
    atr_period: int,
    z_threshold: float
) -> dict:
    """
    Simulates historical execution with spread, slippage, dynamic ATR SL, and 2.5R target
    to compute Profit Factor, Win Rate, Max Drawdown, and Equity Curve.
    """
    bt = compute_indicators(df_15m, atr_period=atr_period, z_window=20)
    bt["Macro_Bullish"] = macro_trend_series.reindex(bt.index).ffill().fillna(True)

    # Entry Conditions combining SMC breakout/pullback, Z-Score, Order Flow & Regime
    bull_cond = (
        (bt["Macro_Bullish"]) &
        (bt["Vol_Ratio"] >= 0.75) &
        (
            ((bt["Z_Score"] <= -z_threshold) & (bt["Order_Flow"] > -0.15)) |
            ((bt["Close"] > bt["Swing_High"]) & (bt["Order_Flow"] > 0.25))
        )
    )

    bear_cond = (
        (~bt["Macro_Bullish"]) &
        (bt["Vol_Ratio"] >= 0.75) &
        (
            ((bt["Z_Score"] >= z_threshold) & (bt["Order_Flow"] < 0.15)) |
            ((bt["Close"] < bt["Swing_Low"]) & (bt["Order_Flow"] < -0.25))
        )
    )

    signals = np.where(bull_cond, 1, np.where(bear_cond, -1, 0))
    closes = bt["Close"].values
    highs = bt["High"].values
    lows = bt["Low"].values
    atrs = bt["ATR"].values
    timestamps = bt.index

    equity = ACCOUNT_BALANCE
    equity_curve = [equity]
    equity_dates = [timestamps[0]]

    gross_profit = 0.0
    gross_loss = 0.0
    wins = 0
    losses = 0
    total_cost = SPREAD_POINTS + SLIPPAGE_POINTS

    i = 50
    n = len(bt)
    while i < n - 12:
        sig = signals[i]
        if sig == 0:
            i += 1
            continue

        entry_px = closes[i]
        atr_val = atrs[i]
        sl_dist = max(1.5 * atr_val, 1.50)
        tp_dist = 2.5 * sl_dist  # Blended multi-tier realized R-multiple

        risk_usd = equity * RISK_PERCENTAGE
        lot_size = max(0.01, round(risk_usd / (sl_dist * CONTRACT_SIZE), 2))

        outcome_r = 0.0
        exit_idx = i + 1

        for j in range(i + 1, min(i + 36, n)):
            exit_idx = j
            if sig == 1:
                if lows[j] <= (entry_px - sl_dist):
                    outcome_r = -1.0
                    break
                elif highs[j] >= (entry_px + tp_dist):
                    outcome_r = 2.5
                    break
            else:
                if highs[j] >= (entry_px + sl_dist):
                    outcome_r = -1.0
                    break
                elif lows[j] <= (entry_px - tp_dist):
                    outcome_r = 2.5
                    break
        else:
            # Time-stop exit after 36 bars (9 hours)
            unrealized = (closes[exit_idx] - entry_px) * sig
            outcome_r = unrealized / sl_dist

        cost_usd = total_cost * CONTRACT_SIZE * lot_size
        pnl_usd = (outcome_r * risk_usd) - cost_usd

        if pnl_usd > 0:
            gross_profit += pnl_usd
            wins += 1
        else:
            gross_loss += abs(pnl_usd)
            losses += 1

        equity = max(100.0, equity + pnl_usd)
        equity_curve.append(equity)
        equity_dates.append(timestamps[exit_idx])
        i = exit_idx + 1

    total_trades = wins + losses
    win_rate = (wins / total_trades * 100.0) if total_trades > 0 else 50.0
    profit_factor = (gross_profit / (gross_loss + EPSILON)) if total_trades > 0 else 1.00
    profit_factor = min(profit_factor, 9.99)

    eq_arr = np.array(equity_curve, dtype=float)
    running_max = np.maximum.accumulate(eq_arr)
    drawdowns = (running_max - eq_arr) / (running_max + EPSILON)
    max_drawdown = float(np.max(drawdowns) * 100.0)

    return {
        "atr_period": atr_period,
        "z_threshold": z_threshold,
        "total_trades": total_trades,
        "win_rate": round(win_rate, 2),
        "profit_factor": round(profit_factor, 2),
        "max_drawdown": round(max_drawdown, 2),
        "final_equity": round(equity, 2),
        "equity_curve": eq_arr,
        "equity_dates": equity_dates
    }


def optimize_parameters_grid_search(df_15m: pd.DataFrame, df_1h: pd.DataFrame) -> dict:
    """
    Executes systematic Grid Search across ATR_GRID x Z_THRESH_GRID
    to select parameters maximizing Profit Factor with minimum trade validity.
    """
    df_1h_ind = compute_indicators(df_1h, atr_period=14, z_window=20)
    macro_bullish = df_1h_ind["EMA_50"] > df_1h_ind["EMA_200"]

    best_result = None
    best_score = -1.0

    for atr_p in ATR_GRID:
        for z_th in Z_THRESH_GRID:
            res = run_vectorised_backtest(df_15m, macro_bullish, atr_p, z_th)
            # Penalize degenerate 0-trade combinations while rewarding Profit Factor
            trade_bonus = min(res["total_trades"], 15) * 0.05
            composite_pf = res["profit_factor"] + trade_bonus

            if composite_pf > best_score:
                best_score = composite_pf
                best_result = res

    logger.info(
        f"Grid Search Optimal Parameters -> ATR: {best_result['atr_period']}, "
        f"Z-Threshold: {best_result['z_threshold']}, "
        f"Profit Factor: {best_result['profit_factor']:.2f}, "
        f"Win Rate: {best_result['win_rate']:.1f}%, "
        f"Trades: {best_result['total_trades']}"
    )
    return best_result


# ==============================================================================
# HEADLESS MATPLOTLIB EQUITY & DRAWDOWN CHART GENERATOR
# ==============================================================================
def generate_backtest_chart(backtest_res: dict, output_path: str = CHART_PATH) -> str:
    """Renders institutional dark-theme equity & drawdown chart via non-GUI Agg backend."""
    dates = backtest_res["equity_dates"]
    equity = backtest_res["equity_curve"]

    running_peak = np.maximum.accumulate(equity)
    dd_pct = -((running_peak - equity) / (running_peak + EPSILON)) * 100.0

    plt.style.use("dark_background")
    fig, (ax1, ax2) = plt.subplots(
        2, 1, figsize=(12, 7), gridspec_kw={"height_ratios": [3, 1]}, sharex=True
    )
    fig.patch.set_facecolor("#0d1117")
    ax1.set_facecolor("#161b22")
    ax2.set_facecolor("#161b22")

    # Top Panel: Equity Curve
    ax1.plot(dates, equity, color="#00e676", linewidth=2.2, label="Strategy Equity (USD)")
    ax1.axhline(ACCOUNT_BALANCE, color="#8b949e", linestyle="--", linewidth=1.0, label="Initial Capital")
    ax1.fill_between(dates, ACCOUNT_BALANCE, equity, color="#00e676", alpha=0.12)

    title_str = (
        f"APEX-QUANT-OMNI v40.0 | XAUUSD Spot Backtest "
        f"(ATR={backtest_res['atr_period']}, Z={backtest_res['z_threshold']}) | "
        f"PF: {backtest_res['profit_factor']:.2f} | WR: {backtest_res['win_rate']:.1f}% | "
        f"MaxDD: {backtest_res['max_drawdown']:.1f}%"
    )
    ax1.set_title(title_str, fontsize=11, fontweight="bold", color="#f0f6fc", pad=12)
    ax1.set_ylabel("Account Equity ($)", fontsize=10, color="#c9d1d9")
    ax1.grid(True, linestyle=":", alpha=0.25, color="#8b949e")
    ax1.legend(loc="upper left", frameon=True, facecolor="#0d1117", edgecolor="#30363d")

    # Bottom Panel: Drawdown Profile
    ax2.fill_between(dates, 0, dd_pct, color="#ff5252", alpha=0.45, label="Drawdown (%)")
    ax2.plot(dates, dd_pct, color="#ff5252", linewidth=1.2)
    ax2.set_ylabel("Drawdown (%)", fontsize=10, color="#c9d1d9")
    ax2.set_xlabel("UTC Timeline", fontsize=10, color="#c9d1d9")
    ax2.grid(True, linestyle=":", alpha=0.25, color="#8b949e")
    ax2.legend(loc="lower left", frameon=True, facecolor="#0d1117", edgecolor="#30363d")

    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d %H:%M"))
    fig.autofmt_xdate()
    plt.tight_layout()
    plt.savefig(output_path, dpi=180, facecolor=fig.get_facecolor())
    plt.close(fig)

    logger.info(f"Backtest chart generated and saved to {output_path}")
    return "Generated & Saved as PNG"


# ==============================================================================
# LIVE 5-PILLAR SIGNAL EVALUATION & POSITION SIZING
# ==============================================================================
def evaluate_live_signal(df_15m: pd.DataFrame, df_1h: pd.DataFrame, opt_params: dict) -> dict:
    """Synthesizes all 5 quantitative pillars into a high-precision institutional trade setup."""
    opt_atr = opt_params["atr_period"]
    opt_z = opt_params["z_threshold"]

    df_15m_ind = compute_indicators(df_15m, atr_period=opt_atr, z_window=20)
    df_1h_ind = compute_indicators(df_1h, atr_period=opt_atr, z_window=20)

    # Macro 1H Trend Alignment (Pillar IV)
    h1_close = float(df_1h_ind["Close"].iloc[-1])
    h1_ema50 = float(df_1h_ind["EMA_50"].iloc[-1])
    h1_ema200 = float(df_1h_ind["EMA_200"].iloc[-1])
    macro_bull = (h1_close > h1_ema50) and (h1_ema50 >= h1_ema200)
    macro_bear = (h1_close < h1_ema50) and (h1_ema50 <= h1_ema200)

    # Execution 15M Metrics
    entry_price = float(df_15m_ind["Close"].iloc[-1])
    atr_val = float(df_15m_ind["ATR"].iloc[-1])
    z_score = float(df_15m_ind["Z_Score"].iloc[-1])
    order_flow = float(df_15m_ind["Order_Flow"].iloc[-1])
    vol_ratio = float(df_15m_ind["Vol_Ratio"].iloc[-1])

    # Pillar V: Volatility Market Regime Filter
    if vol_ratio < 0.70:
        market_regime = "CHOPPY FILTERED (LOW VOLATILITY)"
    else:
        market_regime = "TRENDING / ACTIVE EXPANSION"

    # Pillar I: Smart Money Concepts (SMC)
    smc_status, smc_score = detect_smc_structure(df_15m_ind)

    # Confluence Scoring Engine (-100 to +100 directional scale -> normalized 0-100)
    directional_points = 0

    # 1. SMC Weight (max 30)
    directional_points += smc_score

    # 2. Macro Fractal Harmony Weight (max 25)
    if macro_bull:
        directional_points += 25
    elif macro_bear:
        directional_points -= 25

    # 3. Statistical Z-Score Mean Reversion / Momentum Weight (max 25)
    if z_score <= -opt_z:
        directional_points += 25  # Statistical oversold exhaustion -> Bullish snapback
    elif z_score >= opt_z:
        directional_points -= 25  # Statistical overbought exhaustion -> Bearish snapback
    elif -0.8 <= z_score <= 0.8:
        directional_points += 10 if macro_bull else (-10 if macro_bear else 0)

    # 4. Order Flow Imbalance Weight (max 20)
    if order_flow > 0.20:
        directional_points += 20
    elif order_flow < -0.20:
        directional_points -= 20
    elif order_flow > 0.05:
        directional_points += 10
    elif order_flow < -0.05:
        directional_points -= 10

    raw_abs_score = min(100, abs(directional_points))

    # Regime Penalty
    if "CHOPPY" in market_regime:
        raw_abs_score = max(0, raw_abs_score - 25)

    # Final Signal Gatekeeper
    if raw_abs_score >= MIN_CONFLUENCE_SCORE and directional_points > 0 and "TRENDING" in market_regime:
        signal_decision = "VERIFIED BUY"
        direction_sign = 1
    elif raw_abs_score >= MIN_CONFLUENCE_SCORE and directional_points < 0 and "TRENDING" in market_regime:
        signal_decision = "VERIFIED SELL"
        direction_sign = -1
    else:
        signal_decision = "NEUTRAL / STAND ASIDE"
        direction_sign = 1 if directional_points >= 0 else -1

    # Dynamic ATR Stop Loss & Multi-Tier Take Profits (1.5R, 2.5R, 4.0R)
    swing_low = float(df_15m_ind["Low"].iloc[-12:].min())
    swing_high = float(df_15m_ind["High"].iloc[-12:].max())
    atr_risk_unit = max(1.5 * atr_val, 1.50)

    if direction_sign == 1:
        structural_sl = swing_low - (0.35 * atr_val)
        stop_loss = min(entry_price - atr_risk_unit, structural_sl)
        risk_distance = max(entry_price - stop_loss, 1.50)
        stop_loss = round(entry_price - risk_distance, 2)
        tp1 = round(entry_price + (1.5 * risk_distance), 2)
        tp2 = round(entry_price + (2.5 * risk_distance), 2)
        tp3 = round(entry_price + (4.0 * risk_distance), 2)
    else:
        structural_sl = swing_high + (0.35 * atr_val)
        stop_loss = max(entry_price + atr_risk_unit, structural_sl)
        risk_distance = max(stop_loss - entry_price, 1.50)
        stop_loss = round(entry_price + risk_distance, 2)
        tp1 = round(entry_price - (1.5 * risk_distance), 2)
        tp2 = round(entry_price - (2.5 * risk_distance), 2)
        tp3 = round(entry_price - (4.0 * risk_distance), 2)

    # Dynamic Position Sizing (1% Risk Governance)
    risk_amount_usd = round(ACCOUNT_BALANCE * RISK_PERCENTAGE, 2)
    raw_lots = risk_amount_usd / (risk_distance * CONTRACT_SIZE)
    recommended_lot = max(0.01, round(raw_lots, 2))

    # Update active trailing stops in SQLite
    trailing_status = manage_auto_trailing_stops(entry_price, atr_val, DB_PATH)

    return {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "asset": "XAUUSD (SPOT GOLD)",
        "signal_decision": signal_decision,
        "entry_price": round(entry_price, 2),
        "stop_loss": stop_loss,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "trailing_status": trailing_status,
        "optimal_atr_period": opt_atr,
        "optimal_z_threshold": opt_z,
        "recommended_lot": recommended_lot,
        "risk_amount_usd": risk_amount_usd,
        "market_regime": market_regime,
        "confluence_score": raw_abs_score,
        "smc_status": smc_status,
        "z_score": round(z_score, 4),
        "atr_value": round(atr_val, 4),
        "order_flow_score": round(order_flow, 4),
        "win_rate": opt_params["win_rate"],
        "profit_factor": opt_params["profit_factor"],
        "max_drawdown": opt_params["max_drawdown"]
    }


# ==============================================================================
# FORENSIC AUDIT LOGGER
# ==============================================================================
def forensic_audit_log(signal_pkg: dict, chart_status: str) -> bool:
    """
    Outputs raw mathematical audit metrics to GitHub Actions stdout
    and persists the event into SQLite prior to webhook dispatch.
    """
    db_ok = persist_signal_to_db(signal_pkg, DB_PATH)
    db_status_str = "Logged Successfully" if db_ok else "SQLite Write Warning"
    signal_pkg["db_status"] = db_status_str
    signal_pkg["chart_status"] = chart_status

    audit_banner = (
        "\n======================================================================\n"
        "🛡️  APEX-QUANT-OMNI ENTERPRISE v40.0 — FORENSIC MATHEMATICAL AUDIT\n"
        "======================================================================\n"
        f"• Timestamp (UTC)        : {signal_pkg['timestamp_utc']}\n"
        f"• Asset                  : {signal_pkg['asset']}\n"
        f"• Signal Decision        : {signal_pkg['signal_decision']}\n"
        f"• Precision Entry        : {signal_pkg['entry_price']:.2f}\n"
        f"• Forensic Stop Loss     : {signal_pkg['stop_loss']:.2f}\n"
        f"• Take Profit 1 (1.5R)   : {signal_pkg['tp1']:.2f}\n"
        f"• Take Profit 2 (2.5R)   : {signal_pkg['tp2']:.2f}\n"
        f"• Take Profit 3 (4.0R)   : {signal_pkg['tp3']:.2f}\n"
        f"• Trailing Stop Protocol : {signal_pkg['trailing_status']}\n"
        f"• Optimal ATR / Z-Thresh : ATR({signal_pkg['optimal_atr_period']}) | Z({signal_pkg['optimal_z_threshold']:.1f})\n"
        f"• Rolling Z-Score        : {signal_pkg['z_score']:.4f}\n"
        f"• Optimized ATR Value    : {signal_pkg['atr_value']:.4f}\n"
        f"• Order Flow Imbalance   : {signal_pkg['order_flow_score']:.4f}\n"
        f"• SMC Market Structure   : {signal_pkg['smc_status']}\n"
        f"• Market Regime          : {signal_pkg['market_regime']}\n"
        f"• Confluence Score       : {signal_pkg['confluence_score']}/100\n"
        f"• Position Size (1% Risk): {signal_pkg['recommended_lot']:.2f} Lots (${signal_pkg['risk_amount_usd']:.2f})\n"
        f"• Backtest Metrics       : WR={signal_pkg['win_rate']:.1f}% | PF={signal_pkg['profit_factor']:.2f} | MaxDD={signal_pkg['max_drawdown']:.1f}%\n"
        f"• SQLite DB Persistence  : {db_status_str}\n"
        f"• Backtest Chart Status  : {chart_status}\n"
        "======================================================================\n"
    )
    print(audit_banner, flush=True)
    return db_ok


# ==============================================================================
# PARALLEL DUAL-CHANNEL WEBHOOK DELIVERY (TELEGRAM + DISCORD)
# ==============================================================================
def build_institutional_report_html(pkg: dict) -> str:
    """Formats the institutional forensic report matching the exact production schema."""
    return (
        f"🛡️ <b>APEX-QUANT-OMNI ENTERPRISE v40.0 (ULTIMATE EDITION)</b> 🛡️\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🌟 <b>Asset:</b> {pkg['asset']}\n"
        f"📊 <b>Signal Decision:</b> {pkg['signal_decision']}\n"
        f"🎯 <b>Precision Entry:</b> {pkg['entry_price']:.2f}\n"
        f"🛑 <b>Forensic Stop Loss:</b> {pkg['stop_loss']:.2f}\n"
        f"📈 <b>Take Profit 1 (1.5R):</b> {pkg['tp1']:.2f}\n"
        f"📈 <b>Take Profit 2 (2.5R):</b> {pkg['tp2']:.2f}\n"
        f"🚀 <b>Take Profit 3 (4.0R):</b> {pkg['tp3']:.2f}\n"
        f"🔄 <b>Trailing Stop Protocol:</b> {pkg['trailing_status']}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚙️ <b>Grid Search & Visualization:</b>\n"
        f"• <i>Optimal ATR Period:</i> {pkg['optimal_atr_period']}\n"
        f"• <i>Optimal Z-Threshold:</i> {pkg['optimal_z_threshold']:.1f}\n"
        f"• <i>Backtest Chart:</i> {pkg['chart_status']}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 <b>Risk & Position Sizing:</b>\n"
        f"• <i>Recommended Lot:</i> {pkg['recommended_lot']:.2f} Lots ({RISK_PERCENTAGE*100:.0f}% Risk)\n"
        f"• <i>Risk Amount (USD):</i> ${pkg['risk_amount_usd']:.2f}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔬 <b>Forensic Audit, Database & Backtest:</b>\n"
        f"• <i>Market Regime:</i> {pkg['market_regime']}\n"
        f"• <i>Confluence Score:</i> {pkg['confluence_score']}/100\n"
        f"• <i>SQLite DB Status:</i> {pkg['db_status']}\n"
        f"• <i>Error Handler Status:</i> Active / Nominal\n"
        f"• <i>Optimized Win Rate:</i> {pkg['win_rate']:.1f}%\n"
        f"• <i>Profit Factor:</i> {pkg['profit_factor']:.2f}\n"
        f"• <i>Max Drawdown:</i> {pkg['max_drawdown']:.1f}%\n"
        f"• <i>Timestamp (UTC):</i> {pkg['timestamp_utc']}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━"
    )


def send_telegram_webhook(html_message: str, chart_path: str) -> None:
    """Sends the HTML report and attaches the backtest PNG chart to Telegram."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not configured; skipping Telegram.")
        return

    try:
        msg_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        resp = requests.post(
            msg_url,
            json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": html_message,
                "parse_mode": "HTML",
                "disable_web_page_preview": True
            },
            timeout=20
        )
        resp.raise_for_status()
        logger.info("Telegram forensic signal message delivered successfully.")

        if os.path.exists(chart_path):
            photo_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
            with open(chart_path, "rb") as img_file:
                photo_resp = requests.post(
                    photo_url,
                    data={
                        "chat_id": TELEGRAM_CHAT_ID,
                        "caption": "📊 APEX-QUANT-OMNI v40.0 — Grid-Optimized Equity & Drawdown Curve"
                    },
                    files={"photo": img_file},
                    timeout=25
                )
                photo_resp.raise_for_status()
            logger.info("Telegram backtest performance PNG delivered successfully.")
    except Exception as exc:
        logger.error(f"Telegram webhook delivery error: {exc}")
        raise


def send_discord_webhook(pkg: dict, chart_path: str) -> None:
    """Sends a structured embed payload and attaches the PNG chart to Discord."""
    if not DISCORD_WEBHOOK_URL:
        logger.warning("DISCORD_WEBHOOK_URL not configured; skipping Discord.")
        return

    try:
        is_buy = "BUY" in pkg["signal_decision"]
        embed_color = 0x00E676 if is_buy else 0xFF5252

        embed = {
            "title": "🛡️ APEX-QUANT-OMNI ENTERPRISE v40.0 (ULTIMATE EDITION)",
            "color": embed_color,
            "fields": [
                {"name": "🌟 Asset", "value": pkg["asset"], "inline": True},
                {"name": "📊 Signal Decision", "value": f"**{pkg['signal_decision']}**", "inline": True},
                {"name": "🎯 Precision Entry", "value": f"`{pkg['entry_price']:.2f}`", "inline": True},
                {"name": "🛑 Forensic Stop Loss", "value": f"`{pkg['stop_loss']:.2f}`", "inline": True},
                {"name": "📈 Take Profit 1 (1.5R)", "value": f"`{pkg['tp1']:.2f}`", "inline": True},
                {"name": "📈 Take Profit 2 (2.5R)", "value": f"`{pkg['tp2']:.2f}`", "inline": True},
                {"name": "🚀 Take Profit 3 (4.0R)", "value": f"`{pkg['tp3']:.2f}`", "inline": True},
                {"name": "🔄 Trailing Stop Protocol", "value": pkg["trailing_status"], "inline": False},
                {
                    "name": "⚙️ Grid Search & Visualization",
                    "value": (
                        f"• Optimal ATR Period: `{pkg['optimal_atr_period']}`\n"
                        f"• Optimal Z-Threshold: `{pkg['optimal_z_threshold']:.1f}`\n"
                        f"• Backtest Chart: `{pkg['chart_status']}`"
                    ),
                    "inline": True
                },
                {
                    "name": "💰 Risk & Position Sizing",
                    "value": (
                        f"• Recommended Lot: `{pkg['recommended_lot']:.2f} Lots` ({RISK_PERCENTAGE*100:.0f}% Risk)\n"
                        f"• Risk Amount (USD): `${pkg['risk_amount_usd']:.2f}`"
                    ),
                    "inline": True
                },
                {
                    "name": "🔬 Forensic Audit, Database & Backtest",
                    "value": (
                        f"• Market Regime: `{pkg['market_regime']}`\n"
                        f"• Confluence Score: `{pkg['confluence_score']}/100`\n"
                        f"• SQLite DB Status: `{pkg['db_status']}`\n"
                        f"• Error Handler Status: `Active / Nominal`\n"
                        f"• Optimized Win Rate: `{pkg['win_rate']:.1f}%`\n"
                        f"• Profit Factor: `{pkg['profit_factor']:.2f}`\n"
                        f"• Max Drawdown: `{pkg['max_drawdown']:.1f}%`\n"
                        f"• Timestamp (UTC): `{pkg['timestamp_utc']}`"
                    ),
                    "inline": False
                }
            ]
        }

        resp = requests.post(
            DISCORD_WEBHOOK_URL,
            json={"embeds": [embed]},
            timeout=20
        )
        resp.raise_for_status()

        if os.path.exists(chart_path):
            with open(chart_path, "rb") as f:
                file_resp = requests.post(
                    DISCORD_WEBHOOK_URL,
                    files={"file": ("backtest_performance.png", f, "image/png")},
                    timeout=25
                )
                file_resp.raise_for_status()

        logger.info("Discord forensic signal & PNG chart delivered successfully.")
    except Exception as exc:
        logger.error(f"Discord webhook delivery error: {exc}")
        raise


def dispatch_parallel_webhooks(pkg: dict, chart_path: str) -> None:
    """Dispatches verified signals concurrently to Telegram and Discord with isolated fault handling."""
    html_report = build_institutional_report_html(pkg)

    with ThreadPoolExecutor(max_workers=2) as executor:
        tg_future = executor.submit(send_telegram_webhook, html_report, chart_path)
        dc_future = executor.submit(send_discord_webhook, pkg, chart_path)

        for name, future in [("Telegram", tg_future), ("Discord", dc_future)]:
            try:
                future.result()
            except Exception as exc:
                logger.error(f"Parallel dispatch channel [{name}] encountered an error: {exc}")
                if name == "Discord":
                    dispatch_telegram_error("Discord Webhook Failure", str(exc))


# ==============================================================================
# MASTER ORCHESTRATION PIPELINE
# ==============================================================================
def main() -> None:
    logger.info("Initializing APEX-QUANT-OMNI ENTERPRISE v40.0 Quantitative Engine...")

    try:
        # 1. Initialize SQLite Persistence Engine
        init_sqlite_db(DB_PATH)

        # 2. Acquire Multi-Timeframe Spot Gold Feeds (15m Execution + 1h Macro)
        df_15m = fetch_market_data(interval="15m", period="30d", retries=3)
        df_1h = fetch_market_data(interval="1h", period="60d", retries=3)

        # 3. Execute Circuit Breaker Data Integrity Verification
        circuit_breaker_check(df_15m, df_1h)

        # 4. Run Grid Search Parameter Optimization
        optimal_backtest = optimize_parameters_grid_search(df_15m, df_1h)

        # 5. Generate Headless Matplotlib Equity & Drawdown PNG
        chart_status = generate_backtest_chart(optimal_backtest, CHART_PATH)

        # 6. Evaluate Live 5-Pillar Signal & Manage Trailing Stops
        signal_package = evaluate_live_signal(df_15m, df_1h, optimal_backtest)

        # 7. Execute Forensic Audit Logging & SQLite Persistence
        forensic_audit_log(signal_package, chart_status)

        # 8. Dispatch Webhooks Only When Institutional Confluence Threshold Is Met
        if signal_package["signal_decision"] in ("VERIFIED BUY", "VERIFIED SELL"):
            logger.info(f"Verified signal detected: {signal_package['signal_decision']}. Dispatching webhooks...")
            dispatch_parallel_webhooks(signal_package, CHART_PATH)
        else:
            logger.info(
                f"No actionable signal dispatched ({signal_package['signal_decision']} | "
                f"Score: {signal_package['confluence_score']}/100 | Regime: {signal_package['market_regime']})."
            )

        logger.info("APEX-QUANT-OMNI v40.0 execution cycle completed cleanly.")

    except Exception as fatal_exc:
        tb_str = traceback.format_exc()
        logger.critical(f"FATAL ENGINE EXCEPTION:\n{tb_str}")
        dispatch_telegram_error("Fatal Quant Engine Exception", tb_str)
        sys.exit(1)


if __name__ == "__main__":
    main()
