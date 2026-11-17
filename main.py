"""
APEX-QUANT-OMNI ENTERPRISE v40.0 (ULTIMATE EDITION)
XAUUSD Spot Gold — 5-Pillar Hybrid Quant Signal Engine
Serverless: GitHub Actions | DB: SQLite | Charts: Matplotlib (Agg)
"""
import matplotlib
matplotlib.use("Agg")

import concurrent.futures
import datetime as dt
import json
import lzma
import math
import os
import random
import sqlite3
import string
import struct
import sys
import time
import traceback
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests
import websocket
import yfinance as yf
from requests.adapters import HTTPAdapter


# ==============================================================================
# CONFIGURATION
# ==============================================================================
ENGINE_NAME = "APEX-QUANT-OMNI ENTERPRISE v40.0"
PRIMARY_YF_TICKER = "XAUUSD=X"
TV_SYMBOLS = ["OANDA:XAUUSD", "FX_IDC:XAUUSD",
              "FOREXCOM:XAUUSD", "PEPPERSTONE:XAUUSD"]
PROXY_TICKERS = ["PAXG-USD", "XAUT-USD"]

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
XAU_MULT = 100.0
MIN_RRR = 3.0
CONFLUENCE_THRESHOLD = 65.0
ATR_GRID = [10, 14, 20]
Z_GRID = [1.5, 2.0, 2.5]
Z_WINDOW = 20
CIRCUIT_STALE_H = 96.0
CIRCUIT_JUMP_PCT = 0.08

DUKASCOPY_DAYS = 30
DUKASCOPY_FMT = ">5if"
DUKASCOPY_SIZE = struct.calcsize(DUKASCOPY_FMT)
DUKASCOPY_DIV = 1000.0


class CircuitBreakerTripped(RuntimeError):
    """Raised on data integrity or execution invariant failures."""


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


@dataclass
class QuantitativeSignal:
    decision: str
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
    feed_source: str
    swissquote_spot_mid: Optional[float]
    timestamp_utc: str
    pillar_breakdown: Dict[str, Any] = field(default_factory=dict)


# ==============================================================================
# TELEGRAM ERROR HANDLER
# ==============================================================================
def send_telegram_error_alert(context: str, exc: BaseException) -> bool:
    tb_lines = traceback.format_exception(type(exc), exc, exc.__traceback__)
    tb_tail = "".join(tb_lines[-4:]).strip()[-1200:]

    def _esc(s: str) -> str:
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    msg = (
        f"🚨 <b>{ENGINE_NAME} — CRITICAL ERROR</b> 🚨\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚠️ <b>Module:</b> {_esc(context)}\n"
        f"❌ <b>Exception:</b> <code>{_esc(f'{type(exc).__name__}: {exc}')}</code>\n"
        f"🕒 <b>UTC:</b> {dt.datetime.now(dt.timezone.utc).isoformat()}\n"
        f"📋 <b>Trace:</b>\n<pre>{_esc(tb_tail)}</pre>"
    )
    print(f"[ERROR HANDLER] {context} -> {type(exc).__name__}: {exc}", file=sys.stderr)

    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("[ERROR HANDLER] Telegram credentials missing; alert logged locally.",
              file=sys.stderr)
        return False
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": msg,
                  "parse_mode": "HTML", "disable_web_page_preview": True},
            timeout=15,
        )
        resp.raise_for_status()
        return True
    except Exception as e:
        print(f"[ERROR HANDLER] Telegram dispatch failed: {e}", file=sys.stderr)
        return False


# ==============================================================================
# SQLITE PERSISTENCE
# ==============================================================================
def init_sqlite_db(db_path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS signal_audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp_utc TEXT NOT NULL,
            asset TEXT NOT NULL,
            feed_source TEXT NOT NULL DEFAULT 'UNKNOWN',
            decision TEXT NOT NULL,
            entry_price REAL NOT NULL,
            stop_loss REAL NOT NULL,
            tp1 REAL NOT NULL, tp2 REAL NOT NULL, tp3 REAL NOT NULL,
            trailing_protocol TEXT NOT NULL,
            optimal_atr_period INTEGER NOT NULL,
            optimal_z_threshold REAL NOT NULL,
            recommended_lot REAL NOT NULL, risk_usd REAL NOT NULL,
            market_regime TEXT NOT NULL, confluence_score REAL NOT NULL,
            z_score REAL NOT NULL, atr_value REAL NOT NULL,
            smc_bias TEXT NOT NULL, order_flow_score REAL NOT NULL,
            htf_alignment TEXT NOT NULL,
            win_rate REAL NOT NULL, profit_factor REAL NOT NULL,
            max_drawdown REAL NOT NULL,
            raw_forensic_json TEXT NOT NULL
        );
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS active_positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            opened_at_utc TEXT NOT NULL, updated_at_utc TEXT NOT NULL,
            asset TEXT NOT NULL, direction TEXT NOT NULL,
            entry_price REAL NOT NULL, initial_sl REAL NOT NULL,
            current_sl REAL NOT NULL, tp1 REAL NOT NULL, tp2 REAL NOT NULL,
            tp3 REAL NOT NULL, atr_at_entry REAL NOT NULL, lot_size REAL NOT NULL,
            tp1_breached INTEGER NOT NULL DEFAULT 0,
            highest_favorable REAL NOT NULL, trailing_state TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'OPEN'
        );
    """)
    conn.commit()
    return conn


def forensic_audit_log(conn: sqlite3.Connection, signal: QuantitativeSignal,
                       best_metrics: BacktestMetrics,
                       raw_math: Dict[str, Any]) -> str:
    raw_json = json.dumps(raw_math, default=str)
    print("\n" + "=" * 78)
    print(f"FORENSIC AUDIT | {ENGINE_NAME} | {signal.timestamp_utc}")
    print("=" * 78)
    print(f"Asset              : XAUUSD SPOT [{signal.feed_source}]")
    print(f"Decision           : {signal.decision}")
    print(f"Entry / SL / TP1-3 : {signal.entry_price:.2f} / "
          f"{signal.stop_loss:.2f} / {signal.tp1:.2f} / {signal.tp2:.2f} / {signal.tp3:.2f}")
    print(f"Regime             : {signal.market_regime}")
    print(f"Confluence Score   : {signal.confluence_score:.2f}/100")
    print(f"Grid (ATR,Z)       : ({best_metrics.atr_period},{best_metrics.z_threshold:.1f})")
    print(f"Backtest           : Trades={best_metrics.total_trades} "
          f"WR={best_metrics.win_rate:.1f}% PF={best_metrics.profit_factor:.2f} "
          f"MaxDD={best_metrics.max_drawdown:.2f}%")
    print(f"Lot / Risk USD     : {signal.recommended_lot:.2f} / ${signal.risk_usd:.2f}")
    print(f"Raw Math           : {raw_json}")
    print("=" * 78 + "\n")
    try:
        conn.execute("""
            INSERT INTO signal_audit_log (
                timestamp_utc, asset, feed_source, decision, entry_price, stop_loss,
                tp1, tp2, tp3, trailing_protocol, optimal_atr_period,
                optimal_z_threshold, recommended_lot, risk_usd, market_regime,
                confluence_score, z_score, atr_value, smc_bias, order_flow_score,
                htf_alignment, win_rate, profit_factor, max_drawdown, raw_forensic_json
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            signal.timestamp_utc, "XAUUSD", signal.feed_source, signal.decision,
            round(signal.entry_price, 4), round(signal.stop_loss, 4),
            round(signal.tp1, 4), round(signal.tp2, 4), round(signal.tp3, 4),
            signal.trailing_protocol, int(best_metrics.atr_period),
            float(best_metrics.z_threshold), round(signal.recommended_lot, 2),
            round(signal.risk_usd, 2), signal.market_regime,
            round(signal.confluence_score, 2), round(signal.z_score, 6),
            round(signal.atr_value, 6), signal.smc_bias,
            round(signal.order_flow_score, 4), signal.htf_alignment,
            round(best_metrics.win_rate, 2), round(best_metrics.profit_factor, 4),
            round(best_metrics.max_drawdown, 2), raw_json,
        ))
        conn.commit()
        return "Logged Successfully"
    except Exception as db_exc:
        send_telegram_error_alert("SQLite forensic_audit_log", db_exc)
        return f"DB Error: {db_exc}"


# ==============================================================================
# DATA PIPELINE
# ==============================================================================
def normalize_ohlcv_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.copy()
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df.columns = [str(c).strip().title() for c in df.columns]
    df = df.loc[:, ~df.columns.duplicated()]
    for c in ["Open", "High", "Low", "Close"]:
        if c not in df.columns:
            return pd.DataFrame()
        df[c] = pd.to_numeric(df[c], errors="coerce")
    if "Volume" not in df.columns:
        df["Volume"] = 0.0
    else:
        df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce").fillna(0.0)
    idx = pd.to_datetime(df.index, utc=True)
    df.index = pd.DatetimeIndex(idx).as_unit("ns")
    df.index.name = "Timestamp"
    df = df[~df.index.duplicated(keep="last")].sort_index()
    df = df.dropna(subset=["Open", "High", "Low", "Close"])
    df = df[(df[["Open", "High", "Low", "Close"]] > 0).all(axis=1)]
    if df.empty:
        return pd.DataFrame()
    df["High"] = np.maximum(df["High"], np.maximum(df["Open"], df["Close"]))
    df["Low"] = np.minimum(df["Low"], np.minimum(df["Open"], df["Close"]))
    return df[["Open", "High", "Low", "Close", "Volume"]]


def fetch_swissquote_spot_mid() -> Optional[float]:
    try:
        r = requests.get(
            "https://forex-data-feed.swissquote.com/public-quotes/bboquotes/instrument/XAU/USD",
            headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        r.raise_for_status()
        data = r.json()
        for node in data or []:
            for prof in node.get("spreadProfilePrices", []):
                bid, ask = prof.get("bid"), prof.get("ask")
                if bid and ask and float(bid) > 0 and float(ask) > 0:
                    return (float(bid) + float(ask)) * 0.5
    except Exception:
        pass
    return None


def _tv_send(ws, func, args):
    p = json.dumps({"m": func, "p": args}, separators=(",", ":"))
    ws.send(f"~m~{len(p)}~m~{p}")


def _tv_parse_frames(raw: str) -> List[str]:
    frames, idx, n = [], 0, len(raw)
    while idx < n:
        if not raw.startswith("~m~", idx):
            break
        sep = raw.find("~m~", idx + 3)
        if sep == -1:
            break
        try:
            flen = int(raw[idx + 3:sep])
        except ValueError:
            break
        start = sep + 3
        frames.append(raw[start:start + flen])
        idx = start + flen
    return frames


def _fetch_tradingview_series(symbol: str, resolution: str, n_bars: int) -> pd.DataFrame:
    ws_url = "wss://data.tradingview.com/socket.io/websocket?from=chart%2F"
    sess = "cs_" + "".join(random.choices(string.ascii_lowercase + string.digits, k=12))
    bars: Dict[float, Tuple[float, float, float, float, float]] = {}
    ws = websocket.create_connection(
        ws_url, timeout=15,
        header={"Origin": "https://data.tradingview.com",
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"})
    try:
        _tv_send(ws, "set_auth_token", ["unauthorized_user_token"])
        _tv_send(ws, "chart_create_session", [sess, ""])
        _tv_send(ws, "switch_timezone", [sess, "Etc/UTC"])
        cfg = json.dumps({"symbol": symbol, "adjustment": "splits"}, separators=(",", ":"))
        _tv_send(ws, "resolve_symbol", [sess, "sds_sym_1", f"={cfg}"])
        _tv_send(ws, "create_series",
                 [sess, "sds_1", "s1", "sds_sym_1", resolution, n_bars])
        deadline = time.monotonic() + 18.0
        while time.monotonic() < deadline:
            raw = ws.recv()
            if not raw:
                continue
            for frame in _tv_parse_frames(raw):
                if frame.startswith("~h~"):
                    ws.send(f"~m~{len(frame)}~m~{frame}")
                    continue
                try:
                    pkt = json.loads(frame)
                except json.JSONDecodeError:
                    continue
                if not isinstance(pkt, dict):
                    continue
                if pkt.get("m") in ("symbol_error", "series_error"):
                    return pd.DataFrame()
                if pkt.get("m") in ("timescale_update", "du"):
                    for item in pkt.get("p", []):
                        if not isinstance(item, dict):
                            continue
                        for val in item.values():
                            if not isinstance(val, dict):
                                continue
                            for bar in val.get("s", []):
                                v = bar.get("v", [])
                                if len(v) >= 5:
                                    ts = float(v[0])
                                    vol = float(v[5]) if len(v) >= 6 and v[5] is not None else 0.0
                                    bars[ts] = (float(v[1]), float(v[2]),
                                                float(v[3]), float(v[4]), vol)
                if pkt.get("m") == "series_completed" and len(bars) >= 60:
                    break
            else:
                continue
            break
    finally:
        try:
            ws.close()
        except Exception:
            pass
    if not bars:
        return pd.DataFrame()
    rows = [{"Timestamp": pd.to_datetime(ts, unit="s", utc=True),
             "Open": v[0], "High": v[1], "Low": v[2],
             "Close": v[3], "Volume": v[4]}
            for ts, v in sorted(bars.items())]
    return normalize_ohlcv_dataframe(pd.DataFrame(rows).set_index("Timestamp"))


def _try_tradingview() -> Tuple[pd.DataFrame, pd.DataFrame, str]:
    for sym in TV_SYMBOLS:
        try:
            print(f"[DATA] TradingView attempt: {sym}")
            d15 = _fetch_tradingview_series(sym, "15", 1500)
            d1h = _fetch_tradingview_series(sym, "60", 1000)
            if _is_valid_feed(d15, d1h):
                return d15, d1h, f"TradingView ({sym})"
        except Exception as e:
            print(f"[DATA] TV {sym} failed: {e}")
    return pd.DataFrame(), pd.DataFrame(), ""


def _fetch_dukascopy_day(session: requests.Session, day: dt.datetime):
    url = (f"https://datafeed.dukascopy.com/datafeed/XAUUSD/"
           f"{day.year}/{day.month - 1:02d}/{day.day:02d}/BID_candles_min_1.bi5")
    try:
        r = session.get(url, timeout=10)
        if r.status_code != 200 or not r.content:
            return []
        raw = lzma.decompress(r.content)
        base = pd.Timestamp(day.year, day.month, day.day, tz="UTC")
        usable = len(raw) - (len(raw) % DUKASCOPY_SIZE)
        rows = []
        for off in range(0, usable, DUKASCOPY_SIZE):
            sec, o, c, l, h, vol = struct.unpack(
                DUKASCOPY_FMT, raw[off:off + DUKASCOPY_SIZE])
            o_px, h_px = o / DUKASCOPY_DIV, h / DUKASCOPY_DIV
            l_px, c_px = l / DUKASCOPY_DIV, c / DUKASCOPY_DIV
            if c_px <= 0:
                continue
            rows.append((base + pd.Timedelta(seconds=int(sec)),
                         o_px, h_px, l_px, c_px, float(vol)))
        return rows
    except Exception:
        return []


def _try_dukascopy() -> Tuple[pd.DataFrame, pd.DataFrame]:
    print(f"[DATA] Dukascopy ({DUKASCOPY_DAYS}d 1m .bi5)...")
    today = dt.datetime.now(dt.timezone.utc).replace(hour=0, minute=0,
                                                     second=0, microsecond=0)
    days = [today - dt.timedelta(days=d)
            for d in range(DUKASCOPY_DAYS, -1, -1)]
    session = requests.Session()
    session.mount("https://",
                  HTTPAdapter(pool_connections=16, pool_maxsize=16))
    session.headers.update({"User-Agent": "Mozilla/5.0"})
    all_rows = []
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
            for chunk in pool.map(lambda d: _fetch_dukascopy_day(session, d), days):
                if chunk:
                    all_rows.extend(chunk)
    finally:
        session.close()
    if len(all_rows) < 1800:
        return pd.DataFrame(), pd.DataFrame()
    m1 = pd.DataFrame(all_rows, columns=[
        "Timestamp", "Open", "High", "Low", "Close", "Volume"])
    m1["Timestamp"] = pd.to_datetime(m1["Timestamp"], utc=True)
    m1 = m1.set_index("Timestamp").sort_index()
    agg = {"Open": "first", "High": "max", "Low": "min",
           "Close": "last", "Volume": "sum"}
    return (normalize_ohlcv_dataframe(m1.resample("15min").agg(agg).dropna(subset=["Open", "Close"])),
            normalize_ohlcv_dataframe(m1.resample("1h").agg(agg).dropna(subset=["Open", "Close"])))


def _try_yfinance_ticker(ticker: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    try:
        raw15 = yf.download(ticker, period="60d", interval="15m",
                            auto_adjust=False, progress=False, threads=False)
        raw1h = yf.download(ticker, period="180d", interval="1h",
                            auto_adjust=False, progress=False, threads=False)
        return normalize_ohlcv_dataframe(raw15), normalize_ohlcv_dataframe(raw1h)
    except Exception as e:
        print(f"[DATA] yfinance {ticker} failed: {e}")
        return pd.DataFrame(), pd.DataFrame()


def _try_kraken_paxg() -> Tuple[pd.DataFrame, pd.DataFrame]:
    print("[DATA] Kraken PAXGUSD fallback...")
    url = "https://api.kraken.com/0/public/OHLC"

    def _pull(interval: int) -> pd.DataFrame:
        r = requests.get(url, params={"pair": "PAXGUSD", "interval": interval}, timeout=15)
        r.raise_for_status()
        pl = r.json()
        if pl.get("error"):
            raise RuntimeError(pl["error"])
        keys = [k for k in pl.get("result", {}) if k != "last"]
        if not keys:
            return pd.DataFrame()
        df = pd.DataFrame(pl["result"][keys[0]], columns=[
            "Time", "Open", "High", "Low", "Close", "VWAP", "Volume", "Count"])
        df["Time"] = pd.to_datetime(df["Time"], unit="s", utc=True)
        return normalize_ohlcv_dataframe(
            df.set_index("Time")[["Open", "High", "Low", "Close", "Volume"]])

    try:
        return _pull(15), _pull(60)
    except Exception as e:
        print(f"[DATA] Kraken failed: {e}")
        return pd.DataFrame(), pd.DataFrame()


def _calibrate_proxy(df15: pd.DataFrame, df1h: pd.DataFrame,
                     swiss_mid: Optional[float]):
    if swiss_mid is None or df15.empty:
        return df15, df1h, ""
    delta = swiss_mid - float(df15["Close"].iloc[-1])
    if abs(delta) <= 0.05:
        return df15, df1h, ""
    a, b = df15.copy(), df1h.copy()
    for col in ["Open", "High", "Low", "Close"]:
        a[col] = a[col] + delta
        b[col] = b[col] + delta
    return a, b, f" + Basis Calibrated {delta:+.2f}"


def _is_valid_feed(d15: pd.DataFrame, d1h: pd.DataFrame) -> bool:
    if d15.empty or d1h.empty or len(d15) < 120 or len(d1h) < 60:
        return False
    last = d15.index[-1].to_pydatetime()
    hours = (dt.datetime.now(dt.timezone.utc) - last).total_seconds() / 3600.0
    return hours <= CIRCUIT_STALE_H


def run_circuit_breaker(d15: pd.DataFrame, d1h: pd.DataFrame) -> None:
    if d15.empty or len(d15) < 120:
        raise CircuitBreakerTripped(f"Insufficient 15m bars: {len(d15)} (need >=120)")
    if d1h.empty or len(d1h) < 60:
        raise CircuitBreakerTripped(f"Insufficient 1h bars: {len(d1h)} (need >=60)")
    last = d15.index[-1].to_pydatetime()
    hours = (dt.datetime.now(dt.timezone.utc) - last).total_seconds() / 3600.0
    if hours > CIRCUIT_STALE_H:
        raise CircuitBreakerTripped(f"Feed stale {hours:.1f}h (limit {CIRCUIT_STALE_H}h)")
    px = float(d15["Close"].iloc[-1])
    if not math.isfinite(px) or not (500.0 < px < 15000.0):
        raise CircuitBreakerTripped(f"Implausible price: {px}")
    pct = d15["Close"].pct_change().dropna().abs()
    if not pct.empty and float(pct.iloc[-1]) > CIRCUIT_JUMP_PCT:
        raise CircuitBreakerTripped(
            f"Anomalous spike {float(pct.iloc[-1]) * 100:.2f}%")


def fetch_spot_gold_data() -> Tuple[pd.DataFrame, pd.DataFrame, str, Optional[float]]:
    swiss = fetch_swissquote_spot_mid()
    if swiss:
        print(f"[DATA] Swissquote spot mid: {swiss:.2f}")

    print(f"[DATA] Tier-1: yfinance {PRIMARY_YF_TICKER}")
    d15, d1h = _try_yfinance_ticker(PRIMARY_YF_TICKER)
    if _is_valid_feed(d15, d1h):
        run_circuit_breaker(d15, d1h)
        return d15, d1h, f"yfinance ({PRIMARY_YF_TICKER})", swiss

    d15, d1h, src = _try_tradingview()
    if _is_valid_feed(d15, d1h):
        run_circuit_breaker(d15, d1h)
        return d15, d1h, src, swiss

    d15, d1h = _try_dukascopy()
    if _is_valid_feed(d15, d1h):
        run_circuit_breaker(d15, d1h)
        return d15, d1h, "Dukascopy Spot XAUUSD", swiss

    for tk in PROXY_TICKERS:
        d15, d1h = _try_yfinance_ticker(tk)
        if _is_valid_feed(d15, d1h):
            d15, d1h, note = _calibrate_proxy(d15, d1h, swiss)
            run_circuit_breaker(d15, d1h)
            return d15, d1h, f"yfinance ({tk}){note}", swiss

    d15, d1h = _try_kraken_paxg()
    if _is_valid_feed(d15, d1h):
        d15, d1h, note = _calibrate_proxy(d15, d1h, swiss)
        run_circuit_breaker(d15, d1h)
        return d15, d1h, f"Kraken PAXGUSD{note}", swiss

    raise CircuitBreakerTripped(
        "All Spot XAUUSD feeds failed (yfinance=X, TV, Dukascopy, PAXG, Kraken).")


# ==============================================================================
# 5-PILLAR FEATURE ENGINE
# ==============================================================================
def compute_atr(df: pd.DataFrame, period: int) -> pd.Series:
    h, l, pc = df["High"], df["Low"], df["Close"].shift(1)
    tr = pd.concat([h - l, (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1.0 / max(period, 1), min_periods=period, adjust=False).mean()
    return atr.ffill().bfill() + EPSILON


def compute_adx(df: pd.DataFrame, period: int = 14) -> pd.Series:
    up, dn = df["High"].diff(), -df["Low"].diff()
    pdm = np.where((up > dn) & (up > 0), up, 0.0)
    ndm = np.where((dn > up) & (dn > 0), dn, 0.0)
    atr = compute_atr(df, period)
    pdi = 100.0 * pd.Series(pdm, index=df.index).ewm(
        alpha=1.0 / period, adjust=False).mean() / (atr + EPSILON)
    ndi = 100.0 * pd.Series(ndm, index=df.index).ewm(
        alpha=1.0 / period, adjust=False).mean() / (atr + EPSILON)
    dx = 100.0 * (pdi - ndi).abs() / (pdi + ndi + EPSILON)
    return dx.ewm(alpha=1.0 / period, adjust=False).mean().fillna(0.0)


def compute_smc_structure(df: pd.DataFrame, lookback: int = 3) -> pd.DataFrame:
    h, l, c = df["High"].values, df["Low"].values, df["Close"].values
    n = len(df)
    swing_h = np.full(n, np.nan)
    swing_l = np.full(n, np.nan)
    bias = np.zeros(n, dtype=int)
    bos_b = np.zeros(n, dtype=bool); bos_s = np.zeros(n, dtype=bool)
    choch_b = np.zeros(n, dtype=bool); choch_s = np.zeros(n, dtype=bool)
    cur_h, cur_l, cur_b = h[0], l[0], 0
    for i in range(lookback * 2, n):
        p = i - lookback
        if h[p] >= np.max(h[p - lookback:p + lookback + 1]):
            cur_h = h[p]
        if l[p] <= np.min(l[p - lookback:p + lookback + 1]):
            cur_l = l[p]
        swing_h[i], swing_l[i] = cur_h, cur_l
        ci, cprev = c[i], c[i - 1]
        bu = ci > cur_h and cprev <= cur_h
        bd = ci < cur_l and cprev >= cur_l
        if bu:
            if cur_b == -1: choch_b[i] = True
            else: bos_b[i] = True
            cur_b = 1
        elif bd:
            if cur_b == 1: choch_s[i] = True
            else: bos_s[i] = True
            cur_b = -1
        elif cur_b == 0:
            cur_b = 1 if ci > (cur_h + cur_l) * 0.5 else -1
        bias[i] = cur_b
    return pd.DataFrame({
        "swing_high": swing_h, "swing_low": swing_l, "smc_bias": bias,
        "bos_bull": bos_b, "bos_bear": bos_s,
        "choch_bull": choch_b, "choch_bear": choch_s,
    }, index=df.index)


def compute_order_flow(df: pd.DataFrame, atr: pd.Series) -> pd.DataFrame:
    rng = (df["High"] - df["Low"]).abs() + EPSILON
    body = df["Close"] - df["Open"]
    abody = body.abs()
    bratio = (abody / rng).clip(0.0, 1.0)
    has_vol = bool((df["Volume"] > 0.0).any())
    if has_vol:
        vs = df["Volume"].replace(0.0, np.nan).ffill().fillna(1.0)
        vma = vs.rolling(20, min_periods=5).mean() + EPSILON
        rv = (vs / vma).clip(0.25, 3.0)
    else:
        rv = (rng / (atr + EPSILON)).clip(0.25, 3.0)
    direction = np.sign(body)
    disp_norm = (abody / (atr + EPSILON)).clip(0.0, 2.5)
    signed = direction * bratio * rv * (0.5 + 0.5 * disp_norm)
    return pd.DataFrame({
        "body_ratio": bratio, "rel_volume": rv,
        "signed_flow": signed,
        "smoothed_flow": signed.ewm(span=3, adjust=False).mean(),
        "has_real_volume": has_vol,
    }, index=df.index)


def enrich_features(df15: pd.DataFrame, df1h: pd.DataFrame,
                    atr_period: int, z_window: int = Z_WINDOW) -> pd.DataFrame:
    df = df15.copy()
    df["ATR"] = compute_atr(df, atr_period)
    df["ADX"] = compute_adx(df, 14)
    atr_rank = df["ATR"].rolling(96, min_periods=20).rank(pct=True).fillna(0.5)
    er = ((df["Close"] - df["Close"].shift(10)).abs()
          / (df["Close"].diff().abs().rolling(10, min_periods=5).sum() + EPSILON)
          ).fillna(0.0)
    df["Efficiency_Ratio"] = er
    df["Regime_Trending"] = (df["ADX"] >= 18.0) & (atr_rank >= 0.20) & (er >= 0.22)

    df = pd.concat([df, compute_smc_structure(df, 3)], axis=1)
    df = pd.concat([df, compute_order_flow(df, df["ATR"])], axis=1)

    rm = df["Close"].rolling(z_window, min_periods=z_window).mean()
    rs = df["Close"].rolling(z_window, min_periods=z_window).std(ddof=0)
    df["Z_Score"] = ((df["Close"] - rm) / (rs + EPSILON)).fillna(0.0)

    htf = df1h.copy()
    htf["HTF_EMA20"] = htf["Close"].ewm(span=20, adjust=False).mean()
    htf["HTF_EMA50"] = htf["Close"].ewm(span=50, adjust=False).mean()
    htf["HTF_ATR"] = compute_atr(htf, atr_period)
    htf_smc = compute_smc_structure(htf, 3)
    htf["HTF_SMC_Bias"] = htf_smc["smc_bias"]
    htf["HTF_Trend"] = np.where(
        (htf["Close"] > htf["HTF_EMA20"]) & (htf["HTF_EMA20"] >= htf["HTF_EMA50"]),
        1, np.where(
            (htf["Close"] < htf["HTF_EMA20"]) & (htf["HTF_EMA20"] <= htf["HTF_EMA50"]),
            -1, htf["HTF_SMC_Bias"]))

    keys = pd.DataFrame({"ts": pd.DatetimeIndex(df.index).as_unit("ns")})
    hframe = htf[["HTF_EMA20", "HTF_EMA50", "HTF_Trend", "HTF_SMC_Bias"]].copy()
    hframe["ts"] = pd.DatetimeIndex(htf.index).as_unit("ns")
    aligned = pd.merge_asof(keys, hframe.sort_values("ts"),
                            on="ts", direction="backward").set_index("ts")
    df["HTF_EMA20"] = aligned["HTF_EMA20"].bfill().values
    df["HTF_EMA50"] = aligned["HTF_EMA50"].bfill().values
    df["HTF_Trend"] = aligned["HTF_Trend"].fillna(0).astype(int).values
    df["HTF_SMC_Bias"] = aligned["HTF_SMC_Bias"].fillna(0).astype(int).values
    return df


def score_bar_confluence(row: pd.Series, z_threshold: float):
    htf = int(row["HTF_Trend"]); smc = int(row["smc_bias"])
    z = float(row["Z_Score"]); flow = float(row["smoothed_flow"])
    br = float(row["body_ratio"]); reg = bool(row["Regime_Trending"])
    bb, cb = bool(row["bos_bull"]), bool(row["choch_bull"])
    bs, cs = bool(row["bos_bear"]), bool(row["choch_bear"])

    bull = (25.0 if htf == 1 else 0.0) + (20.0 if smc == 1 else 0.0) \
        + (10.0 if (bb or cb) else 0.0) \
        + (25.0 if z <= -z_threshold else (15.0 if z <= -(z_threshold * 0.65) else 0.0)) \
        + (20.0 if (flow > 0.15 and br >= 0.45) else (10.0 if flow > 0 else 0.0)) \
        + (10.0 if reg else 0.0)
    bear = (25.0 if htf == -1 else 0.0) + (20.0 if smc == -1 else 0.0) \
        + (10.0 if (bs or cs) else 0.0) \
        + (25.0 if z >= z_threshold else (15.0 if z >= (z_threshold * 0.65) else 0.0)) \
        + (20.0 if (flow < -0.15 and br >= 0.45) else (10.0 if flow < 0 else 0.0)) \
        + (10.0 if reg else 0.0)
    bull, bear = min(bull, 100.0), min(bear, 100.0)
    breakdown = {"bull_score": round(bull, 2), "bear_score": round(bear, 2),
                 "htf_trend": htf, "smc_bias": smc, "z_score": round(z, 4),
                 "smoothed_flow": round(flow, 4), "regime_trending": int(reg)}

    if not reg:
        return "NO SIGNAL", max(bull, bear), breakdown
    if bull >= CONFLUENCE_THRESHOLD and bull > bear and htf >= 0:
        return "VERIFIED BUY", bull, breakdown
    if bear >= CONFLUENCE_THRESHOLD and bear > bull and htf <= 0:
        return "VERIFIED SELL", bear, breakdown
    return "NO SIGNAL", max(bull, bear), breakdown


# ==============================================================================
# GRID SEARCH + BACKTEST
# ==============================================================================
def run_backtest_for_params(df15: pd.DataFrame, df1h: pd.DataFrame,
                            atr_period: int, z_threshold: float) -> BacktestMetrics:
    df = enrich_features(df15, df1h, atr_period, Z_WINDOW)
    o = df["Open"].values; h = df["High"].values; l = df["Low"].values
    atrs = df["ATR"].values; zs = df["Z_Score"].values
    htf = df["HTF_Trend"].values; smc = df["smc_bias"].values
    fl = df["smoothed_flow"].values; br = df["body_ratio"].values
    reg = df["Regime_Trending"].values
    bb = df["bos_bull"].values; cb = df["choch_bull"].values
    bs = df["bos_bear"].values; cs = df["choch_bear"].values
    ts = list(df.index)
    n = len(df)
    eq = ACCOUNT_BALANCE; peak = ACCOUNT_BALANCE; mdd = 0.0
    curve = [eq]; curve_ts = [ts[0]]; rets: List[float] = []
    in_pos = False; direction = 0
    entry = sl = tp1 = tp3 = pos_atr = risk_d = best = 0.0
    tp1_hit = False
    cost_half = SPREAD_DOLLARS * 0.5 + SLIPPAGE_DOLLARS
    start = max(atr_period, Z_WINDOW, 30)

    for i in range(start, n - 1):
        if in_pos:
            if direction == 1:
                best = max(best, h[i])
                if l[i] <= sl:
                    px = sl - SLIPPAGE_DOLLARS
                    r = (px - entry) / (pos_atr + EPSILON)
                    usd = r * risk_d; eq += usd; rets.append(usd); in_pos = False
                elif h[i] >= tp3:
                    px = tp3 - SLIPPAGE_DOLLARS
                    r = (px - entry) / (pos_atr + EPSILON)
                    usd = r * risk_d; eq += usd; rets.append(usd); in_pos = False
                else:
                    if not tp1_hit and h[i] >= tp1:
                        tp1_hit = True; sl = max(sl, entry)
                    if tp1_hit:
                        cand = best - 0.75 * pos_atr
                        sl = max(sl, cand)
            else:
                best = min(best, l[i])
                if h[i] >= sl:
                    px = sl + SLIPPAGE_DOLLARS
                    r = (entry - px) / (pos_atr + EPSILON)
                    usd = r * risk_d; eq += usd; rets.append(usd); in_pos = False
                elif l[i] <= tp3:
                    px = tp3 + SLIPPAGE_DOLLARS
                    r = (entry - px) / (pos_atr + EPSILON)
                    usd = r * risk_d; eq += usd; rets.append(usd); in_pos = False
                else:
                    if not tp1_hit and l[i] <= tp1:
                        tp1_hit = True; sl = min(sl, entry)
                    if tp1_hit:
                        cand = best + 0.75 * pos_atr
                        sl = min(sl, cand)
            peak = max(peak, eq)
            mdd = max(mdd, (peak - eq) / (peak + EPSILON) * 100.0)
            curve.append(eq); curve_ts.append(ts[i]); continue

        curve.append(eq); curve_ts.append(ts[i])
        if not reg[i]:
            continue
        htf_i = int(htf[i]); smc_i = int(smc[i])
        z = float(zs[i]); f = float(fl[i]); b = float(br[i])
        bull = ((25.0 if htf_i == 1 else 0.0) + (20.0 if smc_i == 1 else 0.0)
                + (10.0 if (bb[i] or cb[i]) else 0.0)
                + (25.0 if z <= -z_threshold else
                   (15.0 if z <= -(z_threshold * 0.65) else 0.0))
                + (20.0 if (f > 0.15 and b >= 0.45) else (10.0 if f > 0 else 0.0))
                + 10.0)
        bear = ((25.0 if htf_i == -1 else 0.0) + (20.0 if smc_i == -1 else 0.0)
                + (10.0 if (bs[i] or cs[i]) else 0.0)
                + (25.0 if z >= z_threshold else
                   (15.0 if z >= (z_threshold * 0.65) else 0.0))
                + (20.0 if (f < -0.15 and b >= 0.45) else (10.0 if f < 0 else 0.0))
                + 10.0)
        atr_v = float(atrs[i])
        if atr_v <= EPSILON:
            continue
        nxt = float(o[i + 1])
        if bull >= CONFLUENCE_THRESHOLD and bull > bear and htf_i >= 0:
            in_pos, direction = True, 1
            entry = nxt + cost_half; pos_atr = max(atr_v, 0.5)
            sl = entry - pos_atr; tp1 = entry + 1.5 * pos_atr
            tp3 = entry + 4.0 * pos_atr
            tp1_hit, best = False, entry
            risk_d = eq * (RISK_PERCENTAGE / 100.0)
        elif bear >= CONFLUENCE_THRESHOLD and bear > bull and htf_i <= 0:
            in_pos, direction = True, -1
            entry = nxt - cost_half; pos_atr = max(atr_v, 0.5)
            sl = entry + pos_atr; tp1 = entry - 1.5 * pos_atr
            tp3 = entry - 4.0 * pos_atr
            tp1_hit, best = False, entry
            risk_d = eq * (RISK_PERCENTAGE / 100.0)

    tot = len(rets)
    wins = sum(1 for r in rets if r > 0)
    losses = sum(1 for r in rets if r <= 0)
    gp = sum(r for r in rets if r > 0)
    gl = abs(sum(r for r in rets if r < 0))
    wr = (wins / (tot + EPSILON)) * 100.0 if tot else 0.0
    pf = (gp / (gl + EPSILON)) if gl > EPSILON else (3.5 if gp > 0 else 0.0)
    return BacktestMetrics(
        atr_period=atr_period, z_threshold=z_threshold,
        total_trades=tot, wins=wins, losses=losses,
        win_rate=wr, profit_factor=min(pf, 9.99),
        max_drawdown=mdd,
        net_return_pct=(eq - ACCOUNT_BALANCE) / ACCOUNT_BALANCE * 100.0,
        equity_curve=curve, timestamps=curve_ts)


def run_grid_search(df15: pd.DataFrame, df1h: pd.DataFrame):
    cands: List[BacktestMetrics] = []
    for ap in ATR_GRID:
        for zt in Z_GRID:
            cands.append(run_backtest_for_params(df15, df1h, ap, zt))
    valid = [m for m in cands if m.total_trades >= 5] or cands
    best = max(valid, key=lambda m: (
        round(m.profit_factor, 4), round(m.win_rate, 2),
        -round(m.max_drawdown, 2), m.total_trades))
    print("[GRID SEARCH] Optimization matrix:")
    for m in cands:
        mark = " <-- OPTIMAL" if (m.atr_period == best.atr_period
                                  and m.z_threshold == best.z_threshold) else ""
        print(f"  ATR={m.atr_period:2d} | Z={m.z_threshold:.1f} | "
              f"Trades={m.total_trades:3d} | WR={m.win_rate:5.1f}% | "
              f"PF={m.profit_factor:4.2f} | MaxDD={m.max_drawdown:5.2f}%{mark}")
    return best, cands


# ==============================================================================
# CHART
# ==============================================================================
def generate_backtest_chart(best: BacktestMetrics, feed_source: str,
                            chart_path: str = CHART_PATH) -> str:
    plt.style.use("dark_background")
    fig, (ax1, ax2) = plt.subplots(
        2, 1, figsize=(13, 7.5), gridspec_kw={"height_ratios": [3, 1]},
        sharex=True)
    fig.patch.set_facecolor("#0B0F17")
    ax1.set_facecolor("#111827"); ax2.set_facecolor("#111827")
    ts = pd.to_datetime(best.timestamps)
    eq = np.array(best.equity_curve, dtype=float)
    peak = np.maximum.accumulate(eq)
    dd = -((peak - eq) / (peak + EPSILON)) * 100.0

    ax1.plot(ts, eq, color="#F59E0B", linewidth=2.0, label="Equity (USD)")
    ax1.fill_between(ts, eq, ACCOUNT_BALANCE,
                     where=(eq >= ACCOUNT_BALANCE), color="#10B981", alpha=0.14)
    ax1.fill_between(ts, eq, ACCOUNT_BALANCE,
                     where=(eq < ACCOUNT_BALANCE), color="#EF4444", alpha=0.14)
    ax1.axhline(ACCOUNT_BALANCE, color="#6B7280",
                linestyle="--", linewidth=1.0, alpha=0.7)
    ax1.set_title(f"{ENGINE_NAME} | XAUUSD [{feed_source}] "
                  f"(ATR={best.atr_period}, Z={best.z_threshold:.1f})",
                  fontsize=12.5, fontweight="bold", color="#F9FAFB", pad=12)
    ax1.set_ylabel("Equity ($)", color="#D1D5DB")
    ax1.grid(True, linestyle=":", linewidth=0.6, alpha=0.35, color="#4B5563")
    ax1.legend(loc="upper left", facecolor="#1F2937", edgecolor="#374151")
    stats = (f"Trades: {best.total_trades}\nWin Rate: {best.win_rate:.1f}%\n"
             f"Profit Factor: {best.profit_factor:.2f}\n"
             f"Max DD: {best.max_drawdown:.2f}%\n"
             f"Net: {best.net_return_pct:+.2f}%")
    ax1.text(0.985, 0.05, stats, transform=ax1.transAxes, fontsize=9.5,
             va="bottom", ha="right", color="#F3F4F6",
             bbox=dict(boxstyle="round,pad=0.5", facecolor="#1F2937",
                       edgecolor="#F59E0B", alpha=0.9))
    ax2.fill_between(ts, dd, 0.0, color="#EF4444", alpha=0.45, label="Drawdown (%)")
    ax2.plot(ts, dd, color="#F87171", linewidth=1.1)
    ax2.set_ylabel("Drawdown (%)", color="#D1D5DB")
    ax2.set_xlabel("UTC Time", color="#D1D5DB")
    ax2.grid(True, linestyle=":", linewidth=0.6, alpha=0.35, color="#4B5563")
    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d %H:%M"))
    fig.autofmt_xdate()
    plt.tight_layout()
    plt.savefig(chart_path, dpi=150, facecolor=fig.get_facecolor())
    plt.close(fig)
    return "Generated & Saved as PNG"


# ==============================================================================
# TRAILING STOP + POSITION REGISTRATION
# ==============================================================================
def manage_active_trailing_stops(conn: sqlite3.Connection,
                                 latest_bar: pd.Series,
                                 current_atr: float):
    now = dt.datetime.now(dt.timezone.utc).isoformat()
    hi = float(latest_bar["High"]); lo = float(latest_bar["Low"])
    close = float(latest_bar["Close"])
    cur = conn.cursor()
    cur.execute("""SELECT id, direction, entry_price, current_sl, tp1, tp3,
                          atr_at_entry, tp1_breached, highest_favorable
                   FROM active_positions WHERE status='OPEN'""")
    events = []
    for row in cur.fetchall():
        (pid, direction, entry, sl, tp1, tp3, atr_e, tp1b, fav) = row
        step = max(current_atr, atr_e, 0.5)
        new_sl, new_fav, new_tp1b, status = sl, fav, tp1b, "OPEN"
        state = "Initial Forensic SL"
        if direction == "BUY":
            new_fav = max(fav, hi)
            if lo <= sl:
                status, state = "CLOSED_SL", f"Stopped @ {sl:.2f}"
            elif hi >= tp3:
                status, state = "CLOSED_TP3", f"TP3 Hit @ {tp3:.2f}"
            else:
                if not tp1b and hi >= tp1:
                    new_tp1b = 1; new_sl = max(sl, entry)
                    state = "Active @ TP1 Breakeven"
                if new_tp1b:
                    cand = new_fav - 0.75 * step
                    if cand > new_sl:
                        new_sl = cand
                    state = f"Active Trailing SL @ {new_sl:.2f}"
        else:
            new_fav = min(fav, lo)
            if hi >= sl:
                status, state = "CLOSED_SL", f"Stopped @ {sl:.2f}"
            elif lo <= tp3:
                status, state = "CLOSED_TP3", f"TP3 Hit @ {tp3:.2f}"
            else:
                if not tp1b and lo <= tp1:
                    new_tp1b = 1; new_sl = min(sl, entry)
                    state = "Active @ TP1 Breakeven"
                if new_tp1b:
                    cand = new_fav + 0.75 * step
                    if cand < new_sl:
                        new_sl = cand
                    state = f"Active Trailing SL @ {new_sl:.2f}"
        cur.execute("""UPDATE active_positions SET updated_at_utc=?, current_sl=?,
                       tp1_breached=?, highest_favorable=?, trailing_state=?,
                       status=? WHERE id=?""",
                    (now, round(new_sl, 4), int(new_tp1b),
                     round(new_fav, 4), state, status, pid))
        events.append(f"#{pid} {direction}@{entry:.2f} (Close {close:.2f}): "
                      f"{state} [{status}]")
    conn.commit()
    if events:
        print("[TRAILING] Updates:")
        for e in events:
            print(f"  -> {e}")
        return events[-1], events
    return "Active @ TP1 Breakeven (Armed)", events


def register_new_position(conn: sqlite3.Connection, sig: QuantitativeSignal) -> None:
    if sig.decision not in ("VERIFIED BUY", "VERIFIED SELL"):
        return
    direction = "BUY" if sig.decision == "VERIFIED BUY" else "SELL"
    conn.execute("""INSERT INTO active_positions (opened_at_utc, updated_at_utc,
                    asset, direction, entry_price, initial_sl, current_sl,
                    tp1, tp2, tp3, atr_at_entry, lot_size, tp1_breached,
                    highest_favorable, trailing_state, status)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,0,?,?,'OPEN')""",
                 (sig.timestamp_utc, sig.timestamp_utc, "XAUUSD", direction,
                  round(sig.entry_price, 4), round(sig.stop_loss, 4),
                  round(sig.stop_loss, 4), round(sig.tp1, 4),
                  round(sig.tp2, 4), round(sig.tp3, 4),
                  round(sig.atr_value, 4), round(sig.recommended_lot, 2),
                  round(sig.entry_price, 4), "Active @ TP1 Breakeven"))
    conn.commit()


# ==============================================================================
# LIVE SIGNAL
# ==============================================================================
def evaluate_live_signal(df15: pd.DataFrame, df1h: pd.DataFrame,
                         best: BacktestMetrics, feed_source: str,
                         swiss: Optional[float]):
    enr = enrich_features(df15, df1h, best.atr_period, Z_WINDOW)
    row = enr.iloc[-1]
    decision, conf, breakdown = score_bar_confluence(row, best.z_threshold)
    entry = float(row["Close"])
    atr_v = max(float(row["ATR"]), 0.50)
    z = float(row["Z_Score"])
    regime = "TRENDING" if bool(row["Regime_Trending"]) else "CHOPPY FILTERED"
    smc_lbl = "BULLISH" if int(row["smc_bias"]) == 1 else "BEARISH"
    htf_lbl = ("BULLISH 1H HARMONY" if int(row["HTF_Trend"]) == 1
               else ("BEARISH 1H HARMONY" if int(row["HTF_Trend"]) == -1
                     else "NEUTRAL 1H"))

    sl_d = atr_v
    if decision == "VERIFIED SELL":
        sl = entry + sl_d
        tp1, tp2, tp3 = entry - 1.5 * sl_d, entry - 2.5 * sl_d, entry - 4.0 * sl_d
    else:
        sl = entry - sl_d
        tp1, tp2, tp3 = entry + 1.5 * sl_d, entry + 2.5 * sl_d, entry + 4.0 * sl_d
    rrr = abs(tp3 - entry) / (abs(entry - sl) + EPSILON)
    if decision in ("VERIFIED BUY", "VERIFIED SELL") and rrr < MIN_RRR:
        decision = "NO SIGNAL"

    risk_usd = ACCOUNT_BALANCE * (RISK_PERCENTAGE / 100.0)
    lot = max(0.01, round(risk_usd / (sl_d * XAU_MULT + EPSILON), 2))
    ts = dt.datetime.now(dt.timezone.utc).isoformat()

    sig = QuantitativeSignal(
        decision=decision, entry_price=entry, stop_loss=sl,
        tp1=tp1, tp2=tp2, tp3=tp3, atr_value=atr_v, z_score=z,
        confluence_score=conf, market_regime=regime, smc_bias=smc_lbl,
        order_flow_score=float(row["smoothed_flow"]), htf_alignment=htf_lbl,
        recommended_lot=lot, risk_usd=risk_usd, rrr_tp3=rrr,
        trailing_protocol="Active @ TP1 Breakeven", feed_source=feed_source,
        swissquote_spot_mid=swiss, timestamp_utc=ts,
        pillar_breakdown=breakdown)
    raw = {
        "feed_source": feed_source,
        "swissquote_mid": round(swiss, 4) if swiss else None,
        "latest_bar": str(enr.index[-1]),
        "open": round(float(row["Open"]), 4),
        "high": round(float(row["High"]), 4),
        "low": round(float(row["Low"]), 4),
        "close": round(float(row["Close"]), 4),
        "atr": round(atr_v, 6), "adx": round(float(row["ADX"]), 4),
        "efficiency_ratio": round(float(row["Efficiency_Ratio"]), 4),
        "z_score": round(z, 6),
        "body_ratio": round(float(row["body_ratio"]), 4),
        "rel_volume": round(float(row["rel_volume"]), 4),
        "htf_ema20": round(float(row["HTF_EMA20"]), 4),
        "htf_ema50": round(float(row["HTF_EMA50"]), 4),
        "breakdown": breakdown}
    return sig, raw


# ==============================================================================
# WEBHOOKS
# ==============================================================================
def build_html_report(sig: QuantitativeSignal, best: BacktestMetrics,
                      chart_status: str, db_status: str) -> str:
    return (
        f"🛡️ <b>{ENGINE_NAME} (ULTIMATE EDITION)</b> 🛡️\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🌟 <b>Asset:</b> XAUUSD (SPOT GOLD) [{sig.feed_source}]\n"
        f"📊 <b>Signal Decision:</b> {sig.decision}\n"
        f"🎯 <b>Precision Entry:</b> {sig.entry_price:.2f}\n"
        f"🛑 <b>Forensic Stop Loss:</b> {sig.stop_loss:.2f}\n"
        f"📈 <b>TP1 (1.5R):</b> {sig.tp1:.2f}\n"
        f"📈 <b>TP2 (2.5R):</b> {sig.tp2:.2f}\n"
        f"🚀 <b>TP3 (4.0R):</b> {sig.tp3:.2f}\n"
        f"🔄 <b>Trailing:</b> {sig.trailing_protocol}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚙️ <b>Grid Search &amp; Visualization:</b>\n"
        f"• <i>Optimal ATR:</i> {best.atr_period}\n"
        f"• <i>Optimal Z-Threshold:</i> {best.z_threshold:.1f}\n"
        f"• <i>Backtest Chart:</i> {chart_status}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 <b>Risk &amp; Position Sizing:</b>\n"
        f"• <i>Recommended Lot:</i> {sig.recommended_lot:.2f} ({RISK_PERCENTAGE:.0f}%)\n"
        f"• <i>Risk Amount:</i> ${sig.risk_usd:.2f}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔬 <b>Forensic Audit:</b>\n"
        f"• <i>Market Regime:</i> {sig.market_regime}\n"
        f"• <i>Confluence:</i> {sig.confluence_score:.1f}/100\n"
        f"• <i>SQLite DB:</i> {db_status}\n"
        f"• <i>Error Handler:</i> Active / Nominal\n"
        f"• <i>Win Rate:</i> {best.win_rate:.1f}%\n"
        f"• <i>Profit Factor:</i> {best.profit_factor:.2f}\n"
        f"• <i>Max Drawdown:</i> {best.max_drawdown:.1f}%\n"
        f"• <i>Timestamp (UTC):</i> {sig.timestamp_utc}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━")


def html_to_discord(html: str) -> str:
    return (html.replace("<b>", "**").replace("</b>", "**")
                .replace("<i>", "*").replace("</i>", "*")
                .replace("&amp;", "&"))


def _send_telegram(html_payload: str, chart_path: str) -> str:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return "Skipped (missing creds)"
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": html_payload,
                  "parse_mode": "HTML", "disable_web_page_preview": True},
            timeout=20)
        r.raise_for_status()
        if os.path.exists(chart_path):
            with open(chart_path, "rb") as f:
                pr = requests.post(
                    f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto",
                    data={"chat_id": TELEGRAM_CHAT_ID,
                          "caption": f"{ENGINE_NAME} | XAUUSD Backtest"},
                    files={"photo": f}, timeout=25)
                pr.raise_for_status()
        return "Delivered OK"
    except Exception as e:
        send_telegram_error_alert("Telegram delivery", e)
        return f"Failed: {e}"


def _send_discord(payload: str, chart_path: str) -> str:
    if not DISCORD_WEBHOOK_URL:
        return "Skipped (missing webhook)"
    try:
        if os.path.exists(chart_path):
            with open(chart_path, "rb") as f:
                r = requests.post(DISCORD_WEBHOOK_URL,
                                  data={"content": payload},
                                  files={"file": ("backtest_performance.png",
                                                  f, "image/png")},
                                  timeout=25)
        else:
            r = requests.post(DISCORD_WEBHOOK_URL,
                              json={"content": payload}, timeout=20)
        r.raise_for_status()
        return "Delivered OK"
    except Exception as e:
        send_telegram_error_alert("Discord delivery", e)
        return f"Failed: {e}"


def dispatch_webhooks(sig: QuantitativeSignal, best: BacktestMetrics,
                      chart_status: str, db_status: str) -> Dict[str, str]:
    html_msg = build_html_report(sig, best, chart_status, db_status)
    discord_msg = html_to_discord(html_msg)
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
        f_tg = ex.submit(_send_telegram, html_msg, CHART_PATH)
        f_dc = ex.submit(_send_discord, discord_msg, CHART_PATH)
        result = {"telegram": f_tg.result(), "discord": f_dc.result()}
    print(f"[WEBHOOK] Telegram={result['telegram']} | "
          f"Discord={result['discord']}")
    return result


# ==============================================================================
# MAIN
# ==============================================================================
def main() -> int:
    conn: Optional[sqlite3.Connection] = None
    try:
        print(f"[BOOT] {ENGINE_NAME} initialising...")
        conn = init_sqlite_db(DB_PATH)

        df15, df1h, feed_source, swiss = fetch_spot_gold_data()
        print(f"[DATA] Feed='{feed_source}' | {len(df15)}x15m | "
              f"{len(df1h)}x1h | Close={float(df15['Close'].iloc[-1]):.2f}")

        best, _ = run_grid_search(df15, df1h)
        chart_status = generate_backtest_chart(best, feed_source, CHART_PATH)
        print(f"[CHART] {chart_status} -> {CHART_PATH}")

        sig, raw_math = evaluate_live_signal(df15, df1h, best, feed_source, swiss)

        latest_atr = float(compute_atr(df15, best.atr_period).iloc[-1])
        trail_summary, _ = manage_active_trailing_stops(
            conn, df15.iloc[-1], latest_atr)
        if sig.decision == "NO SIGNAL":
            sig.trailing_protocol = trail_summary

        db_status = forensic_audit_log(conn, sig, best, raw_math)

        if sig.decision in ("VERIFIED BUY", "VERIFIED SELL"):
            register_new_position(conn, sig)
            dispatch_webhooks(sig, best, chart_status, db_status)
        else:
            print(f"[GOVERNOR] Decision={sig.decision} "
                  f"(Regime={sig.market_regime}, "
                  f"Score={sig.confluence_score:.1f}/100). Webhook suppressed.")
        return 0
    except CircuitBreakerTripped as cb:
        send_telegram_error_alert("Circuit Breaker Tripped", cb)
        return 1
    except Exception as exc:
        send_telegram_error_alert("Core Execution", exc)
        return 1
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    sys.exit(main())
