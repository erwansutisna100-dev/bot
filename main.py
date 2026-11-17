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
from requests.adapters import HTTPAdapter
import websocket
import yfinance as yf


# ==============================================================================
# CONFIGURATION
# ==============================================================================
ENGINE_NAME = "APEX-QUANT-OMNI ENTERPRISE v40.0"
TRADINGVIEW_SPOT_SYMBOLS = ["OANDA:XAUUSD", "FX_IDC:XAUUSD", "FOREXCOM:XAUUSD", "PEPPERSTONE:XAUUSD"]
PRIMARY_YF_TICKER = "XAUUSD=X"
SPOT_YFINANCE_PROXIES = ["PAXG-USD", "XAUT-USD"]

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
XAU_CONTRACT_MULTIPLIER = 100.0
MIN_RRR_TARGET = 3.0
CONFLUENCE_THRESHOLD = 65.0

# 3D GRID SEARCH: ATR × Z × Trailing
TRAILING_ATR_MULT = 0.75
TRAILING_ATR_GRID = [0.50, 0.75, 1.00, 1.25]
ATR_GRID = [10, 14, 20]
Z_THRESHOLD_GRID = [1.5, 2.0, 2.5]
Z_LOOKBACK_WINDOW = 20

DUKASCOPY_DAYS_LOOKBACK = 30
DUKASCOPY_CANDLE_FMT = ">5if"
DUKASCOPY_CANDLE_SIZE = struct.calcsize(DUKASCOPY_CANDLE_FMT)
DUKASCOPY_PRICE_DIVISOR = 1000.0

CIRCUIT_BREAKER_MAX_STALE_HOURS = 96.0
CIRCUIT_BREAKER_MAX_BAR_JUMP_PCT = 0.08


class CircuitBreakerTripped(RuntimeError):
    pass


@dataclass
class BacktestMetrics:
    atr_period: int
    z_threshold: float
    trailing_mult: float
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
    safe_context = context.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    safe_err = f"{type(exc).__name__}: {exc}".replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
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
        print("[ERROR HANDLER] Telegram credentials not configured.", file=sys.stderr)
        return False
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML",
                  "disable_web_page_preview": True}, timeout=15)
        resp.raise_for_status()
        return True
    except Exception as dispatch_err:
        print(f"[ERROR HANDLER] Failed to dispatch: {dispatch_err}", file=sys.stderr)
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
            timestamp_utc TEXT NOT NULL, asset TEXT NOT NULL,
            feed_source TEXT NOT NULL DEFAULT 'UNKNOWN',
            decision TEXT NOT NULL, entry_price REAL NOT NULL,
            stop_loss REAL NOT NULL, tp1 REAL NOT NULL, tp2 REAL NOT NULL, tp3 REAL NOT NULL,
            trailing_protocol TEXT NOT NULL, optimal_atr_period INTEGER NOT NULL,
            optimal_z_threshold REAL NOT NULL, recommended_lot REAL NOT NULL,
            risk_usd REAL NOT NULL, market_regime TEXT NOT NULL,
            confluence_score REAL NOT NULL, z_score REAL NOT NULL,
            atr_value REAL NOT NULL, smc_bias TEXT NOT NULL,
            order_flow_score REAL NOT NULL, htf_alignment TEXT NOT NULL,
            win_rate REAL NOT NULL, profit_factor REAL NOT NULL,
            max_drawdown REAL NOT NULL, raw_forensic_json TEXT NOT NULL
        );
    """)
    existing_cols = {row[1] for row in conn.execute("PRAGMA table_info(signal_audit_log);").fetchall()}
    if "feed_source" not in existing_cols:
        conn.execute("ALTER TABLE signal_audit_log ADD COLUMN feed_source TEXT NOT NULL DEFAULT 'UNKNOWN';")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS active_positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            opened_at_utc TEXT NOT NULL, updated_at_utc TEXT NOT NULL,
            asset TEXT NOT NULL, direction TEXT NOT NULL,
            entry_price REAL NOT NULL, initial_sl REAL NOT NULL,
            current_sl REAL NOT NULL, tp1 REAL NOT NULL, tp2 REAL NOT NULL, tp3 REAL NOT NULL,
            atr_at_entry REAL NOT NULL, lot_size REAL NOT NULL,
            tp1_breached INTEGER NOT NULL DEFAULT 0,
            highest_favorable REAL NOT NULL, trailing_state TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'OPEN',
            trailing_multiplier REAL NOT NULL DEFAULT 0.75
        );
    """)
    ap_cols = {row[1] for row in conn.execute("PRAGMA table_info(active_positions);").fetchall()}
    if "trailing_multiplier" not in ap_cols:
        conn.execute("ALTER TABLE active_positions ADD COLUMN trailing_multiplier REAL NOT NULL DEFAULT 0.75;")
    conn.commit()
    return conn


def forensic_audit_log(conn, signal, best_metrics, raw_math) -> str:
    raw_payload_json = json.dumps(raw_math, default=str)
    sq_str = f"{signal.swissquote_spot_mid:.2f}" if signal.swissquote_spot_mid else "N/A"
    print("\n" + "=" * 78)
    print(f"FORENSIC AUDIT REPORT | {ENGINE_NAME} | {signal.timestamp_utc}")
    print("=" * 78)
    print(f"Asset                : XAUUSD (SPOT GOLD) [{signal.feed_source}]")
    print(f"MT5 Spot Benchmark   : Swissquote Live BBO Mid = {sq_str}")
    print(f"Signal Decision      : {signal.decision}")
    print(f"Market Regime        : {signal.market_regime}")
    print(f"Confluence Score     : {signal.confluence_score:.2f} / 100.00")
    print(f"Precision Entry      : {signal.entry_price:.2f}")
    print(f"Forensic Stop Loss   : {signal.stop_loss:.2f}")
    print(f"TP1 (1.5R)           : {signal.tp1:.2f}")
    print(f"TP2 (2.5R)           : {signal.tp2:.2f}")
    print(f"TP3 (4.0R)           : {signal.tp3:.2f} (RRR 1:{signal.rrr_tp3:.2f})")
    print(f"Trailing Protocol    : {signal.trailing_protocol} (Step {best_metrics.trailing_mult:.2f}×ATR)")
    print(f"Optimal Grid Params  : ATR={best_metrics.atr_period} | Z-Thresh={best_metrics.z_threshold:.1f} | Trail={best_metrics.trailing_mult:.2f}×")
    print(f"Backtest Stats       : Trades={best_metrics.total_trades} | WR={best_metrics.win_rate:.2f}% | PF={best_metrics.profit_factor:.2f} | MaxDD={best_metrics.max_drawdown:.2f}%")
    print(f"Lot & Risk (1%)      : {signal.recommended_lot:.2f} Lots | ${signal.risk_usd:.2f}")
    print(f"Raw Math Pillars     : {raw_payload_json}")
    print("=" * 78 + "\n")
    try:
        conn.execute("""
            INSERT INTO signal_audit_log (
                timestamp_utc, asset, feed_source, decision, entry_price, stop_loss,
                tp1, tp2, tp3, trailing_protocol, optimal_atr_period,
                optimal_z_threshold, recommended_lot, risk_usd, market_regime,
                confluence_score, z_score, atr_value, smc_bias, order_flow_score,
                htf_alignment, win_rate, profit_factor, max_drawdown, raw_forensic_json
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
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
            round(best_metrics.max_drawdown, 2), raw_payload_json,
        ))
        conn.commit()
        return "Logged Successfully"
    except Exception as db_exc:
        send_telegram_error_alert("SQLite forensic_audit_log Persistence", db_exc)
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


def clean_yfinance_dataframe(df):
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.copy()
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    return normalize_ohlcv_dataframe(df)


def fetch_swissquote_spot_mid() -> Optional[float]:
    try:
        r = requests.get(
            "https://forex-data-feed.swissquote.com/public-quotes/bboquotes/instrument/XAU/USD",
            headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        r.raise_for_status()
        data = r.json()
        if not isinstance(data, list):
            return None
        for node in data:
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


def _tv_parse_frames(raw_msg: str) -> List[str]:
    frames, idx, total = [], 0, len(raw_msg)
    while idx < total:
        if not raw_msg.startswith("~m~", idx):
            break
        sep = raw_msg.find("~m~", idx + 3)
        if sep == -1:
            break
        try:
            flen = int(raw_msg[idx + 3:sep])
        except ValueError:
            break
        start = sep + 3
        frames.append(raw_msg[start:start + flen])
        idx = start + flen
    return frames


def _fetch_tradingview_series(symbol, resolution, n_bars):
    ws_url = "wss://data.tradingview.com/socket.io/websocket?from=chart%2F"
    sess = "cs_" + "".join(random.choices(string.ascii_lowercase + string.digits, k=12))
    bars = {}
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
        _tv_send(ws, "create_series", [sess, "sds_1", "s1", "sds_sym_1", resolution, n_bars])
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
                m, p = pkt.get("m"), pkt.get("p", [])
                if m in ("symbol_error", "series_error"):
                    return pd.DataFrame()
                if m in ("timescale_update", "du"):
                    for item in p:
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
                                    bars[ts] = (float(v[1]), float(v[2]), float(v[3]), float(v[4]), vol)
                if m == "series_completed" and len(bars) >= 60:
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
             "Open": v[0], "High": v[1], "Low": v[2], "Close": v[3], "Volume": v[4]}
            for ts, v in sorted(bars.items())]
    return normalize_ohlcv_dataframe(pd.DataFrame(rows).set_index("Timestamp"))


def _try_tradingview_spot_gold():
    for sym in TRADINGVIEW_SPOT_SYMBOLS:
        try:
            print(f"[DATA] Querying TradingView Spot XAUUSD ({sym})...")
            d15 = _fetch_tradingview_series(sym, "15", 1500)
            d1h = _fetch_tradingview_series(sym, "60", 1000)
            if _is_valid_feed(d15, d1h):
                return d15, d1h, f"TradingView Spot ({sym})"
        except Exception as e:
            print(f"[DATA] TradingView {sym} attempt failed: {e}")
    return pd.DataFrame(), pd.DataFrame(), ""


def _fetch_dukascopy_day_candles(session, day):
    url = (f"https://datafeed.dukascopy.com/datafeed/XAUUSD/"
           f"{day.year}/{day.month - 1:02d}/{day.day:02d}/BID_candles_min_1.bi5")
    try:
        r = session.get(url, timeout=10)
        if r.status_code != 200 or not r.content:
            return []
        raw = lzma.decompress(r.content)
        base = pd.Timestamp(day.year, day.month, day.day, tz="UTC")
        usable = len(raw) - (len(raw) % DUKASCOPY_CANDLE_SIZE)
        rows = []
        for off in range(0, usable, DUKASCOPY_CANDLE_SIZE):
            sec, o, c, l, h, vol = struct.unpack(DUKASCOPY_CANDLE_FMT, raw[off:off + DUKASCOPY_CANDLE_SIZE])
            o_px, h_px = o / DUKASCOPY_PRICE_DIVISOR, h / DUKASCOPY_PRICE_DIVISOR
            l_px, c_px = l / DUKASCOPY_PRICE_DIVISOR, c / DUKASCOPY_PRICE_DIVISOR
            if c_px <= 0:
                continue
            rows.append((base + pd.Timedelta(seconds=int(sec)), o_px, h_px, l_px, c_px, float(vol)))
        return rows
    except Exception:
        return []


def _try_dukascopy_spot_gold():
    print("[DATA] Querying Dukascopy institutional Spot XAUUSD (1m daily .bi5)...")
    today = dt.datetime.now(dt.timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    days = [today - dt.timedelta(days=d) for d in range(DUKASCOPY_DAYS_LOOKBACK, -1, -1)]
    session = requests.Session()
    session.mount("https://", HTTPAdapter(pool_connections=16, pool_maxsize=16))
    session.headers.update({"User-Agent": "Mozilla/5.0"})
    all_m1 = []
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
            for chunk in pool.map(lambda d: _fetch_dukascopy_day_candles(session, d), days):
                if chunk:
                    all_m1.extend(chunk)
    finally:
        session.close()
    if len(all_m1) < 1800:
        return pd.DataFrame(), pd.DataFrame()
    m1 = pd.DataFrame(all_m1, columns=["Timestamp", "Open", "High", "Low", "Close", "Volume"])
    m1["Timestamp"] = pd.to_datetime(m1["Timestamp"], utc=True)
    m1 = m1.set_index("Timestamp").sort_index()
    agg = {"Open": "first", "High": "max", "Low": "min", "Close": "last", "Volume": "sum"}
    return (normalize_ohlcv_dataframe(m1.resample("15min").agg(agg).dropna(subset=["Open", "Close"])),
            normalize_ohlcv_dataframe(m1.resample("1h").agg(agg).dropna(subset=["Open", "Close"])))


def _try_yfinance_ticker(ticker):
    try:
        raw15 = yf.download(ticker, period="60d", interval="15m", auto_adjust=False, progress=False, threads=False)
        raw1h = yf.download(ticker, period="180d", interval="1h", auto_adjust=False, progress=False, threads=False)
        return clean_yfinance_dataframe(raw15), clean_yfinance_dataframe(raw1h)
    except Exception as e:
        print(f"[DATA] yfinance {ticker} failed: {e}")
        return pd.DataFrame(), pd.DataFrame()


def _try_kraken_spot_gold():
    print("[DATA] Querying Kraken public Spot Gold proxy (PAXGUSD)...")
    url = "https://api.kraken.com/0/public/OHLC"

    def _pull(interval):
        r = requests.get(url, params={"pair": "PAXGUSD", "interval": interval}, timeout=15)
        r.raise_for_status()
        pl = r.json()
        if pl.get("error"):
            raise RuntimeError(pl["error"])
        keys = [k for k in pl.get("result", {}) if k != "last"]
        if not keys:
            return pd.DataFrame()
        df = pd.DataFrame(pl["result"][keys[0]],
                          columns=["Time", "Open", "High", "Low", "Close", "VWAP", "Volume", "Count"])
        df["Time"] = pd.to_datetime(df["Time"], unit="s", utc=True)
        return normalize_ohlcv_dataframe(df.set_index("Time")[["Open", "High", "Low", "Close", "Volume"]])
    try:
        return _pull(15), _pull(60)
    except Exception as e:
        print(f"[DATA] Kraken failed: {e}")
        return pd.DataFrame(), pd.DataFrame()


def _calibrate_proxy_to_spot(d15, d1h, swiss):
    if swiss is None or d15.empty:
        return d15, d1h, ""
    delta = swiss - float(d15["Close"].iloc[-1])
    if abs(delta) <= 0.05:
        return d15, d1h, ""
    a, b = d15.copy(), d1h.copy()
    for col in ["Open", "High", "Low", "Close"]:
        a[col] = a[col] + delta
        b[col] = b[col] + delta
    return a, b, f" + Basis Calibrated {delta:+.2f}"


def _is_valid_feed(d15, d1h):
    if d15.empty or d1h.empty or len(d15) < 120 or len(d1h) < 60:
        return False
    last = d15.index[-1].to_pydatetime()
    hours = (dt.datetime.now(dt.timezone.utc) - last).total_seconds() / 3600.0
    return hours <= CIRCUIT_BREAKER_MAX_STALE_HOURS


def fetch_spot_gold_data():
    swiss = fetch_swissquote_spot_mid()
    if swiss:
        print(f"[DATA] Swissquote spot mid: {swiss:.2f}")

    print(f"[DATA] Tier-1: yfinance {PRIMARY_YF_TICKER}")
    d15, d1h = _try_yfinance_ticker(PRIMARY_YF_TICKER)
    if _is_valid_feed(d15, d1h):
        run_circuit_breaker(d15, d1h)
        return d15, d1h, f"yfinance ({PRIMARY_YF_TICKER})", swiss

    d15, d1h, src = _try_tradingview_spot_gold()
    if _is_valid_feed(d15, d1h):
        run_circuit_breaker(d15, d1h)
        return d15, d1h, src, swiss

    d15, d1h = _try_dukascopy_spot_gold()
    if _is_valid_feed(d15, d1h):
        run_circuit_breaker(d15, d1h)
        return d15, d1h, "Dukascopy Spot XAUUSD", swiss

    for tk in SPOT_YFINANCE_PROXIES:
        d15, d1h = _try_yfinance_ticker(tk)
        if _is_valid_feed(d15, d1h):
            d15, d1h, note = _calibrate_proxy_to_spot(d15, d1h, swiss)
            run_circuit_breaker(d15, d1h)
            return d15, d1h, f"yfinance ({tk}){note}", swiss

    d15, d1h = _try_kraken_spot_gold()
    if _is_valid_feed(d15, d1h):
        d15, d1h, note = _calibrate_proxy_to_spot(d15, d1h, swiss)
        run_circuit_breaker(d15, d1h)
        return d15, d1h, f"Kraken PAXGUSD{note}", swiss

    raise CircuitBreakerTripped("All Spot XAUUSD feeds failed.")


def run_circuit_breaker(d15, d1h):
    if d15.empty or len(d15) < 120:
        raise CircuitBreakerTripped(f"Insufficient 15m bars: {len(d15)} (need >=120)")
    if d1h.empty or len(d1h) < 60:
        raise CircuitBreakerTripped(f"Insufficient 1h bars: {len(d1h)} (need >=60)")
    last = d15.index[-1].to_pydatetime()
    hours = (dt.datetime.now(dt.timezone.utc) - last).total_seconds() / 3600.0
    if hours > CIRCUIT_BREAKER_MAX_STALE_HOURS:
        raise CircuitBreakerTripped(f"Stale data {hours:.1f}h")
    px = float(d15["Close"].iloc[-1])
    if not math.isfinite(px) or not (500.0 < px < 15000.0):
        raise CircuitBreakerTripped(f"Implausible price: {px}")
    pct = d15["Close"].pct_change().dropna().abs()
    if not pct.empty and float(pct.iloc[-1]) > CIRCUIT_BREAKER_MAX_BAR_JUMP_PCT:
        raise CircuitBreakerTripped(f"Anomalous spike {float(pct.iloc[-1])*100:.2f}%")


# ==============================================================================
# FEATURE ENGINE
# ==============================================================================
def compute_atr(df, period):
    h, l, pc = df["High"], df["Low"], df["Close"].shift(1)
    tr = pd.concat([h - l, (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1.0 / max(period, 1), min_periods=period, adjust=False).mean()
    return atr.ffill().bfill() + EPSILON


def compute_adx(df, period=14):
    up, dn = df["High"].diff(), -df["Low"].diff()
    pdm = np.where((up > dn) & (up > 0), up, 0.0)
    ndm = np.where((dn > up) & (dn > 0), dn, 0.0)
    atr = compute_atr(df, period)
    pdi = 100.0 * pd.Series(pdm, index=df.index).ewm(alpha=1.0 / period, adjust=False).mean() / (atr + EPSILON)
    ndi = 100.0 * pd.Series(ndm, index=df.index).ewm(alpha=1.0 / period, adjust=False).mean() / (atr + EPSILON)
    dx = 100.0 * (pdi - ndi).abs() / (pdi + ndi + EPSILON)
    return dx.ewm(alpha=1.0 / period, adjust=False).mean().fillna(0.0)


def compute_smc_structure(df, lookback=3):
    h, l, c = df["High"].values, df["Low"].values, df["Close"].values
    n = len(df)
    sh, sl = np.full(n, np.nan), np.full(n, np.nan)
    bias = np.zeros(n, dtype=int)
    bb, bs = np.zeros(n, dtype=bool), np.zeros(n, dtype=bool)
    cb, cs = np.zeros(n, dtype=bool), np.zeros(n, dtype=bool)
    ch, cl, cur_b = h[0], l[0], 0
    for i in range(lookback * 2, n):
        p = i - lookback
        if h[p] >= np.max(h[p - lookback:p + lookback + 1]):
            ch = h[p]
        if l[p] <= np.min(l[p - lookback:p + lookback + 1]):
            cl = l[p]
        sh[i], sl[i] = ch, cl
        ci, cp = c[i], c[i - 1]
        bu = ci > ch and cp <= ch
        bd = ci < cl and cp >= cl
        if bu:
            if cur_b == -1:
                cb[i] = True
            else:
                bb[i] = True
            cur_b = 1
        elif bd:
            if cur_b == 1:
                cs[i] = True
            else:
                bs[i] = True
            cur_b = -1
        elif cur_b == 0:
            cur_b = 1 if ci > (ch + cl) * 0.5 else -1
        bias[i] = cur_b
    return pd.DataFrame({"swing_high": sh, "swing_low": sl, "smc_bias": bias,
                         "bos_bull": bb, "bos_bear": bs, "choch_bull": cb, "choch_bear": cs}, index=df.index)


def compute_order_flow_features(df, atr):
    rng = (df["High"] - df["Low"]).abs() + EPSILON
    body = df["Close"] - df["Open"]
    ab = body.abs()
    br = (ab / rng).clip(0.0, 1.0)
    has_vol = bool((df["Volume"] > 0.0).any())
    if has_vol:
        vs = df["Volume"].replace(0.0, np.nan).ffill().fillna(1.0)
        vma = vs.rolling(20, min_periods=5).mean() + EPSILON
        rv = (vs / vma).clip(0.25, 3.0)
    else:
        rv = (rng / (atr + EPSILON)).clip(0.25, 3.0)
    disp = (ab / (atr + EPSILON)).clip(0.0, 2.5)
    signed = np.sign(body) * br * rv * (0.5 + 0.5 * disp)
    return pd.DataFrame({"body_ratio": br, "rel_volume": rv, "signed_flow": signed,
                         "smoothed_flow": signed.ewm(span=3, adjust=False).mean(),
                         "has_real_volume": has_vol}, index=df.index)


def enrich_features(df15, df1h, atr_period, z_window=Z_LOOKBACK_WINDOW):
    df = df15.copy()
    df["ATR"] = compute_atr(df, atr_period)
    df["ADX"] = compute_adx(df, 14)
    atr_rank = df["ATR"].rolling(96, min_periods=20).rank(pct=True).fillna(0.5)
    er = ((df["Close"] - df["Close"].shift(10)).abs()
          / (df["Close"].diff().abs().rolling(10, min_periods=5).sum() + EPSILON)).fillna(0.0)
    df["Efficiency_Ratio"] = er
    df["Regime_Trending"] = (df["ADX"] >= 18.0) & (atr_rank >= 0.20) & (er >= 0.22)
    df = pd.concat([df, compute_smc_structure(df, 3)], axis=1)
    df = pd.concat([df, compute_order_flow_features(df, df["ATR"])], axis=1)
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
        (htf["Close"] > htf["HTF_EMA20"]) & (htf["HTF_EMA20"] >= htf["HTF_EMA50"]), 1,
        np.where((htf["Close"] < htf["HTF_EMA20"]) & (htf["HTF_EMA20"] <= htf["HTF_EMA50"]), -1,
                 htf["HTF_SMC_Bias"]))
    keys = pd.DataFrame({"ts": pd.DatetimeIndex(df.index).as_unit("ns")})
    hf = htf[["HTF_EMA20", "HTF_EMA50", "HTF_Trend", "HTF_SMC_Bias"]].copy()
    hf["ts"] = pd.DatetimeIndex(htf.index).as_unit("ns")
    aligned = pd.merge_asof(keys, hf.sort_values("ts"), on="ts", direction="backward").set_index("ts")
    df["HTF_EMA20"] = aligned["HTF_EMA20"].bfill().values
    df["HTF_EMA50"] = aligned["HTF_EMA50"].bfill().values
    df["HTF_Trend"] = aligned["HTF_Trend"].fillna(0).astype(int).values
    df["HTF_SMC_Bias"] = aligned["HTF_SMC_Bias"].fillna(0).astype(int).values
    return df


def score_bar_confluence(row, z_threshold):
    htf = int(row["HTF_Trend"]); smc = int(row["smc_bias"])
    z = float(row["Z_Score"]); flow = float(row["smoothed_flow"])
    br = float(row["body_ratio"]); reg = bool(row["Regime_Trending"])
    bull = 0.0
    if htf == 1: bull += 25.0
    if smc == 1: bull += 20.0
    if bool(row["bos_bull"]) or bool(row["choch_bull"]): bull += 10.0
    if z <= -z_threshold: bull += 25.0
    elif z <= -(z_threshold * 0.65): bull += 15.0
    if flow > 0.15 and br >= 0.45: bull += 20.0
    elif flow > 0.0: bull += 10.0
    if reg: bull += 10.0
    bear = 0.0
    if htf == -1: bear += 25.0
    if smc == -1: bear += 20.0
    if bool(row["bos_bear"]) or bool(row["choch_bear"]): bear += 10.0
    if z >= z_threshold: bear += 25.0
    elif z >= (z_threshold * 0.65): bear += 15.0
    if flow < -0.15 and br >= 0.45: bear += 20.0
    elif flow < 0.0: bear += 10.0
    if reg: bear += 10.0
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
# BACKTEST ENGINE (param-driven trail)
# ==============================================================================
def run_backtest_for_params(df15, df1h, atr_period, z_threshold, trailing_mult=0.75):
    df = enrich_features(df15, df1h, atr_period, Z_LOOKBACK_WINDOW)
    o = df["Open"].values; h = df["High"].values; l = df["Low"].values
    atrs = df["ATR"].values; zs = df["Z_Score"].values
    htf = df["HTF_Trend"].values; smc = df["smc_bias"].values
    fl = df["smoothed_flow"].values; br = df["body_ratio"].values
    reg = df["Regime_Trending"].values
    bb = df["bos_bull"].values; cb = df["choch_bull"].values
    bs = df["bos_bear"].values; cs = df["choch_bear"].values
    ts = list(df.index); n = len(df)
    eq = ACCOUNT_BALANCE; peak = ACCOUNT_BALANCE; mdd = 0.0
    curve = [eq]; curve_ts = [ts[0]]; rets = []
    in_pos = False; direction = 0
    entry = sl = tp1 = tp3 = pos_atr = risk_d = best = 0.0
    tp1_hit = False
    cost_half = SPREAD_DOLLARS * 0.5 + SLIPPAGE_DOLLARS
    start = max(atr_period, Z_LOOKBACK_WINDOW, 30)
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
                        sl = max(sl, best - (trailing_mult * pos_atr))
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
                        sl = min(sl, best + (trailing_mult * pos_atr))
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
                + (25.0 if z <= -z_threshold else (15.0 if z <= -(z_threshold * 0.65) else 0.0))
                + (20.0 if (f > 0.15 and b >= 0.45) else (10.0 if f > 0 else 0.0))
                + 10.0)
        bear = ((25.0 if htf_i == -1 else 0.0) + (20.0 if smc_i == -1 else 0.0)
                + (10.0 if (bs[i] or cs[i]) else 0.0)
                + (25.0 if z >= z_threshold else (15.0 if z >= (z_threshold * 0.65) else 0.0))
                + (20.0 if (f < -0.15 and b >= 0.45) else (10.0 if f < 0 else 0.0))
                + 10.0)
        atr_v = float(atrs[i])
        if atr_v <= EPSILON:
            continue
        nxt = float(o[i + 1])
        if bull >= CONFLUENCE_THRESHOLD and bull > bear and htf_i >= 0:
            in_pos, direction = True, 1
            entry = nxt + cost_half; pos_atr = max(atr_v, 0.5)
            sl = entry - pos_atr; tp1 = entry + 1.5 * pos_atr; tp3 = entry + 4.0 * pos_atr
            tp1_hit, best = False, entry
            risk_d = eq * (RISK_PERCENTAGE / 100.0)
        elif bear >= CONFLUENCE_THRESHOLD and bear > bull and htf_i <= 0:
            in_pos, direction = True, -1
            entry = nxt - cost_half; pos_atr = max(atr_v, 0.5)
            sl = entry + pos_atr; tp1 = entry - 1.5 * pos_atr; tp3 = entry - 4.0 * pos_atr
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
        atr_period=atr_period, z_threshold=z_threshold, trailing_mult=trailing_mult,
        total_trades=tot, wins=wins, losses=losses, win_rate=wr,
        profit_factor=min(pf, 9.99), max_drawdown=mdd,
        net_return_pct=(eq - ACCOUNT_BALANCE) / ACCOUNT_BALANCE * 100.0,
        equity_curve=curve, timestamps=curve_ts, trade_returns=rets)


def run_grid_search_optimization(df15, df1h):
    cands = []
    for ap in ATR_GRID:
        for zt in Z_THRESHOLD_GRID:
            for tm in TRAILING_ATR_GRID:
                cands.append(run_backtest_for_params(df15, df1h, ap, zt, tm))
    valid = [m for m in cands if m.total_trades >= 5] or cands
    best = max(valid, key=lambda m: (round(m.profit_factor, 4), round(m.win_rate, 2),
                                      -round(m.max_drawdown, 2), m.total_trades))
    print(f"[GRID SEARCH] {len(cands)} combos evaluated (ATR×Z×Trail). Top 5:")
    for m in sorted(cands, key=lambda x: -x.profit_factor)[:5]:
        mark = " <-- OPTIMAL" if (m.atr_period == best.atr_period and m.z_threshold == best.z_threshold
                                   and m.trailing_mult == best.trailing_mult) else ""
        print(f"  ATR={m.atr_period:2d} | Z={m.z_threshold:.1f} | Trail={m.trailing_mult:.2f}x "
              f"| Trades={m.total_trades:3d} | WR={m.win_rate:5.1f}% "
              f"| PF={m.profit_factor:4.2f} | MaxDD={m.max_drawdown:5.2f}%{mark}")
    return best, cands


# ==============================================================================
# CHART
# ==============================================================================
def generate_backtest_chart(best, feed_source, chart_path=CHART_PATH):
    plt.style.use("dark_background")
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(13, 7.5),
                                    gridspec_kw={"height_ratios": [3, 1]}, sharex=True)
    fig.patch.set_facecolor("#0B0F17")
    ax1.set_facecolor("#111827"); ax2.set_facecolor("#111827")
    ts = pd.to_datetime(best.timestamps)
    eq = np.array(best.equity_curve, dtype=float)
    peak = np.maximum.accumulate(eq)
    dd = -((peak - eq) / (peak + EPSILON)) * 100.0
    ax1.plot(ts, eq, color="#F59E0B", linewidth=2.0, label="Equity (USD)")
    ax1.fill_between(ts, eq, ACCOUNT_BALANCE, where=(eq >= ACCOUNT_BALANCE), color="#10B981", alpha=0.14)
    ax1.fill_between(ts, eq, ACCOUNT_BALANCE, where=(eq < ACCOUNT_BALANCE), color="#EF4444", alpha=0.14)
    ax1.axhline(ACCOUNT_BALANCE, color="#6B7280", linestyle="--", linewidth=1.0, alpha=0.7)
    ax1.set_title(f"{ENGINE_NAME} | XAUUSD [{feed_source}] (ATR={best.atr_period}, Z={best.z_threshold:.1f}, Trail={best.trailing_mult:.2f}x)",
                  fontsize=12.5, fontweight="bold", color="#F9FAFB", pad=12)
    ax1.set_ylabel("Equity ($)", fontsize=10, color="#D1D5DB")
    ax1.grid(True, linestyle=":", linewidth=0.6, alpha=0.35, color="#4B5563")
    ax1.legend(loc="upper left", facecolor="#1F2937", edgecolor="#374151")
    stats = (f"Trades: {best.total_trades}\nWin Rate: {best.win_rate:.1f}%\n"
             f"Profit Factor: {best.profit_factor:.2f}\nMax Drawdown: {best.max_drawdown:.2f}%\n"
             f"Net Return: {best.net_return_pct:+.2f}%")
    ax1.text(0.985, 0.05, stats, transform=ax1.transAxes, fontsize=9.5, va="bottom", ha="right",
             color="#F3F4F6", bbox=dict(boxstyle="round,pad=0.5", facecolor="#1F2937",
                                         edgecolor="#F59E0B", alpha=0.9))
    ax2.fill_between(ts, dd, 0.0, color="#EF4444", alpha=0.45, label="Drawdown (%)")
    ax2.plot(ts, dd, color="#F87171", linewidth=1.1)
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
# TRAILING ENGINE (per-position trail)
# ==============================================================================
def manage_active_trailing_stops(conn, latest_bar, current_atr):
    now = dt.datetime.now(dt.timezone.utc).isoformat()
    hi = float(latest_bar["High"]); lo = float(latest_bar["Low"]); close = float(latest_bar["Close"])
    cur = conn.cursor()
    cur.execute("""SELECT id, direction, entry_price, current_sl, tp1, tp3,
                          atr_at_entry, tp1_breached, highest_favorable, trailing_multiplier
                   FROM active_positions WHERE status='OPEN'""")
    events = []
    for row in cur.fetchall():
        (pid, direction, entry, sl, tp1, tp3, atr_e, tp1b, fav, trail_mult) = row
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
                    cand = new_fav - (trail_mult * step)
                    if cand > new_sl:
                        new_sl = cand
                    state = f"Trailing ({trail_mult:.2f}x) SL @ {new_sl:.2f}"
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
                    cand = new_fav + (trail_mult * step)
                    if cand < new_sl:
                        new_sl = cand
                    state = f"Trailing ({trail_mult:.2f}x) SL @ {new_sl:.2f}"
        cur.execute("""UPDATE active_positions SET updated_at_utc=?, current_sl=?,
                       tp1_breached=?, highest_favorable=?, trailing_state=?, status=?
                       WHERE id=?""",
                    (now, round(new_sl, 4), int(new_tp1b), round(new_fav, 4), state, status, pid))
        events.append(f"#{pid} {direction}@{entry:.2f} (Close {close:.2f}): {state} [{status}]")
    conn.commit()
    if events:
        print("[TRAILING ENGINE] Position Updates:")
        for e in events:
            print(f"  -> {e}")
        return events[-1], events
    return "Active @ TP1 Breakeven (Armed)", events


def register_new_position(conn, signal, trailing_multiplier):
    if signal.decision not in ("VERIFIED BUY", "VERIFIED SELL"):
        return
    direction = "BUY" if signal.decision == "VERIFIED BUY" else "SELL"
    conn.execute("""INSERT INTO active_positions (
        opened_at_utc, updated_at_utc, asset, direction, entry_price,
        initial_sl, current_sl, tp1, tp2, tp3, atr_at_entry, lot_size,
        tp1_breached, highest_favorable, trailing_state, status, trailing_multiplier
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,0,?,?,'OPEN',?)""",
                 (signal.timestamp_utc, signal.timestamp_utc, "XAUUSD", direction,
                  round(signal.entry_price, 4), round(signal.stop_loss, 4),
                  round(signal.stop_loss, 4), round(signal.tp1, 4), round(signal.tp2, 4),
                  round(signal.tp3, 4), round(signal.atr_value, 4),
                  round(signal.recommended_lot, 2), round(signal.entry_price, 4),
                  "Active @ TP1 Breakeven", round(trailing_multiplier, 4)))
    conn.commit()


# ==============================================================================
# LIVE SIGNAL
# ==============================================================================
def evaluate_live_signal(df15, df1h, best, feed_source, swiss):
    enr = enrich_features(df15, df1h, best.atr_period, Z_LOOKBACK_WINDOW)
    row = enr.iloc[-1]
    decision, conf, breakdown = score_bar_confluence(row, best.z_threshold)
    entry = float(row["Close"])
    atr_v = max(float(row["ATR"]), 0.50)
    z = float(row["Z_Score"])
    regime = "TRENDING" if bool(row["Regime_Trending"]) else "CHOPPY FILTERED"
    smc_lbl = "BULLISH" if int(row["smc_bias"]) == 1 else "BEARISH"
    htf_lbl = ("BULLISH 1H HARMONY" if int(row["HTF_Trend"]) == 1
               else ("BEARISH 1H HARMONY" if int(row["HTF_Trend"]) == -1 else "NEUTRAL 1H"))
    sl_d = atr_v
    if decision == "VERIFIED SELL":
        sl = entry + sl_d
        tp1, tp2, tp3 = entry - 1.5 * sl_d, entry - 2.5 * sl_d, entry - 4.0 * sl_d
    else:
        sl = entry - sl_d
        tp1, tp2, tp3 = entry + 1.5 * sl_d, entry + 2.5 * sl_d, entry + 4.0 * sl_d
    rrr = abs(tp3 - entry) / (abs(entry - sl) + EPSILON)
    if decision in ("VERIFIED BUY", "VERIFIED SELL") and rrr < MIN_RRR_TARGET:
        decision = "NO SIGNAL"
    risk_usd = ACCOUNT_BALANCE * (RISK_PERCENTAGE / 100.0)
    lot = max(0.01, round(risk_usd / (sl_d * XAU_CONTRACT_MULTIPLIER + EPSILON), 2))
    ts = dt.datetime.now(dt.timezone.utc).isoformat()
    sig = QuantitativeSignal(
        decision=decision, entry_price=entry, stop_loss=sl,
        tp1=tp1, tp2=tp2, tp3=tp3, atr_value=atr_v, z_score=z,
        confluence_score=conf, market_regime=regime, smc_bias=smc_lbl,
        order_flow_score=float(row["smoothed_flow"]), htf_alignment=htf_lbl,
        recommended_lot=lot, risk_usd=risk_usd, rrr_tp3=rrr,
        trailing_protocol="Active @ TP1 Breakeven", feed_source=feed_source,
        swissquote_spot_mid=swiss, timestamp_utc=ts, pillar_breakdown=breakdown)
    raw = {"feed_source": feed_source,
           "swissquote_spot_mid": round(swiss, 4) if swiss else None,
           "latest_bar_time": str(enr.index[-1]),
           "open": round(float(row["Open"]), 4), "high": round(float(row["High"]), 4),
           "low": round(float(row["Low"]), 4), "close": round(float(row["Close"]), 4),
           "atr": round(atr_v, 6), "adx": round(float(row["ADX"]), 4),
           "efficiency_ratio": round(float(row["Efficiency_Ratio"]), 4),
           "z_score": round(z, 6), "body_ratio": round(float(row["body_ratio"]), 4),
           "rel_volume": round(float(row["rel_volume"]), 4),
           "has_real_volume": bool(row["has_real_volume"]),
           "htf_ema20": round(float(row["HTF_EMA20"]), 4),
           "htf_ema50": round(float(row["HTF_EMA50"]), 4),
           "trailing_atr_mult": best.trailing_mult, "breakdown": breakdown}
    return sig, raw


# ==============================================================================
# WEBHOOKS
# ==============================================================================
def build_forensic_report_html(sig, best, chart_status, db_status):
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
        f"🔄 <b>Trailing Stop:</b> {sig.trailing_protocol} ({best.trailing_mult:.2f}×ATR)\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚙️ <b>Grid Search &amp; Visualization:</b>\n"
        f"• <i>Optimal ATR:</i> {best.atr_period}\n"
        f"• <i>Optimal Z-Threshold:</i> {best.z_threshold:.1f}\n"
        f"• <i>Optimal Trail:</i> {best.trailing_mult:.2f}×ATR\n"
        f"• <i>Backtest Chart:</i> {chart_status}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 <b>Risk &amp; Position Sizing:</b>\n"
        f"• <i>Recommended Lot:</i> {sig.recommended_lot:.2f} ({RISK_PERCENTAGE:.0f}% Risk)\n"
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


def html_to_discord(html):
    return (html.replace("<b>", "**").replace("</b>", "**")
            .replace("<i>", "*").replace("</i>", "*").replace("&amp;", "&"))


def _send_telegram(html_payload, chart_path):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return "Skipped (missing creds)"
    try:
        r = requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                          json={"chat_id": TELEGRAM_CHAT_ID, "text": html_payload,
                                "parse_mode": "HTML", "disable_web_page_preview": True}, timeout=20)
        r.raise_for_status()
        if os.path.exists(chart_path):
            with open(chart_path, "rb") as f:
                pr = requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto",
                                   data={"chat_id": TELEGRAM_CHAT_ID,
                                         "caption": f"{ENGINE_NAME} | XAUUSD Backtest"},
                                   files={"photo": f}, timeout=25)
                pr.raise_for_status()
        return "Delivered OK"
    except Exception as e:
        send_telegram_error_alert("Telegram delivery", e)
        return f"Failed: {e}"


def _send_discord(payload, chart_path):
    if not DISCORD_WEBHOOK_URL:
        return "Skipped (missing webhook)"
    try:
        if os.path.exists(chart_path):
            with open(chart_path, "rb") as f:
                r = requests.post(DISCORD_WEBHOOK_URL, data={"content": payload},
                                  files={"file": ("backtest_performance.png", f, "image/png")}, timeout=25)
        else:
            r = requests.post(DISCORD_WEBHOOK_URL, json={"content": payload}, timeout=20)
        r.raise_for_status()
        return "Delivered OK"
    except Exception as e:
        send_telegram_error_alert("Discord delivery", e)
        return f"Failed: {e}"


def dispatch_dual_channel_webhooks(sig, best, chart_status, db_status):
    html_msg = build_forensic_report_html(sig, best, chart_status, db_status)
    discord_msg = html_to_discord(html_msg)
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
        f_tg = ex.submit(_send_telegram, html_msg, CHART_PATH)
        f_dc = ex.submit(_send_discord, discord_msg, CHART_PATH)
        result = {"telegram": f_tg.result(), "discord": f_dc.result()}
    print(f"[WEBHOOK] Telegram={result['telegram']} | Discord={result['discord']}")
    return result


# ==============================================================================
# MAIN
# ==============================================================================
def main() -> int:
    conn = None
    try:
        print(f"[BOOT] {ENGINE_NAME} initialising...")
        print(f"[CONFIG] 3D Grid: ATR×Z×Trail (default={TRAILING_ATR_MULT:.2f}, grid={TRAILING_ATR_GRID})")
        conn = init_sqlite_db(DB_PATH)
        df15, df1h, feed_source, swiss = fetch_spot_gold_data()
        print(f"[DATA] Feed='{feed_source}' | {len(df15)}x15m | {len(df1h)}x1h | Close={float(df15['Close'].iloc[-1]):.2f}")
        best, _ = run_grid_search_optimization(df15, df1h)
        chart_status = generate_backtest_chart(best, feed_source, CHART_PATH)
        print(f"[CHART] {chart_status} -> {CHART_PATH}")
        sig, raw_math = evaluate_live_signal(df15, df1h, best, feed_source, swiss)
        latest_atr = float(compute_atr(df15, best.atr_period).iloc[-1])
        trail_summary, _ = manage_active_trailing_stops(conn, df15.iloc[-1], latest_atr)
        if sig.decision == "NO SIGNAL":
            sig.trailing_protocol = trail_summary
        db_status = forensic_audit_log(conn, sig, best, raw_math)
        if sig.decision in ("VERIFIED BUY", "VERIFIED SELL"):
            register_new_position(conn, sig, best.trailing_mult)
            dispatch_dual_channel_webhooks(sig, best, chart_status, db_status)
        else:
            print(f"[GOVERNOR] Decision={sig.decision} (Regime={sig.market_regime}, Score={sig.confluence_score:.1f}/100). Webhook suppressed.")
        return 0
    except CircuitBreakerTripped as cb:
        send_telegram_error_alert("Circuit Breaker Tripped", cb)
        return 1
    except Exception as exc:
        send_telegram_error_alert("APEX-QUANT-OMNI Core Execution", exc)
        return 1
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    sys.exit(main())
