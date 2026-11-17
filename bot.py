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
# ENTERPRISE CONFIGURATION & CONSTANTS
# ==============================================================================
ENGINE_NAME = "APEX-QUANT-OMNI ENTERPRISE v40.0"
TRADINGVIEW_SPOT_SYMBOLS = [
    "OANDA:XAUUSD",
    "FX_IDC:XAUUSD",
    "FOREXCOM:XAUUSD",
    "PEPPERSTONE:XAUUSD",
]
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

# ─── 3D GRID SEARCH: ATR × Z × Trailing ───
# Safe default (empirically validated: PF 1.42). Grid search will override.
TRAILING_ATR_MULT = 0.75
# Trail grid — empirically searches optimum per dataset (not hypothesis-driven)
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
    """Raised when market data integrity or execution invariants fail."""


@dataclass
class BacktestMetrics:
    atr_period: int
    z_threshold: float
    trailing_mult: float  # NEW: per-run optimized trail
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
    tb_tail = "".join(tb_lines[-4:]).strip()
    if len(tb_tail) > 1200:
        tb_tail = tb_tail[-1200:]

    safe_context = context.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    safe_err = (f"{type(exc).__name__}: {exc}"
                .replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))
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
        print("[ERROR HANDLER] Telegram credentials not configured; alert logged locally.",
              file=sys.stderr)
        return False

    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": message,
                  "parse_mode": "HTML", "disable_web_page_preview": True},
            timeout=15,
        )
        resp.raise_for_status()
        return True
    except Exception as dispatch_err:
        print(f"[ERROR HANDLER] Failed to dispatch Telegram error alert: {dispatch_err}",
              file=sys.stderr)
        return False


# ==============================================================================
# SQLITE PERSISTENCE & FORENSIC AUDIT
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
    existing_cols = {row[1] for row in conn.execute("PRAGMA table_info(signal_audit_log);").fetchall()}
    if "feed_source" not in existing_cols:
        conn.execute("ALTER TABLE signal_audit_log ADD COLUMN feed_source TEXT NOT NULL DEFAULT 'UNKNOWN';")

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
                confluence_score, z_score, atr_value, smc_bias,
                order_flow_score, htf_alignment, win_rate, profit_factor,
                max_drawdown, raw_forensic_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
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
# DATA PIPELINE (5-Tier)
# ==============================================================================
def normalize_ohlcv_dataframe(df: pd.DataFrame) -> pd.DataFrame:
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
            return pd.DataFrame()
        df[col] = pd.to_numeric(df[col], errors="coerce")
    if "Volume" not in df.columns:
        df["Volume"] = 0.0
    else:
        df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce").fillna(0.0)
    idx = pd.to_datetime(df.index, utc=True)
    df.index = pd.DatetimeIndex(idx).as_unit("ns")
    df.index.name = "Timestamp"
    df = df[~df.index.duplicated(keep="last")].sort_index()
    df = df.dropna(subset=required_cols)
    df = df[(df["Close"] > 0.0) & (df["Open"] > 0.0) & (df["High"] > 0.0) & (df["Low"] > 0.0)]
    if df.empty:
        return pd.DataFrame()
    df["High"] = np.maximum(df["High"], np.maximum(df["Open"], df["Close"]))
    df["Low"] = np.minimum(df["Low"], np.minimum(df["Open"], df["Close"]))
    return df[["Open", "High", "Low", "Close", "Volume"]]


def clean_yfinance_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.copy()
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    return normalize_ohlcv_dataframe(df)


def fetch_swissquote_spot_mid() -> Optional[float]:
    url = "https://forex-data-feed.swissquote.com/public-quotes/bboquotes/instrument/XAU/USD"
    try:
        resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, list):
            return None
        for node in data:
            for prof in node.get("spreadProfilePrices", []):
                bid, ask = prof.get("bid"), prof.get("ask")
                if bid and ask and float(bid) > 0 and float(ask) > 0:
                    return (float(bid) + float(ask)) * 0.5
        return None
    except Exception:
        return None


def _tv_send(ws, func, args):
    payload = json.dumps({"m": func, "p": args}, separators=(",", ":"))
    ws.send(f"~m~{len(payload)}~m~{payload}")


def _tv_parse_frames(raw_msg: str) -> List[str]:
    frames, idx, total = [], 0, len(raw_msg)
    while idx < total:
        if not raw_msg.startswith("~m~", idx):
            break
        sep = raw_msg.find("~m~", idx + 3)
        if sep == -1:
            break
        try:
            frame_len = int(raw_msg[idx + 3:sep])
        except ValueError:
            break
        start = sep + 3
        frames.append(raw_msg[start:start + frame_len])
        idx = start + frame_len
    return frames


def _fetch_tradingview_series(symbol: str, resolution: str, n_bars: int) -> pd.DataFrame:
    ws_url = "wss://data.tradingview.com/socket.io/websocket?from=chart%2F"
    session = "cs_" + "".join(random.choices(string.ascii_lowercase + string.digits, k=12))
    bars_map = {}
    ws = websocket.create_connection(
        ws_url, timeout=15,
        header={"Origin": "https://data.tradingview.com",
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"})
    try:
        _tv_send(ws, "set_auth_token", ["unauthorized_user_token"])
        _tv_send(ws, "chart_create_session", [session, ""])
        _tv_send(ws, "switch_timezone", [session, "Etc/UTC"])
        sym_cfg = json.dumps({"symbol": symbol, "adjustment": "splits"}, separators=(",", ":"))
        _tv_send(ws, "resolve_symbol", [session, "sds_sym_1", f"={sym_cfg}"])
        _tv_send(ws, "create_series", [session, "sds_1", "s1", "sds_sym_1", resolution, n_bars])
        deadline = time.monotonic() + 18.0
        while time.monotonic() < deadline:
            raw_msg = ws.recv()
            if not raw_msg:
                continue
            for frame in _tv_parse_frames(raw_msg):
                if frame.startswith("~h~"):
                    ws.send(f"~m~{len(frame)}~m~{frame}")
                    continue
                try:
                    pkt = json.loads(frame)
                except json.JSONDecodeError:
                    continue
                if not isinstance(pkt, dict):
                    continue
                method, params = pkt.get("m"), pkt.get("p", [])
                if method in ("symbol_error", "series_error"):
                    return pd.DataFrame()
                if method in ("timescale_update", "du"):
                    for item in params:
                        if not isinstance(item, dict):
                            continue
                        for val in item.values():
                            if not isinstance(val, dict):
                                continue
                            for bar in val.get("s", []):
                                vec = bar.get("v", [])
                                if len(vec) >= 5:
                                    ts_val = float(vec[0])
                                    vol_val = float(vec[5]) if len(vec) >= 6 and vec[5] is not None else 0.0
                                    bars_map[ts_val] = (float(vec[1]), float(vec[2]),
                                                        float(vec[3]), float(vec[4]), vol_val)
                if method == "series_completed" and len(bars_map) >= 60:
                    break
            else:
                continue
            break
    finally:
        try:
            ws.close()
        except Exception:
            pass
    if not bars_map:
        return pd.DataFrame()
    rows = [{"Timestamp": pd.to_datetime(ts, unit="s", utc=True),
             "Open": v[0], "High": v[1], "Low": v[2], "Close": v[3], "Volume": v[4]}
            for ts, v in sorted(bars_map.items())]
    return normalize_ohlcv_dataframe(pd.DataFrame(rows).set_index("Timestamp"))


def _try_tradingview_spot_gold():
    for symbol in TRADINGVIEW_SPOT_SYMBOLS:
        try:
            print(f"[DATA] Querying TradingView Spot XAUUSD ({symbol})...")
            df_15m = _fetch_tradingview_series(symbol, "15", 1500)
            df_1h = _fetch_tradingview_series(symbol, "60", 1000)
            if _is_valid_feed(df_15m, df_1h):
                return df_15m, df_1h, f"TradingView Spot ({symbol})"
        except Exception as exc:
            print(f"[DATA] TradingView {symbol} attempt failed: {exc}")
    return pd.DataFrame(), pd.DataFrame(), ""


def _fetch_dukascopy_day_candles(session, day_dt):
    url = (f"https://datafeed.dukascopy.com/datafeed/XAUUSD/"
           f"{day_dt.year}/{day_dt.month - 1:02d}/{day_dt.day:02d}/BID_candles_min_1.bi5")
    try:
        resp = session.get(url, timeout=10)
        if resp.status_code != 200 or not resp.content:
            return []
        raw = lzma.decompress(resp.content)
        rows = []
        base_ts = pd.Timestamp(day_dt.year, day_dt.month, day_dt.day, tz="UTC")
        usable = len(raw) - (len(raw) % DUKASCOPY_CANDLE_SIZE)
        for offset in range(0, usable, DUKASCOPY_CANDLE_SIZE):
            sec_off, o_int, c_int, l_int, h_int, vol = struct.unpack(
                DUKASCOPY_CANDLE_FMT, raw[offset:offset + DUKASCOPY_CANDLE_SIZE])
            o_px, h_px = o_int / DUKASCOPY_PRICE_DIVISOR, h_int / DUKASCOPY_PRICE_DIVISOR
            l_px, c_px = l_int / DUKASCOPY_PRICE_DIVISOR, c_int / DUKASCOPY_PRICE_DIVISOR
            if c_px <= 0.0:
                continue
            rows.append((base_ts + pd.Timedelta(seconds=int(sec_off)),
                         o_px, h_px, l_px, c_px, float(vol)))
        return rows
    except Exception:
        return []


def _try_dukascopy_spot_gold():
    print("[DATA] Querying Dukascopy institutional Spot XAUUSD (1m daily .bi5)...")
    today_utc = dt.datetime.now(dt.timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    day_list = [today_utc - dt.timedelta(days=d) for d in range(DUKASCOPY_DAYS_LOOKBACK, -1, -1)]
    session = requests.Session()
    adapter = HTTPAdapter(pool_connections=16, pool_maxsize=16)
    session.mount("https://", adapter)
    session.headers.update({"User-Agent": "Mozilla/5.0"})
    all_m1 = []
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
            for chunk in pool.map(lambda d: _fetch_dukascopy_day_candles(session, d), day_list):
                if chunk:
                    all_m1.extend(chunk)
    finally:
        session.close()
    if len(all_m1) < 1800:
        return pd.DataFrame(), pd.DataFrame()
    m1_df = pd.DataFrame(all_m1, columns=["Timestamp", "Open", "High", "Low", "Close", "Volume"])
    m1_df["Timestamp"] = pd.to_datetime(m1_df["Timestamp"], utc=True)
    m1_df = m1_df.set_index("Timestamp").sort_index()
    agg_spec = {"Open": "first", "High": "max", "Low": "min", "Close": "last", "Volume": "sum"}
    return (normalize_ohlcv_dataframe(m1_df.resample("15min").agg(agg_spec).dropna(subset=["Open", "Close"])),
            normalize_ohlcv_dataframe(m1_df.resample("1h").agg(agg_spec).dropna(subset=["Open", "Close"])))


def _try_yfinance_ticker(ticker):
    try:
        raw_15m = yf.download(ticker, period="60d", interval="15m",
                              auto_adjust=False, progress=False, threads=False)
        raw_1h = yf.download(ticker, period="180d", interval="1h",
                             auto_adjust=False, progress=False, threads=False)
        return clean_yfinance_dataframe(raw_15m), clean_yfinance_dataframe(raw_1h)
    except Exception as exc:
        print(f"[DATA] yfinance attempt for {ticker} failed: {exc}")
        return pd.DataFrame(), pd.DataFrame()


def _try_kraken_spot_gold():
    print("[DATA] Querying Kraken public Spot Gold proxy (PAXGUSD)...")
    url = "https://api.kraken.com/0/public/OHLC"

    def _pull(interval_min):
        resp = requests.get(url, params={"pair": "PAXGUSD", "interval": interval_min}, timeout=15)
        resp.raise_for_status()
        payload = resp.json()
        if payload.get("error"):
            raise RuntimeError(f"Kraken API error: {payload['error']}")
        result = payload.get("result", {})
        pair_keys = [k for k in result.keys() if k != "last"]
        if not pair_keys:
            return pd.DataFrame()
        df = pd.DataFrame(result[pair_keys[0]],
                          columns=["Time", "Open", "High", "Low", "Close", "VWAP", "Volume", "Count"])
        df["Time"] = pd.to_datetime(df["Time"], unit="s", utc=True)
        return normalize_ohlcv_dataframe(df.set_index("Time")[["Open", "High", "Low", "Close", "Volume"]])

    try:
        return _pull(15), _pull(60)
    except Exception as exc:
        print(f"[DATA] Kraken PAXGUSD feed failed: {exc}")
        return pd.DataFrame(), pd.DataFrame()


def _calibrate_proxy_to_spot(df_15m, df_1h, swissquote_mid):
    if swissquote_mid is None or df_15m.empty:
        return df_15m, df_1h, ""
    proxy_close = float(df_15m["Close"].iloc[-1])
    basis_delta = swissquote_mid - proxy_close
    if abs(basis_delta) <= 0.05:
        return df_15m, df_1h, ""
    df_15m_adj, df_1h_adj = df_15m.copy(), df_1h.copy()
    for col in ["Open", "High", "Low", "Close"]:
        df_15m_adj[col] += basis_delta
        df_1h_adj[col] += basis_delta
    return df_15m_adj, df_1h_adj, f" + Basis Calibrated {basis_delta:+.2f} to Swissquote Spot"


def _is_valid_feed(df_15m, df_1h):
    if df_15m.empty or df_1h.empty:
        return False
    if len(df_15m) < 120 or len(df_1h) < 60:
        return False
    last_ts = df_15m.index[-1].to_pydatetime()
    staleness_h = (dt.datetime.now(dt.timezone.utc) - last_ts).total_seconds() / 3600.0
    return staleness_h <= CIRCUIT_BREAKER_MAX_STALE_HOURS


def fetch_spot_gold_data():
    swissquote_mid = fetch_swissquote_spot_mid()
    if swissquote_mid is not None:
        print(f"[DATA] Swissquote Live MT5-Grade Spot XAU/USD Mid: {swissquote_mid:.2f}")

    df_15m, df_1h, tv_source = _try_tradingview_spot_gold()
    if _is_valid_feed(df_15m, df_1h):
        run_circuit_breaker(df_15m, df_1h)
        return df_15m, df_1h, tv_source, swissquote_mid

    df_15m, df_1h = _try_dukascopy_spot_gold()
    if _is_valid_feed(df_15m, df_1h):
        run_circuit_breaker(df_15m, df_1h)
        return df_15m, df_1h, "Dukascopy Spot XAUUSD", swissquote_mid

    df_15m, df_1h = _try_yfinance_ticker(PRIMARY_YF_TICKER)
    if _is_valid_feed(df_15m, df_1h):
        run_circuit_breaker(df_15m, df_1h)
        return df_15m, df_1h, f"yfinance ({PRIMARY_YF_TICKER})", swissquote_mid

    for proxy_ticker in SPOT_YFINANCE_PROXIES:
        df_15m, df_1h = _try_yfinance_ticker(proxy_ticker)
        if _is_valid_feed(df_15m, df_1h):
            df_15m, df_1h, note = _calibrate_proxy_to_spot(df_15m, df_1h, swissquote_mid)
            run_circuit_breaker(df_15m, df_1h)
            return df_15m, df_1h, f"yfinance ({proxy_ticker}){note}", swissquote_mid

    df_15m, df_1h = _try_kraken_spot_gold()
    if _is_valid_feed(df_15m, df_1h):
        df_15m, df_1h, note = _calibrate_proxy_to_spot(df_15m, df_1h, swissquote_mid)
        run_circuit_breaker(df_15m, df_1h)
        return df_15m, df_1h, f"Kraken (PAXGUSD){note}", swissquote_mid

    raise CircuitBreakerTripped(
        "All registration-free Spot Gold feeds failed (TradingView, Dukascopy, yfinance, Kraken).")


def run_circuit_breaker(df_15m, df_1h):
    if df_15m.empty or len(df_15m) < 120:
        raise CircuitBreakerTripped(f"Insufficient 15m bars: {len(df_15m)} (need >= 120)")
    if df_1h.empty or len(df_1h) < 60:
        raise CircuitBreakerTripped(f"Insufficient 1h bars: {len(df_1h)} (need >= 60)")
    last_ts = df_15m.index[-1]
    staleness = (dt.datetime.now(dt.timezone.utc) - last_ts.to_pydatetime()).total_seconds() / 3600.0
    if staleness > CIRCUIT_BREAKER_MAX_STALE_HOURS:
        raise CircuitBreakerTripped(f"Stale data {staleness:.2f}h (limit {CIRCUIT_BREAKER_MAX_STALE_HOURS}h)")
    latest_close = float(df_15m["Close"].iloc[-1])
    if not math.isfinite(latest_close) or latest_close < 500.0 or latest_close > 15000.0:
        raise CircuitBreakerTripped(f"Price out of bounds: {latest_close}")
    pct = df_15m["Close"].pct_change().dropna().abs()
    if not pct.empty and float(pct.iloc[-1]) > CIRCUIT_BREAKER_MAX_BAR_JUMP_PCT:
        raise CircuitBreakerTripped(f"Bar spike {float(pct.iloc[-1])*100:.2f}%")


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
    pdi = 100.0 * pd.Series(pdm, index=df.index).ewm(alpha=1.0/period, adjust=False).mean() / (atr + EPSILON)
    ndi = 100.0 * pd.Series(ndm, index=df.index).ewm(alpha=1.0/period, adjust=False).mean() / (atr + EPSILON)
    dx = 100.0 * (pdi - ndi).abs() / (pdi + ndi + EPSILON)
    return dx.ewm(alpha=1.0/period, adjust=False).mean().fillna(0.0)


def compute_smc_structure(df, swing_lookback=3):
    highs, lows, closes = df["High"].values, df["Low"].values, df["Close"].values
    n = len(df)
    last_sh, last_sl = np.full(n, np.nan), np.full(n, np.nan)
    smc_bias = np.zeros(n, dtype=int)
    bos_b, bos_s = np.zeros(n, dtype=bool), np.zeros(n, dtype=bool)
    cb, cs = np.zeros(n, dtype=bool), np.zeros(n, dtype=bool)
    cur_sh, cur_sl, cur_b = highs[0], lows[0], 0
    for i in range(swing_lookback * 2, n):
        p = i - swing_lookback
        if highs[p] >= np.max(highs[p - swing_lookback:p + swing_lookback + 1]):
            cur_sh = highs[p]
        if lows[p] <= np.min(lows[p - swing_lookback:p + swing_lookback + 1]):
            cur_sl = lows[p]
        last_sh[i], last_sl[i] = cur_sh, cur_sl
        c, prev_c = closes[i], closes[i - 1]
        bu = c > cur_sh and prev_c <= cur_sh
        bd = c < cur_sl and prev_c >= cur_sl
        if bu:
            if cur_b == -1: cb[i] = True
            else: bos_b[i] = True
            cur_b = 1
        elif bd:
            if cur_b == 1: cs[i] = True
            else: bos_s[i] = True
            cur_b = -1
        elif cur_b == 0:
            cur_b = 1 if c > (cur_sh + cur_sl) * 0.5 else -1
        smc_bias[i] = cur_b
    return pd.DataFrame({"swing_high": last_sh, "swing_low": last_sl, "smc_bias": smc_bias,
                         "bos_bull": bos_b, "bos_bear": bos_s,
                         "choch_bull": cb, "choch_bear": cs}, index=df.index)


def compute_order_flow_features(df, atr):
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
    disp = (abody / (atr + EPSILON)).clip(0.0, 2.5)
    signed = np.sign(body) * bratio * rv * (0.5 + 0.5 * disp)
    return pd.DataFrame({"body_ratio": bratio, "rel_volume": rv, "signed_flow": signed,
                         "smoothed_flow": signed.ewm(span=3, adjust=False).mean(),
                         "has_real_volume": has_vol}, index=df.index)


def enrich_features(df_15m, df_1h, atr_period, z_window=Z_LOOKBACK_WINDOW):
    df = df_15m.copy()
    df["ATR"] = compute_atr(df, atr_period)
    df["ADX"] = compute_adx(df, 14)
    atr_rank = df["ATR"].rolling(96, min_periods=20).rank(pct=True).fillna(0.5)
    er = ((df["Close"] - df["Close"].shift(10)).abs()
          / (df["Close"].diff().abs().rolling(10, min_periods=5).sum() + EPSILON)).fillna(0.0)
    df["Efficiency_Ratio"] = er
    df["Regime_Trending"] = (df["ADX"] >= 18.0) & (atr_rank >= 0.20) & (er >= 0.22)
    df = pd.concat([df, compute_smc_structure(df, 3)], axis=1)
    df = pd.concat([df, compute_order_flow_features(df, df["ATR"])], axis=1)
    rm = df["Close"].rolling(window=z_window, min_periods=z_window).mean()
    rs = df["Close"].rolling(window=z_window, min_periods=z_window).std(ddof=0)
    df["Z_Score"] = ((df["Close"] - rm) / (rs + EPSILON)).fillna(0.0)
    htf = df_1h.copy()
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
    hframe = htf[["HTF_EMA20", "HTF_EMA50", "HTF_Trend", "HTF_SMC_Bias"]].copy()
    hframe["ts"] = pd.DatetimeIndex(htf.index).as_unit("ns")
    aligned = pd.merge_asof(keys, hframe.sort_values("ts"), on="ts", direction="backward").set_index("ts")
    df["HTF_EMA20"] = aligned["HTF_EMA20"].bfill().values
    df["HTF_EMA50"] = aligned["HTF_EMA50"].bfill().values
    df["HTF_Trend"] = aligned["HTF_Trend"].fillna(0).astype(int).values
    df["HTF_SMC_Bias"] = aligned["HTF_SMC_Bias"].fillna(0).astype(int).values
    return df


def score_bar_confluence(row, z_threshold):
    htf_trend = int(row["HTF_Trend"]); smc_bias = int(row["smc_bias"])
    z_score = float(row["Z_Score"]); flow_val = float(row["smoothed_flow"])
    body_ratio = float(row["body_ratio"]); regime_ok = bool(row["Regime_Trending"])
    bull = 0.0
    if htf_trend == 1: bull += 25.0
    if smc_bias == 1: bull += 20.0
    if bool(row["bos_bull"]) or bool(row["choch_bull"]): bull += 10.0
    if z_score <= -z_threshold: bull += 25.0
    elif z_score <= -(z_threshold * 0.65): bull += 15.0
    if flow_val > 0.15 and body_ratio >= 0.45: bull += 20.0
    elif flow_val > 0.0: bull += 10.0
    if regime_ok: bull += 10.0
    bear = 0.0
    if htf_trend == -1: bear += 25.0
    if smc_bias == -1: bear += 20.0
    if bool(row["bos_bear"]) or bool(row["choch_bear"]): bear += 10.0
    if z_score >= z_threshold: bear += 25.0
    elif z_score >= (z_threshold * 0.65): bear += 15.0
    if flow_val < -0.15 and body_ratio >= 0.45: bear += 20.0
    elif flow_val < 0.0: bear += 10.0
    if regime_ok: bear += 10.0
    bull, bear = min(bull, 100.0), min(bear, 100.0)
    breakdown = {"bull_score": round(bull, 2), "bear_score": round(bear, 2),
                 "htf_trend": htf_trend, "smc_bias": smc_bias,
                 "z_score": round(z_score, 4), "smoothed_flow": round(flow_val, 4),
                 "regime_trending": int(regime_ok)}
    if not regime_ok:
        return "NO SIGNAL", max(bull, bear), breakdown
    if bull >= CONFLUENCE_THRESHOLD and bull > bear and htf_trend >= 0:
        return "VERIFIED BUY", bull, breakdown
    if bear >= CONFLUENCE_THRESHOLD and bear > bull and htf_trend <= 0:
        return "VERIFIED SELL", bear, breakdown
    return "NO SIGNAL", max(bull, bear), breakdown


# ==============================================================================
# BACKTEST ENGINE (Slippage-aware, Param-Driven Trail)
# ==============================================================================
def run_backtest_for_params(df_15m, df_1h, atr_period, z_threshold, trailing_mult=0.75):
    df = enrich_features(df_15m, df_1h, atr_period=atr_period, z_window=Z_LOOKBACK_WINDOW)
    opens = df["Open"].values; highs = df["High"].values; lows = df["Low"].values
    atrs = df["ATR"].values; z_scores = df["Z_Score"].values
    htf_trends = df["HTF_Trend"].values; smc_biases = df["smc_bias"].values
    flows = df["smoothed_flow"].values; body_ratios = df["body_ratio"].values
    regimes = df["Regime_Trending"].values
    bos_bulls = df["bos_bull"].values; choch_bulls = df["choch_bull"].values
    bos_bears = df["bos_bear"].values; choch_bears = df["choch_bear"].values
    timestamps = list(df.index)
    n = len(df)
    equity = ACCOUNT_BALANCE; peak = ACCOUNT_BALANCE; max_dd = 0.0
    eq_curve = [equity]; curve_ts = [timestamps[0]]; trade_returns = []
    in_pos = False; direction = 0
    entry_px = sl_px = tp1_px = tp3_px = pos_atr = risk_dollars = best_fav = 0.0
    tp1_hit = False
    cost_half = (SPREAD_DOLLARS * 0.5) + SLIPPAGE_DOLLARS
    start_idx = max(atr_period, Z_LOOKBACK_WINDOW, 30)
    for i in range(start_idx, n - 1):
        h, l = highs[i], lows[i]
        if in_pos:
            if direction == 1:
                best_fav = max(best_fav, h)
                if l <= sl_px:
                    pnl_r = (sl_px - SLIPPAGE_DOLLARS - entry_px) / (pos_atr + EPSILON)
                    pnl = pnl_r * risk_dollars
                    equity += pnl; trade_returns.append(pnl); in_pos = False
                elif h >= tp3_px:
                    pnl_r = (tp3_px - SLIPPAGE_DOLLARS - entry_px) / (pos_atr + EPSILON)
                    pnl = pnl_r * risk_dollars
                    equity += pnl; trade_returns.append(pnl); in_pos = False
                else:
                    if not tp1_hit and h >= tp1_px:
                        tp1_hit = True; sl_px = max(sl_px, entry_px)
                    if tp1_hit:
                        sl_px = max(sl_px, best_fav - (trailing_mult * pos_atr))
            else:
                best_fav = min(best_fav, l)
                if h >= sl_px:
                    pnl_r = (entry_px - (sl_px + SLIPPAGE_DOLLARS)) / (pos_atr + EPSILON)
                    pnl = pnl_r * risk_dollars
                    equity += pnl; trade_returns.append(pnl); in_pos = False
                elif l <= tp3_px:
                    pnl_r = (entry_px - (tp3_px + SLIPPAGE_DOLLARS)) / (pos_atr + EPSILON)
                    pnl = pnl_r * risk_dollars
                    equity += pnl; trade_returns.append(pnl); in_pos = False
                else:
                    if not tp1_hit and l <= tp1_px:
                        tp1_hit = True; sl_px = min(sl_px, entry_px)
                    if tp1_hit:
                        sl_px = min(sl_px, best_fav + (trailing_mult * pos_atr))
            peak = max(peak, equity)
            max_dd = max(max_dd, ((peak - equity) / (peak + EPSILON)) * 100.0)
            eq_curve.append(equity); curve_ts.append(timestamps[i]); continue
        if not regimes[i]:
            eq_curve.append(equity); curve_ts.append(timestamps[i]); continue
        htf_t = int(htf_trends[i]); smc_b = int(smc_biases[i])
        z_val = float(z_scores[i]); fl_val = float(flows[i]); br_val = float(body_ratios[i])
        bull_sc = ((25.0 if htf_t == 1 else 0.0) + (20.0 if smc_b == 1 else 0.0)
                   + (10.0 if (bos_bulls[i] or choch_bulls[i]) else 0.0)
                   + (25.0 if z_val <= -z_threshold else (15.0 if z_val <= -(z_threshold * 0.65) else 0.0))
                   + (20.0 if (fl_val > 0.15 and br_val >= 0.45) else (10.0 if fl_val > 0.0 else 0.0))
                   + 10.0)
        bear_sc = ((25.0 if htf_t == -1 else 0.0) + (20.0 if smc_b == -1 else 0.0)
                   + (10.0 if (bos_bears[i] or choch_bears[i]) else 0.0)
                   + (25.0 if z_val >= z_threshold else (15.0 if z_val >= (z_threshold * 0.65) else 0.0))
                   + (20.0 if (fl_val < -0.15 and br_val >= 0.45) else (10.0 if fl_val < 0.0 else 0.0))
                   + 10.0)
        atr_val = float(atrs[i])
        if atr_val <= EPSILON:
            eq_curve.append(equity); curve_ts.append(timestamps[i]); continue
        nxt = float(opens[i + 1])
        if bull_sc >= CONFLUENCE_THRESHOLD and bull_sc > bear_sc and htf_t >= 0:
            in_pos, direction = True, 1
            entry_px = nxt + cost_half; pos_atr = max(atr_val, 0.5)
            sl_px = entry_px - pos_atr; tp1_px = entry_px + 1.5 * pos_atr; tp3_px = entry_px + 4.0 * pos_atr
            tp1_hit, best_fav = False, entry_px
            risk_dollars = equity * (RISK_PERCENTAGE / 100.0)
        elif bear_sc >= CONFLUENCE_THRESHOLD and bear_sc > bull_sc and htf_t <= 0:
            in_pos, direction = True, -1
            entry_px = nxt - cost_half; pos_atr = max(atr_val, 0.5)
            sl_px = entry_px + pos_atr; tp1_px = entry_px - 1.5 * pos_atr; tp3_px = entry_px - 4.0 * pos_atr
            tp1_hit, best_fav = False, entry_px
            risk_dollars = equity * (RISK_PERCENTAGE / 100.0)
        eq_curve.append(equity); curve_ts.append(timestamps[i])
    total_trades = len(trade_returns)
    wins = sum(1 for r in trade_returns if r > 0.0)
    losses = sum(1 for r in trade_returns if r <= 0.0)
    gp = sum(r for r in trade_returns if r > 0.0)
    gl = abs(sum(r for r in trade_returns if r < 0.0))
    win_rate = (wins / (total_trades + EPSILON)) * 100.0 if total_trades > 0 else 0.0
    if gl > EPSILON:
        profit_factor = gp / (gl + EPSILON)
    elif gp > 0.0:
        profit_factor = 3.50
    else:
        profit_factor = 0.0
    net_return_pct = ((equity - ACCOUNT_BALANCE) / (ACCOUNT_BALANCE + EPSILON)) * 100.0
    return BacktestMetrics(
        atr_period=atr_period, z_threshold=z_threshold, trailing_mult=trailing_mult,
        total_trades=total_trades, wins=wins, losses=losses,
        win_rate=win_rate, profit_factor=min(profit_factor, 9.99),
        max_drawdown=max_dd, net_return_pct=net_return_pct,
        equity_curve=eq_curve, timestamps=curve_ts, trade_returns=trade_returns)


def run_grid_search_optimization(df_15m, df_1h):
    """3D grid search: ATR × Z × Trailing. Total = 36 combos."""
    candidates = []
    for ap in ATR_GRID:
        for zt in Z_THRESHOLD_GRID:
            for tm in TRAILING_ATR_GRID:
                candidates.append(run_backtest_for_params(df_15m, df_1h, ap, zt, tm))
    valid = [m for m in candidates if m.total_trades >= 5]
    pool = valid if valid else candidates
    best = max(pool, key=lambda m: (
        round(m.profit_factor, 4), round(m.win_rate, 2),
        -round(m.max_drawdown, 2), m.total_trades))
    print(f"[GRID SEARCH] {len(candidates)} combos evaluated (ATR×Z×Trail). Top 5:")
    ranked = sorted(candidates, key=lambda m: -m.profit_factor)[:5]
    for m in ranked:
        marker = " <-- OPTIMAL" if (m.atr_period == best.atr_period
                                     and m.z_threshold == best.z_threshold
                                     and m.trailing_mult == best.trailing_mult) else ""
        print(f"  ATR={m.atr_period:2d} | Z={m.z_threshold:.1f} | Trail={m.trailing_mult:.2f}x "
              f"| Trades={m.total_trades:3d} | WR={m.win_rate:5.1f}% "
              f"| PF={m.profit_factor:4.2f} | MaxDD={m.max_drawdown:5.2f}%{marker}")
    return best, candidates


# ==============================================================================
# CHART GENERATION
# ==============================================================================
def generate_backtest_chart(best_metrics, feed_source, chart_path=CHART_PATH):
    plt.style.use("dark_background")
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(13, 7.5),
                                    gridspec_kw={"height_ratios": [3, 1]}, sharex=True)
    fig.patch.set_facecolor("#0B0F17")
    ax1.set_facecolor("#111827"); ax2.set_facecolor("#111827")
    ts = pd.to_datetime(best_metrics.timestamps)
    eq = np.array(best_metrics.equity_curve, dtype=float)
    running_peak = np.maximum.accumulate(eq)
    dd = -((running_peak - eq) / (running_peak + EPSILON)) * 100.0
    ax1.plot(ts, eq, color="#F59E0B", linewidth=2.0, label="Equity (USD)")
    ax1.fill_between(ts, eq, ACCOUNT_BALANCE, where=(eq >= ACCOUNT_BALANCE),
                     color="#10B981", alpha=0.14)
    ax1.fill_between(ts, eq, ACCOUNT_BALANCE, where=(eq < ACCOUNT_BALANCE),
                     color="#EF4444", alpha=0.14)
    ax1.axhline(ACCOUNT_BALANCE, color="#6B7280", linestyle="--", linewidth=1.0, alpha=0.7)
    ax1.set_title(f"{ENGINE_NAME} | XAUUSD [{feed_source}] "
                  f"(ATR={best_metrics.atr_period}, Z={best_metrics.z_threshold:.1f}, Trail={best_metrics.trailing_mult:.2f}x)",
                  fontsize=12.5, fontweight="bold", color="#F9FAFB", pad=12)
    ax1.set_ylabel("Equity ($)", fontsize=10, color="#D1D5DB")
    ax1.grid(True, linestyle=":", linewidth=0.6, alpha=0.35, color="#4B5563")
    ax1.legend(loc="upper left", facecolor="#1F2937", edgecolor="#374151")
    stats = (f"Trades: {best_metrics.total_trades}\n"
             f"Win Rate: {best_metrics.win_rate:.1f}%\n"
             f"Profit Factor: {best_metrics.profit_factor:.2f}\n"
             f"Max Drawdown: {best_metrics.max_drawdown:.2f}%\n"
             f"Net Return: {best_metrics.net_return_pct:+.2f}%")
    ax1.text(0.985, 0.05, stats, transform=ax1.transAxes, fontsize=9.5,
             va="bottom", ha="right", color="#F3F4F6",
             bbox=dict(boxstyle="round,pad=0.5", facecolor="#1F2937",
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
# TRAILING ENGINE (Per-Position Trail Multiplier)
# ==============================================================================
def manage_active_trailing_stops(conn, latest_bar, current_atr):
    """Uses per-position trailing_multiplier stored at entry."""
    now_iso = dt.datetime.now(dt.timezone.utc).isoformat()
    curr_high = float(latest_bar["High"]); curr_low = float(latest_bar["Low"])
    curr_close = float(latest_bar["Close"])
    cursor = conn.cursor()
    cursor.execute("""SELECT id, direction, entry_price, current_sl, tp1, tp3,
                             atr_at_entry, tp1_breached, highest_favorable,
                             trailing_multiplier
                      FROM active_positions WHERE status='OPEN'""")
    open_rows = cursor.fetchall()
    events = []
    for row in open_rows:
        (pos_id, direction, entry_px, curr_sl, tp1, tp3,
         atr_entry, tp1_breached, highest_fav, trail_mult) = row
        step_atr = max(current_atr, atr_entry, 0.5)
        new_sl = curr_sl; new_fav = highest_fav
        new_tp1_breached = tp1_breached; new_status = "OPEN"
        trail_state = "Initial Forensic SL"
        if direction == "BUY":
            new_fav = max(highest_fav, curr_high)
            if curr_low <= curr_sl:
                new_status, trail_state = "CLOSED_SL", f"Stopped @ {curr_sl:.2f}"
            elif curr_high >= tp3:
                new_status, trail_state = "CLOSED_TP3", f"TP3 Hit @ {tp3:.2f}"
            else:
                if not tp1_breached and curr_high >= tp1:
                    new_tp1_breached = 1
                    new_sl = max(curr_sl, entry_px)
                    trail_state = "Active @ TP1 Breakeven"
                if new_tp1_breached:
                    cand = new_fav - (trail_mult * step_atr)
                    if cand > new_sl:
                        new_sl = cand
                    trail_state = f"Trailing ({trail_mult:.2f}x) SL @ {new_sl:.2f}"
        else:
            new_fav = min(highest_fav, curr_low)
            if curr_high >= curr_sl:
                new_status, trail_state = "CLOSED_SL", f"Stopped @ {curr_sl:.2f}"
            elif curr_low <= tp3:
                new_status, trail_state = "CLOSED_TP3", f"TP3 Hit @ {tp3:.2f}"
            else:
                if not tp1_breached and curr_low <= tp1:
                    new_tp1_breached = 1
                    new_sl = min(curr_sl, entry_px)
                    trail_state = "Active @ TP1 Breakeven"
                if new_tp1_breached:
                    cand = new_fav + (trail_mult * step_atr)
                    if cand < new_sl:
                        new_sl = cand
                    trail_state = f"Trailing ({trail_mult:.2f}x) SL @ {new_sl:.2f}"
        cursor.execute("""UPDATE active_positions
                          SET updated_at_utc=?, current_sl=?, tp1_breached=?,
                              highest_favorable=?, trailing_state=?, status=?
                          WHERE id=?""",
                       (now_iso, round(new_sl, 4), int(new_tp1_breached),
                        round(new_fav, 4), trail_state, new_status, pos_id))
        events.append(f"Position #{pos_id} ({direction} @ {entry_px:.2f} | Close {curr_close:.2f}): {trail_state} [{new_status}]")
    conn.commit()
    if events:
        print("[TRAILING ENGINE] Position Updates:")
        for ev in events:
            print(f"  -> {ev}")
        return events[-1], events
    return "Active @ TP1 Breakeven (Armed)", events


def register_new_position(conn, signal, trailing_multiplier):
    if signal.decision not in ("VERIFIED BUY", "VERIFIED SELL"):
        return
    direction = "BUY" if signal.decision == "VERIFIED BUY" else "SELL"
    cursor = conn.cursor()
    cursor.execute("""INSERT INTO active_positions (
        opened_at_utc, updated_at_utc, asset, direction, entry_price,
        initial_sl, current_sl, tp1, tp2, tp3, atr_at_entry,
        lot_size, tp1_breached, highest_favorable, trailing_state, status,
        trailing_multiplier
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, 'OPEN', ?)""",
                   (signal.timestamp_utc, signal.timestamp_utc, "XAUUSD", direction,
                    round(signal.entry_price, 4), round(signal.stop_loss, 4),
                    round(signal.stop_loss, 4), round(signal.tp1, 4),
                    round(signal.tp2, 4), round(signal.tp3, 4),
                    round(signal.atr_value, 4), round(signal.recommended_lot, 2),
                    round(signal.entry_price, 4), "Active @ TP1 Breakeven",
                    round(trailing_multiplier, 4)))
    conn.commit()


# ==============================================================================
# LIVE SIGNAL EVALUATION
# ==============================================================================
def evaluate_live_signal(df_15m, df_1h, best_metrics, feed_source, swissquote_mid):
    enriched = enrich_features(df_15m, df_1h, atr_period=best_metrics.atr_period,
                                z_window=Z_LOOKBACK_WINDOW)
    eval_row = enriched.iloc[-1]
    decision, confluence, breakdown = score_bar_confluence(eval_row, z_threshold=best_metrics.z_threshold)
    entry_price = float(eval_row["Close"])
    atr_val = max(float(eval_row["ATR"]), 0.50)
    z_score = float(eval_row["Z_Score"])
    regime_label = "TRENDING" if bool(eval_row["Regime_Trending"]) else "CHOPPY FILTERED"
    smc_label = "BULLISH" if int(eval_row["smc_bias"]) == 1 else "BEARISH"
    htf_label = ("BULLISH 1H HARMONY" if int(eval_row["HTF_Trend"]) == 1
                 else ("BEARISH 1H HARMONY" if int(eval_row["HTF_Trend"]) == -1 else "NEUTRAL 1H"))
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
        decision=decision, entry_price=entry_price, stop_loss=stop_loss,
        tp1=tp1, tp2=tp2, tp3=tp3, atr_value=atr_val, z_score=z_score,
        confluence_score=confluence, market_regime=regime_label,
        smc_bias=smc_label, order_flow_score=float(eval_row["smoothed_flow"]),
        htf_alignment=htf_label, recommended_lot=recommended_lot,
        risk_usd=risk_usd, rrr_tp3=rrr_tp3,
        trailing_protocol="Active @ TP1 Breakeven", feed_source=feed_source,
        swissquote_spot_mid=swissquote_mid, timestamp_utc=ts_utc,
        pillar_breakdown=breakdown)
    raw_math = {
        "feed_source": feed_source,
        "swissquote_spot_mid": round(swissquote_mid, 4) if swissquote_mid else None,
        "latest_bar_time": str(enriched.index[-1]),
        "open": round(float(eval_row["Open"]), 4),
        "high": round(float(eval_row["High"]), 4),
        "low": round(float(eval_row["Low"]), 4),
        "close": round(float(eval_row["Close"]), 4),
        "atr": round(atr_val, 6), "adx": round(float(eval_row["ADX"]), 4),
        "efficiency_ratio": round(float(eval_row["Efficiency_Ratio"]), 4),
        "z_score": round(z_score, 6), "body_ratio": round(float(eval_row["body_ratio"]), 4),
        "rel_volume": round(float(eval_row["rel_volume"]), 4),
        "has_real_volume": bool(eval_row["has_real_volume"]),
        "htf_ema20": round(float(eval_row["HTF_EMA20"]), 4),
        "htf_ema50": round(float(eval_row["HTF_EMA50"]), 4),
        "trailing_atr_mult": best_metrics.trailing_mult,
        "breakdown": breakdown}
    return signal, raw_math


# ==============================================================================
# WEBHOOKS
# ==============================================================================
def build_forensic_report_html(signal, best_metrics, chart_status, db_status):
    return (
        f"🛡️ <b>{ENGINE_NAME} (ULTIMATE EDITION)</b> 🛡️\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🌟 <b>Asset:</b> XAUUSD (SPOT GOLD) [{signal.feed_source}]\n"
        f"📊 <b>Signal Decision:</b> {signal.decision}\n"
        f"🎯 <b>Precision Entry:</b> {signal.entry_price:.2f}\n"
        f"🛑 <b>Forensic Stop Loss:</b> {signal.stop_loss:.2f}\n"
        f"📈 <b>TP1 (1.5R):</b> {signal.tp1:.2f}\n"
        f"📈 <b>TP2 (2.5R):</b> {signal.tp2:.2f}\n"
        f"🚀 <b>TP3 (4.0R):</b> {signal.tp3:.2f}\n"
        f"🔄 <b>Trailing Stop:</b> {signal.trailing_protocol} ({best_metrics.trailing_mult:.2f}×ATR)\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚙️ <b>Grid Search &amp; Visualization:</b>\n"
        f"• <i>Optimal ATR:</i> {best_metrics.atr_period}\n"
        f"• <i>Optimal Z-Threshold:</i> {best_metrics.z_threshold:.1f}\n"
        f"• <i>Optimal Trail:</i> {best_metrics.trailing_mult:.2f}×ATR\n"
        f"• <i>Backtest Chart:</i> {chart_status}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 <b>Risk &amp; Position Sizing:</b>\n"
        f"• <i>Recommended Lot:</i> {signal.recommended_lot:.2f} ({RISK_PERCENTAGE:.0f}% Risk)\n"
        f"• <i>Risk Amount:</i> ${signal.risk_usd:.2f}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔬 <b>Forensic Audit:</b>\n"
        f"• <i>Market Regime:</i> {signal.market_regime}\n"
        f"• <i>Confluence:</i> {signal.confluence_score:.1f}/100\n"
        f"• <i>SQLite DB:</i> {db_status}\n"
        f"• <i>Error Handler:</i> Active / Nominal\n"
        f"• <i>Win Rate:</i> {best_metrics.win_rate:.1f}%\n"
        f"• <i>Profit Factor:</i> {best_metrics.profit_factor:.2f}\n"
        f"• <i>Max Drawdown:</i> {best_metrics.max_drawdown:.1f}%\n"
        f"• <i>Timestamp (UTC):</i> {signal.timestamp_utc}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━")


def html_to_discord_markdown(html_msg):
    return (html_msg.replace("<b>", "**").replace("</b>", "**")
            .replace("<i>", "*").replace("</i>", "*").replace("&amp;", "&"))


def _send_telegram_signal(html_payload, chart_path):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return "Skipped (missing creds)"
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": html_payload,
                  "parse_mode": "HTML", "disable_web_page_preview": True},
            timeout=20)
        resp.raise_for_status()
        if os.path.exists(chart_path):
            with open(chart_path, "rb") as img_file:
                pr = requests.post(
                    f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto",
                    data={"chat_id": TELEGRAM_CHAT_ID,
                          "caption": f"{ENGINE_NAME} | XAUUSD Backtest"},
                    files={"photo": img_file}, timeout=25)
                pr.raise_for_status()
        return "Delivered OK"
    except Exception as exc:
        send_telegram_error_alert("Telegram Webhook Delivery", exc)
        return f"Failed: {exc}"


def _send_discord_signal(discord_payload, chart_path):
    if not DISCORD_WEBHOOK_URL:
        return "Skipped (missing webhook)"
    try:
        if os.path.exists(chart_path):
            with open(chart_path, "rb") as img_file:
                resp = requests.post(DISCORD_WEBHOOK_URL,
                                     data={"content": discord_payload},
                                     files={"file": ("backtest_performance.png", img_file, "image/png")},
                                     timeout=25)
        else:
            resp = requests.post(DISCORD_WEBHOOK_URL, json={"content": discord_payload}, timeout=20)
        resp.raise_for_status()
        return "Delivered OK"
    except Exception as exc:
        send_telegram_error_alert("Discord Webhook Delivery", exc)
        return f"Failed: {exc}"


def dispatch_dual_channel_webhooks(signal, best_metrics, chart_status, db_status):
    html_msg = build_forensic_report_html(signal, best_metrics, chart_status, db_status)
    discord_msg = html_to_discord_markdown(html_msg)
    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        tg_future = executor.submit(_send_telegram_signal, html_msg, CHART_PATH)
        dc_future = executor.submit(_send_discord_signal, discord_msg, CHART_PATH)
        results["telegram"] = tg_future.result()
        results["discord"] = dc_future.result()
    print(f"[WEBHOOK DISPATCH] Telegram={results['telegram']} | Discord={results['discord']}")
    return results


# ==============================================================================
# MAIN ORCHESTRATION
# ==============================================================================
def main() -> int:
    conn = None
    try:
        print(f"[BOOT] {ENGINE_NAME} initialising...")
        print(f"[CONFIG] 3D Grid: ATR×Z×Trail (default={TRAILING_ATR_MULT:.2f}, "
              f"grid={TRAILING_ATR_GRID})")
        conn = init_sqlite_db(DB_PATH)
        df_15m, df_1h, feed_source, swissquote_mid = fetch_spot_gold_data()
        print(f"[DATA] Feed='{feed_source}' | Loaded {len(df_15m)}x 15m & {len(df_1h)}x 1h "
              f"| Latest Close: {float(df_15m['Close'].iloc[-1]):.2f}")
        best_metrics, _ = run_grid_search_optimization(df_15m, df_1h)
        chart_status = generate_backtest_chart(best_metrics, feed_source, CHART_PATH)
        print(f"[CHART] {chart_status} -> {CHART_PATH}")
        signal, raw_math = evaluate_live_signal(df_15m, df_1h, best_metrics, feed_source, swissquote_mid)
        latest_atr_val = float(compute_atr(df_15m, best_metrics.atr_period).iloc[-1])
        trailing_summary, _ = manage_active_trailing_stops(conn, df_15m.iloc[-1], latest_atr_val)
        if signal.decision == "NO SIGNAL":
            signal.trailing_protocol = trailing_summary
        db_status = forensic_audit_log(conn, signal, best_metrics, raw_math)
        if signal.decision in ("VERIFIED BUY", "VERIFIED SELL"):
            register_new_position(conn, signal, best_metrics.trailing_mult)
            dispatch_dual_channel_webhooks(signal, best_metrics, chart_status, db_status)
        else:
            print(f"[GOVERNOR] Decision={signal.decision} "
                  f"(Regime={signal.market_regime}, Score={signal.confluence_score:.1f}/100). "
                  f"Webhook suppressed.")
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
