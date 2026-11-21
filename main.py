import json, os
import asyncio
import threading
import logging
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from datetime import datetime, time as dt_time
import time as time_module
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import borsapy as bp
import pytz
import holidays

from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

# ==========================================
# --- AYARLAR ---
# ==========================================
BOT_TOKEN = os.environ.get("BOT_TOKEN")
TV_SESSION = os.environ.get("TV_SESSION")
TV_SESSION_SIGN = os.environ.get("TV_SESSION_SIGN")

CHAT_IDS_STR = os.environ.get("CHAT_IDS", "5066109900,8776376951,8634151585")
try:
    CHAT_IDS = [int(cid.strip()) for cid in CHAT_IDS_STR.split(",")]
except ValueError:
    CHAT_IDS = [5066109900, 8776376951, 8634151585]

ADMIN_ID = CHAT_IDS[0] if CHAT_IDS else None

if not BOT_TOKEN:
    raise ValueError("❌ HATA: BOT_TOKEN bulunamadı!")

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# ==========================================
# VERİ DİZİNİ VE DOSYALAR (SADECE KOSAN AT İÇİN)
# ==========================================
DATA_DIR = os.getenv("DATA_DIR", "/tmp")
if DATA_DIR == "/tmp":
    logger.warning("⚠️ DATA_DIR=/tmp: Restart/deploy sonrası açık pozisyonlar kaybolur!")
KOSAN_AT_FILE = os.path.join(DATA_DIR, "kosan_at_trades.json")

last_error_time = {}
async def send_admin_alert(error_msg, error_key="general"):
    now = time_module.time()
    if error_key in last_error_time and (now - last_error_time[error_key]) < 900:
        return
    last_error_time[error_key] = now
    if ADMIN_ID:
        try:
            await bot_app.bot.send_message(chat_id=ADMIN_ID, text=f"🚨 <b>SİSTEM HATASI</b>\n{error_msg}", parse_mode="HTML")
        except Exception as e:
            logger.error(f"Admin uyarı gönderilemedi: {e}")

if TV_SESSION and TV_SESSION_SIGN:
    try:
        bp.set_tradingview_auth(session=TV_SESSION, session_sign=TV_SESSION_SIGN)
        logger.info("✅ Borsapy: TradingView canlı veri yetkisi alındı!")
    except Exception as e:
        logger.error(f"⚠️ TradingView Auth Hatası: {e}")
else:
    logger.info("ℹ️ TradingView çerezleri girilmedi. Gecikmeli veri.")

TIMEZONE = pytz.timezone("Europe/Istanbul")
TR_HOLIDAYS = holidays.Turkey(years=range(2024, 2031))

# ==========================================
# WHITE / RED LIST (SENİN VERDİĞİN LİSTELER)
# ==========================================
WHITE_LIST = [
    "EKGYO", "MGROS", "TRALT", "THYAO", "TRMET", "PGSUS", "KRDMD", "ODAS",
    "ALARK", "VESTL", "AKSEN", "TOASO", "ARCLK", "TTKOM", "SISE", "FROTO",
    "TKFEN", "HALKB", "GUBRF", "ASELS", "ASTOR", "PETKM", "KCHOL", "ISCTR"
]

RED_LIST = [
    "EREGL", "SASA", "SAHOL", "AEFES", "AKBNK", "TAVHL", "GARAN",
    "SOKM", "ULKER", "BIMAS", "DOHOL", "TUPRS", "ENKAI", "XU030",
    "OYAKC", "BRSAN", "CIMSA", "VAKBN", "ENJSA", "TCELL", "YKBNK", "DOAS"
]

ALL_SYMBOLS = WHITE_LIST + RED_LIST
BIST_STOCKS = [s for s in ALL_SYMBOLS if not s.startswith("XU")]

SECTORS = {
    "🏦 Bankacılık": ["GARAN", "YKBNK", "AKBNK", "ISCTR", "VAKBN", "HALKB"],
    "🏢 Holdingler": ["SAHOL", "KCHOL", "ALARK", "DOHOL"],
    "✈️ Havacılık": ["THYAO", "PGSUS", "TAVHL"],
    "⚡ Enerji": ["TUPRS", "AKSEN", "ENJSA", "ASTOR"],
    "🔩 Demir-Çelik & Metal": ["EREGL", "KRDMD", "BRSAN", "TRMET", "TRALT"],
    "🚗 Otomotiv": ["TOASO", "FROTO", "DOAS"],
    "🧪 Kimya": ["PETKM", "SASA", "GUBRF"],
    "🛒 Perakende": ["BIMAS", "MGROS", "SOKM"],
    "🏗️ İnşaat & Çimento": ["ENKAI", "TKFEN", "CIMSA", "OYAKC"],
    "📡 Telekomünikasyon": ["TCELL", "TTKOM"],
    "🛡️ Savunma": ["ASELS"],
    "🥤 Gıda & İçecek": ["AEFES", "ULKER"],
    "📺 Beyaz Eşya": ["ARCLK", "VESTL"],
    "🪟 Cam": ["SISE"],
    "🏘️ GYO": ["EKGYO"],
}

# ==========================================
# KOŞAN AT PARAMETRELERİ (SENİN İKİNCİ KODDAN)
# ==========================================
TF_MINUTES = {"5m": 5, "15m": 15, "30m": 30, "1h": 60, "4h": 240, "1d": 1440}
KOSAN_RSI_PERIOD = 8
KOSAN_LONG_LEVEL = 75
KOSAN_SHORT_LEVEL = 35
SL_PCT = 1.5
WHITE_BASE_TP_PCT = 2.5
RED_BASE_TP_PCT = 1.5
WHITE_ZB_TP_PCT = 4.0
RED_ZB_TP_PCT = 3.0
ZB_LOOKBACK = 18
ZB_ORDER = 2
MAX_SIGNAL_LAG_MIN = 30

# Portföy Dağılımı
WHITE_PRIME_ALLOC = 100_000.0
WHITE_EARLY_ALLOC = 50_000.0
WHITE_OFF_ALLOC = 75_000.0
WHITE_ADD_ALLOC = 50_000.0

RED_PRIME_ALLOC = 50_000.0
RED_EARLY_ALLOC = 25_000.0
RED_OFF_ALLOC = 30_000.0

# Zaman Kuralları
MARKET_OPEN_MSG_TIME = dt_time(9, 50)
EARLY_WINDOW_START = dt_time(10, 0)
EARLY_WINDOW_END = dt_time(11, 0)
PRIME_START = dt_time(11, 0)
PRIME_END = dt_time(17, 0)
NO_NEW_ENTRY_AFTER = dt_time(17, 0)

# State
KOSAN_AT_TRADES = {}
KOSAN_LAST_SIGNAL = {}
KOSAN_DAILY_STATS = {"date": "", "signals": 0, "tp_4": 0, "tp_3": 0, "tp_2_5": 0, "tp_1_5": 0, "sl": 0, "zb_exit": 0}
KOSAN_WEEKLY_STATS = {"week_key": "", "signals": 0, "tp_4": 0, "tp_3": 0, "tp_2_5": 0, "tp_1_5": 0, "sl": 0, "zb_exit": 0}
LATEST_PRICES = {}
STATE_LOCK = asyncio.Lock()
is_scanning = False
background_tasks = set()
DATA_CACHE = {}
CACHE_TTL = 60
EXECUTOR = ThreadPoolExecutor(max_workers=20)
FETCH_SEM = asyncio.Semaphore(4)

# ==========================================
# YARDIMCI FONKSİYONLAR
# ==========================================
def is_arife(now):
    return now.month == 10 and now.day == 28

def is_trading_day(d):
    return d.weekday() <= 4 and d not in TR_HOLIDAYS

def is_market_open(now=None):
    now = now or datetime.now(TIMEZONE)
    if not is_trading_day(now.date()):
        return False
    if is_arife(now):
        return dt_time(9, 55) <= now.time() <= dt_time(13, 0)
    return dt_time(9, 55) <= now.time() <= dt_time(18, 10)

def get_real_lag_minutes(last_bar_time, tf):
    bar_close_time = last_bar_time + pd.Timedelta(minutes=TF_MINUTES.get(tf, 0))
    now = datetime.now(TIMEZONE)
    if now < bar_close_time:
        return 0
    return int((now - bar_close_time).total_seconds() / 60)

def get_lag_emoji(minutes):
    if minutes <= 5:
        return "🟢"
    elif minutes <= 15:
        return "🟡"
    return "🔴"

def closed_only(df, tf):
    if df is None or df.empty:
        return df
    now = datetime.now(TIMEZONE)
    close_t = df.index + pd.Timedelta(minutes=TF_MINUTES[tf])
    mask = close_t <= now
    after_close = (df.index.date == now.date()) & (now.time() >= dt_time(18, 5))
    return df[mask | after_close]

def session_days_elapsed(entry_date, today):
    n = 0
    d = entry_date
    while d <= today:
        if is_trading_day(d):
            n += 1
        d += pd.Timedelta(days=1) if hasattr(d, 'year') else dt_time(0)
    return n

# ==========================================
# KAYIT / YÜKLEME (SADECE KOSAN AT)
# ==========================================
def save_trades():
    try:
        out_kosan = {}
        for k, v in KOSAN_AT_TRADES.items():
            t = v.copy()
            for f in ("entry_time", "last_bar"):
                if isinstance(t.get(f), (datetime, pd.Timestamp)):
                    t[f] = t[f].isoformat()
            out_kosan[k] = t
        with open(KOSAN_AT_FILE + ".tmp", "w", encoding="utf-8") as f:
            json.dump(out_kosan, f, ensure_ascii=False, indent=2)
        os.replace(KOSAN_AT_FILE + ".tmp", KOSAN_AT_FILE)
    except Exception as e:
        logger.error(f"Trade kayıt hatası: {e}")

def _parse_dt(s):
    dt = datetime.fromisoformat(s)
    return TIMEZONE.localize(dt) if dt.tzinfo is None else dt

def load_trades():
    global KOSAN_AT_TRADES
    if os.path.exists(KOSAN_AT_FILE):
        try:
            with open(KOSAN_AT_FILE, "r", encoding="utf-8") as f:
                loaded = json.load(f)
            for k, v in loaded.items():
                for fld in ("entry_time", "last_bar"):
                    if isinstance(v.get(fld), str):
                        v[fld] = _parse_dt(v[fld])
                if "last_bar" not in v:
                    v["last_bar"] = v["entry_time"]
                KOSAN_AT_TRADES[k] = v
            logger.info(f"✅ {len(KOSAN_AT_TRADES)} aktif Koşan At işlemi yüklendi.")
        except Exception as e:
            logger.error(f"Koşan At yükleme hatası: {e}")

# ==========================================
# TELEGRAM GÖNDERİM
# ==========================================
async def send_all(text, reply_ids=None):
    ids = {}
    for cid in CHAT_IDS:
        try:
            kw = {}
            if reply_ids and str(cid) in reply_ids:
                kw = {"reply_to_message_id": reply_ids[str(cid)], "allow_sending_without_reply": True}
            sent = await bot_app.bot.send_message(chat_id=cid, text=text, parse_mode="HTML", **kw)
            ids[str(cid)] = sent.message_id
        except Exception as e:
            logger.error(f"Telegram gönderim hatası ({cid}): {e}")
    return ids

# ==========================================
# MATEMATİK / VERİ
# ==========================================
def get_rsi(series, period=8):
    delta = series.diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    avg_gain = gain.ewm(alpha=1/period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1/period, adjust=False, min_periods=period).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def fetch_ohlc(symbol, interval, limit=500):
    for attempt in range(3):
        try:
            df = bp.Ticker(symbol).history(period=f"{limit + 2} bar", interval=interval)
            if df is not None and not df.empty:
                df.columns = [str(c).lower() for c in df.columns]
                if not isinstance(df.index, pd.DatetimeIndex):
                    df.index = pd.to_datetime(df.index)
                if df.index.tz is None:
                    df.index = df.index.tz_localize(TIMEZONE)
                else:
                    df.index = df.index.tz_convert(TIMEZONE)
                return df
        except Exception as e:
            logger.error(f"Veri çekme hatası ({symbol} {interval}): {e}")
            time_module.sleep(2)
    return None

async def fetch_ohlc_async(symbol, interval, limit=500):
    cache_key = f"{symbol}_{interval}_{limit}"
    if cache_key in DATA_CACHE:
        cached_df, cache_time = DATA_CACHE[cache_key]
        if time_module.time() - cache_time < CACHE_TTL:
            return cached_df.copy()
    try:
        async with FETCH_SEM:
            await asyncio.sleep(0.3)
            loop = asyncio.get_running_loop()
            df = await asyncio.wait_for(loop.run_in_executor(EXECUTOR, fetch_ohlc, symbol, interval, limit), timeout=20.0)
            if df is not None:
                DATA_CACHE[cache_key] = (df, time_module.time())
            return df
    except asyncio.TimeoutError:
        logger.warning(f"Timeout: {symbol} {interval}")
        return None
    except Exception as e:
        logger.error(f"Async Fetch Hatası ({symbol}): {e}")
        return None

# ==========================================
# YARDIMCI: BADGE, ALLOC, SL/TP
# ==========================================
def is_white(symbol):
    return symbol in WHITE_LIST

def badge(symbol):
    return "⬜🏳️ WHITE LIST" if is_white(symbol) else "🟥🚩 RED LIST"

def is_prime(now_t):
    return PRIME_START <= now_t < PRIME_END

def is_early(now_t):
    return EARLY_WINDOW_START <= now_t < EARLY_WINDOW_END

def can_enter_now(now_t):
    return now_t < NO_NEW_ENTRY_AFTER

def get_alloc_and_label(symbol, now_t):
    white_flag = is_white(symbol)
    if is_prime(now_t):
        return (WHITE_PRIME_ALLOC if white_flag else RED_PRIME_ALLOC), "Verimli Saat (11:00-17:00)"
    if is_early(now_t):
        return (WHITE_EARLY_ALLOC if white_flag else RED_EARLY_ALLOC), "10:00-11:00 Dilimi"
    return (WHITE_OFF_ALLOC if white_flag else RED_OFF_ALLOC), "Saat Dışı"

def calc_sl_tp(avg_cost, direction, tp_pct):
    if direction == "LONG":
        return round(avg_cost * (1.0 - SL_PCT / 100.0), 4), round(avg_cost * (1.0 + tp_pct / 100.0), 4)
    return round(avg_cost * (1.0 + SL_PCT / 100.0), 4), round(avg_cost * (1.0 - tp_pct / 100.0), 4)

def record_exit(reason, tp_pct=0.0):
    for st in (KOSAN_DAILY_STATS, KOSAN_WEEKLY_STATS):
        if reason == "SL":
            st["sl"] += 1
        elif reason == "ZB_EXIT":
            st["zb_exit"] += 1
        elif reason == "TP":
            if abs(tp_pct - 4.0) < 1e-6: st["tp_4"] += 1
            elif abs(tp_pct - 3.0) < 1e-6: st["tp_3"] += 1
            elif abs(tp_pct - 2.5) < 1e-6: st["tp_2_5"] += 1
            elif abs(tp_pct - 1.5) < 1e-6: st["tp_1_5"] += 1

# ==========================================
# ZEKİ BEY SERİSİ
# ==========================================
def check_zeki_bey_series(rsi_series, idx, direction, lookback=18, order=2):
    min_needed = max(10, order * 2 + 3)
    if idx < min_needed:
        return False
    start_i = max(0, idx - (lookback - 1))
    vals = rsi_series.iloc[start_i:idx + 1].dropna().values
    if len(vals) < min_needed:
        return False

    curr_val = float(vals[-1])
    prev_val = float(vals[-2])

    def find_peaks(data, ord_n):
        peaks = []
        for i in range(ord_n, len(data) - ord_n):
            if data[i] == max(data[i - ord_n:i + ord_n + 1]):
                peaks.append(i)
        return peaks

    def find_troughs(data, ord_n):
        troughs = []
        for i in range(ord_n, len(data) - ord_n):
            if data[i] == min(data[i - ord_n:i + ord_n + 1]):
                troughs.append(i)
        return troughs

    if direction == "LONG":
        peaks = find_peaks(vals, order)
        if len(peaks) >= 2:
            p1, p2 = peaks[-2], peaks[-1]
            if p2 - p1 < order:
                return False
            slope = (vals[p2] - vals[p1]) / (p2 - p1)
            proj_curr = vals[p2] + slope * (len(vals) - 1 - p2)
            proj_prev = vals[p2] + slope * (len(vals) - 2 - p2)
            if prev_val <= proj_prev and curr_val > proj_curr:
                return True

    elif direction == "SHORT":
        troughs = find_troughs(vals, order)
        if len(troughs) >= 2:
            t1, t2 = troughs[-2], troughs[-1]
            if t2 - t1 < order:
                return False
            slope = (vals[t2] - vals[t1]) / (t2 - t1)
            proj_curr = vals[t2] + slope * (len(vals) - 1 - t2)
            proj_prev = vals[t2] + slope * (len(vals) - 2 - t2)
            if prev_val >= proj_prev and curr_val < proj_curr:
                return True
    return False

# ==========================================
# KOŞAN AT TAKİP
# ==========================================
async def track_kosan(sym, df5, df_1h):
    trade = KOSAN_AT_TRADES[sym]
    long = trade["direction"] == "LONG"
    ids = trade.get("msg_ids")
    new = df5[df5.index > trade["last_bar"]]
    closed = False
    changed = False

    for ts, bar in new.iterrows():
        hi, lo, op = float(bar['high']), float(bar['low']), float(bar['open'])
        trade["last_bar"] = ts
        changed = True

        sl_hit = lo <= trade["sl"] if long else hi >= trade["sl"]
        if sl_hit:
            exit_p = min(op, trade["sl"]) if long else max(op, trade["sl"])
            pnl = (exit_p - trade["avg_cost"]) / trade["avg_cost"] * 100 * (1 if long else -1)
            gap_txt = " (gap ile açılıştan)" if exit_p != trade["sl"] else ""
            reason = f"🛑 <b>STOP OLDU</b> ({exit_p:,.2f}){gap_txt}"
            closed = True
            break

        if trade["status"] == "WAITING_2ND":
            ema = float(bar['ema50'])
            if (long and lo <= ema) or ((not long) and hi >= ema):
                trade["entry2"] = ema
                trade["avg_cost"] = (trade["entry1"] + ema) / 2
                trade["sl"], trade["tp"] = calc_sl_tp(trade["avg_cost"], trade["direction"], trade["tp_pct"])
                trade["status"] = "FULLY_ENTERED"
                save_trades()
                msg = (f"🔄 <b>{sym} - 2. GİRİŞ TETİKLENDİ (50K)</b> 🔄\n"
                       f"Fiyat 5M EMA 50'ye ({ema:,.2f}) değdi.\n\n"
                       f"💰 <b>Yeni Ortalama Maliyet:</b> {trade['avg_cost']:,.2f}\n"
                       f"🛑 <b>Yeni Stop (SL %{SL_PCT}):</b> {trade['sl']:,.2f}\n"
                       f"🎯 <b>Yeni TP (%{trade['tp_pct']}):</b> {trade['tp']:,.2f}")
                await send_all(msg, ids)
                continue

        tp_hit = hi >= trade["tp"] if long else lo <= trade["tp"]
        if tp_hit:
            pnl = (trade["tp"] - trade["avg_cost"]) / trade["avg_cost"] * 100 * (1 if long else -1)
            reason = f"🎯 <b>TP VURULDU (TAM KÂR)</b> ({trade['tp']:,.2f})"
            closed = True
            break

    if closed:
        msg = (f"🚨 <b>{sym} İŞLEM KAPANDI</b> 🚨\n{reason}\n"
               f"📊 <b>K/Z (ort. maliyete göre):</b> %{pnl:+.2f}\nYeni sinyaller aranıyor.")
        await send_all(msg, ids)
        del KOSAN_AT_TRADES[sym]
        save_trades()
        record_exit("TP" if "TP" in reason else "SL", trade["tp_pct"] if "TP" in reason else 0)
        return

    # Zeki Bey Exit (Tepe/Dip kırılımı)
    if not trade.get("zb_exit_sent", False) and df_1h is not None:
        df_1h_closed = closed_only(df_1h, "1h")
        if df_1h_closed is not None and len(df_1h_closed) > 20:
            rsi_series = get_rsi(df_1h_closed['close'], 8)
            last_idx = len(df_1h_closed) - 1
            zb_exit = check_zeki_bey_series(rsi_series, last_idx, "SHORT" if long else "LONG", ZB_LOOKBACK, ZB_ORDER)
            if zb_exit:
                trade["zb_exit_sent"] = True
                msg = f"🛑 <b>ZEKİ BEY ÇIKIŞ SİNYALİ - {sym}</b>\n1H RSI ters yöne kırıldı, pozisyon kapatılıyor."
                await send_all(msg, ids)
                del KOSAN_AT_TRADES[sym]
                save_trades()
                record_exit("ZB_EXIT", trade["tp_pct"])
                return

    if changed:
        save_trades()

async def check_kosan_at(sym, xu_trend, df_1h_raw):
    try:
        has_trade = sym in KOSAN_AT_TRADES
        df_5m = await fetch_ohlc_async(sym, "5m", limit=500 if has_trade else 150)
        df_5m = closed_only(df_5m, "5m")
        if df_5m is None or len(df_5m) < 60:
            return
        df_5m = df_5m.copy()
        df_5m['rsi'] = get_rsi(df_5m['close'], KOSAN_RSI_PERIOD)
        df_5m['ema50'] = df_5m['close'].ewm(span=50, adjust=False).mean()

        if has_trade:
            await track_kosan(sym, df_5m, df_1h_raw)
            return

        if df_1h_raw is None or len(df_1h_raw) < 20:
            return
        df_1h = df_1h_raw.copy()
        df_1h['rsi_1h'] = get_rsi(df_1h['close'], 14)

        df = pd.merge_asof(
            df_5m[['open', 'high', 'low', 'close', 'rsi', 'ema50']].sort_index(),
            df_1h[['rsi_1h']].sort_index(),
            left_index=True, right_index=True, direction='backward'
        ).dropna()
        if len(df) < 2:
            return

        bar_time = df.index[-1]
        if KOSAN_LAST_SIGNAL.get(sym) == bar_time:
            return
        lag_dk = get_real_lag_minutes(bar_time, "5m")
        if lag_dk > MAX_SIGNAL_LAG_MIN:
            return

        now_t = datetime.now(TIMEZONE).time()
        if not can_enter_now(now_t):
            return # 17:00 sonrası yeni işlem yok

        curr, prev = df.iloc[-1], df.iloc[-2]
        long_cond = (prev['rsi'] <= KOSAN_LONG_LEVEL and curr['rsi'] > KOSAN_LONG_LEVEL
                     and curr['close'] > curr['ema50'] and curr['rsi_1h'] > 50 and xu_trend == 1)
        short_cond = (prev['rsi'] >= KOSAN_SHORT_LEVEL and curr['rsi'] < KOSAN_SHORT_LEVEL
                      and curr['close'] < curr['ema50'] and curr['rsi_1h'] < 50 and xu_trend == -1)
        if not (long_cond or short_cond):
            return

        direction = "LONG" if long_cond else "SHORT"
        entry = float(curr['close'])
        tp_pct = WHITE_BASE_TP_PCT if is_white(sym) else RED_BASE_TP_PCT
        alloc_tl, saat_etiket = get_alloc_and_label(sym, now_t)
        sl, tp = calc_sl_tp(entry, direction, tp_pct)
        xu_text = "📈 <b>Yukarı</b>" if xu_trend == 1 else "📉 <b>Aşağı</b>"
        dir_emoji = "📈" if long_cond else "📉"

        msg = (f"🐎 <b>KOŞAN AT SİNYALİ ({direction}) - {sym} [5M]</b> 🐎\n"
               f"✅ <b>1. Giriş Yapıldı (50K)</b>\n\n"
               f"{dir_emoji} <b>Yön:</b> {direction}\n"
               f"📊 <b>1H RSI(14):</b> {curr['rsi_1h']:.1f} | <b>5M RSI(8):</b> {curr['rsi']:.1f}\n"
               f"🌍 <b>Endeks (XU30):</b> {xu_text}\n\n"
               f"💰 <b>1. Giriş (Sinyal):</b> {entry:,.2f}\n"
               f"⏳ <b>2. Giriş (50K) Bekleniyor:</b> 5M EMA 50 ({curr['ema50']:,.2f})\n"
               f"🛑 <b>İlk Stop (SL %{SL_PCT}):</b> {sl:,.2f}\n"
               f"🎯 <b>TP (%{tp_pct}):</b> {tp:,.2f}\n"
               f"⏳ <b>Takip:</b> SL/TP olana kadar\n\n"
               f"⚠️ <i>İşlem aktif. SL/TP olana kadar takip edilir.</i>\n"
               f"⏱ <b>Veri Gecikmesi:</b> {get_lag_emoji(lag_dk)} {lag_dk} dk")

        zeki_sent = False
        df_1h_closed = closed_only(df_1h_raw, "1h")
        if df_1h_closed is not None and len(df_1h_closed) > 20:
            rsi_series = get_rsi(df_1h_closed['close'], 8)
            last_idx = len(df_1h_closed) - 1
            zb_signal = check_zeki_bey_series(rsi_series, last_idx, direction, ZB_LOOKBACK, ZB_ORDER)
            if zb_signal:
                msg += (f"\n\n🕵️‍♂️ <b>Zeki Bey Özel Sinyali!</b>\n"
                        f"1H RSI tepe/dip kırılımı gerçekleşti.")
                zeki_sent = True

        ids = await send_all(msg)

        KOSAN_LAST_SIGNAL[sym] = bar_time
        KOSAN_AT_TRADES[sym] = {
            "symbol": sym, "direction": direction, "entry1": entry, "entry2": None,
            "avg_cost": entry, "sl": sl, "tp": tp, "tp_pct": tp_pct,
            "status": "WAITING_2ND", "zeki_sent": zeki_sent,
            "entry_time": datetime.now(TIMEZONE), "last_bar": bar_time,
            "msg_ids": ids
        }
        save_trades()
        KOSAN_DAILY_STATS["signals"] += 1
        KOSAN_WEEKLY_STATS["signals"] += 1
    except Exception as e:
        logger.error(f"Koşan At hatası ({sym}): {e}")

# ==========================================
# BIST 30 ÖZETİ (Ana Koddan)
# ==========================================
async def generate_market_summary(session_name):
    # Bu fonksiyon ana kodundaki gibi çalışır, veri borsapy ile çekilir.
    pass # Kısaltmak için buraya yazmıyorum, ana kodundaki aynısını kullanabilirsin.

# ==========================================
# KOMUTLAR
# ==========================================
async def analyze_symbol(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    # Ana koddaki analiz fonksiyonu aynen kalacak.
    pass

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id not in CHAT_IDS:
        return
    await update.message.reply_text("✅ Sistem çalışıyor. Analiz için /ASELS gibi komut yazabilirsiniz. Manuel tarama için /tara yazın.")

async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    # Ana koddaki yardım menüsü
    pass

async def cmd_pozlar(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id not in CHAT_IDS:
        return
    if not KOSAN_AT_TRADES:
        await update.message.reply_text("📭 Açık pozisyon bulunmuyor.")
        return

    lines = ["📋 <b>AÇIK POZİSYONLAR</b>\n"]
    for sym, pos in list(KOSAN_AT_TRADES.items()):
        cur_p = LATEST_PRICES.get(sym, pos["avg_cost"])
        is_long = pos["direction"] == "LONG"
        pnl_pct = ((cur_p - pos["avg_cost"]) / pos["avg_cost"]) * 100.0 * (1.0 if is_long else -1.0)
        icon = "🟢" if pnl_pct >= 0 else "🔴"
        lines.append(
            f"{icon} <b>{sym}</b> ({badge(sym)}) | <b>{pos['direction']}</b>\n"
            f"   • Ort. Maliyet: <code>{pos['avg_cost']:.2f}</code> | Aktif: <code>{cur_p:.2f}</code> | K/Z: <b>%{pnl_pct:+.2f}</b>\n"
            f"   • Tutar: <code>{pos['allocated_tl']:,.0f} TL</code> | SL: <code>{pos['sl']:.2f}</code> | TP: <code>%{pos['tp_pct']:.1f}</code> ({pos['tp']:.2f})"
        )
    await update.message.reply_text("\n\n".join(lines), parse_mode="HTML")

async def cmd_tara(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id not in CHAT_IDS:
        return
    if is_scanning:
        await update.message.reply_text("⏳ Şu an arka planda bir tarama devam ediyor. Lütfen bitmesini bekleyin...")
        return
    await update.message.reply_text("🔄 Manuel tarama başlatıldı...")
    async def run_manual_scan():
        await tick(force=True)
        await send_all("✅ Tarama tamamlandı. Sinyal koşulu oluştuysa mesajınız gelmiştir.")
    task = asyncio.create_task(run_manual_scan())
    background_tasks.add(task)
    task.add_done_callback(background_tasks.discard)

# ==========================================
# TARAMA VE ZAMANLAYICI
# ==========================================
async def tick(force=False):
    global is_scanning
    if not force and not is_market_open():
        return
    if is_scanning:
        return
    is_scanning = True
    logger.info(f"🔄 Tarama başladı. (Zaman: {datetime.now(TIMEZONE).strftime('%H:%M')})")

    try:
        xu_trend = 0
        df_xu = await fetch_ohlc_async("XU030", "1h", limit=100)
        if df_xu is not None and len(df_xu) > 50:
            xu_ema50 = df_xu['close'].ewm(span=50, adjust=False).mean().iloc[-1]
            xu_trend = 1 if df_xu['close'].iloc[-1] > xu_ema50 else -1

        sem = asyncio.Semaphore(10)
        async def work(sym):
            async with sem:
                try:
                    df_1h = await fetch_ohlc_async(sym, "1h", limit=150)
                    await check_kosan_at(sym, xu_trend, df_1h)
                    return True
                except Exception as e:
                    logger.error(f"Tarama hatası ({sym}): {e}")
                    return False

        await asyncio.gather(*[work(s) for s in ALL_SYMBOLS])
    finally:
        is_scanning = False

async def clear_daily_state():
    async with STATE_LOCK:
        KOSAN_AT_TRADES.clear()
        KOSAN_LAST_SIGNAL.clear()
        DATA_CACHE.clear()
        save_trades()
    await send_all("🧹 <b>Gün Sonu Temizliği</b>\nSaat 18:30 oldu. Tüm açık işlemler ve takipler sıfırlandı. Yarın yeni sinyallerle devam edilecek.")

async def weekly_summary_and_cleanup():
    # Cuma günü 18:15'te haftalık özet, 18:30'da temizlik
    pass

async def market_open_scan():
    if not is_market_open():
        return
    await send_all("🟢 Seans açıldı, tarama ve takip başladı.")
    await send_all("Sistem her zaman senin için çalışıyor, kazanmak istiyorsan sadece sistemle kal... MZB 😉")
    await tick(force=True)

# ==========================================
# BOT BAŞLATMA
# ==========================================
bot_app = Application.builder().token(BOT_TOKEN).build()

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error(f"🚨 Telegram Hatası: {context.error}")
    if update and update.effective_chat:
        try:
            await context.bot.send_message(chat_id=update.effective_chat.id, text="⚠️ Beklenmeyen bir hata oluştu, lütfen tekrar deneyin.")
        except Exception:
            pass

bot_app.add_error_handler(error_handler)

auth_filter = filters.User(user_id=CHAT_IDS)
bot_app.add_handler(CommandHandler("start", cmd_start, filters=auth_filter))
bot_app.add_handler(CommandHandler(["help", "yardim", "durum"], cmd_help, filters=auth_filter))
bot_app.add_handler(CommandHandler("tara", cmd_tara, filters=auth_filter))
bot_app.add_handler(CommandHandler("pozlar", cmd_pozlar, filters=auth_filter))
bot_app.add_handler(CommandHandler("ozet", cmd_ozet, filters=auth_filter))
bot_app.add_handler(MessageHandler(filters.TEXT & auth_filter, analyze_symbol))

class DummyHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Alive")
    def do_HEAD(self):
        self.send_response(200)
        self.end_headers()
    def log_message(self, format, *args):
        return

def run_dummy_server():
    HTTPServer(("0.0.0.0", int(os.environ.get("PORT", 8080))), DummyHandler).serve_forever()

async def main():
    load_trades()
    threading.Thread(target=run_dummy_server, daemon=True).start()
    await bot_app.initialize()
    await bot_app.start()
    await bot_app.updater.start_polling(drop_pending_updates=True)

    sched = AsyncIOScheduler(timezone=TIMEZONE, job_defaults={'coalesce': True, 'max_instances': 1, 'misfire_grace_time': 60})

    sched.add_job(market_open_scan, CronTrigger(day_of_week="mon-fri", hour=9, minute=50, timezone=TIMEZONE))
    sched.add_job(tick, CronTrigger(day_of_week="mon-fri", hour="10-17", minute="1-59/5", timezone=TIMEZONE))
    sched.add_job(tick, CronTrigger(day_of_week="mon-fri", hour=18, minute=10, timezone=TIMEZONE), kwargs={'force': True})

    sched.add_job(clear_daily_state, CronTrigger(day_of_week="mon-fri", hour=18, minute=30, timezone=TIMEZONE))
    sched.start()
    logger.info("✅ Bot çalışıyor. Koşan At entegre edildi. 17:00 sonrası işlem yok.")
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())