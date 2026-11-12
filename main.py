import json, os
import asyncio
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from datetime import datetime, timedelta, time as dt_time
import time as time_module
from concurrent.futures import ThreadPoolExecutor
import numpy as np
import logging

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

CHAT_IDS_STR = os.environ.get("CHAT_IDS", "5066109900,8776376951")
try:
    CHAT_IDS = [int(cid.strip()) for cid in CHAT_IDS_STR.split(",")]
except ValueError:
    CHAT_IDS = [5066109900, 8776376951]

ADMIN_ID = CHAT_IDS[0] if CHAT_IDS else None

if not BOT_TOKEN:
    raise ValueError("❌ HATA: BOT_TOKEN bulunamadı!")

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

last_error_time = {}
async def send_admin_alert(error_msg, error_key="general"):
    global last_error_time
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

SYMBOLS = [
    "TOASO", "TRALT", "ASTOR", "GUBRF", "TRMET", "TCELL", "GARAN", "YKBNK", "TAVHL", "ENKAI",
    "TTKOM", "SAHOL", "SASA", "AEFES", "ASELS", "MGROS", "TUPRS", "AKBNK", "KCHOL", "KRDMD",
    "ISCTR", "THYAO", "BIMAS", "EREGL", "EKGYO", "PGSUS", "FROTO", "SISE", "VAKBN", "ALARK"
]

BIST_STOCKS = [s for s in SYMBOLS if not s.startswith("XU")]

SECTORS = {
    "🏦 Bankacılık": ["GARAN", "YKBNK", "AKBNK", "ISCTR", "VAKBN"],
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
# SİNYAL PARAMETRELERİ
# ==========================================
RSI_PERIOD = 8

# 1. RSI BÜYÜSÜ AYARLARI
INSTANT_SHORT = 75
INSTANT_LONG = 20
RSI_COOLDOWN_HOURS = 3   

# 2. ATR DÜNYASI AYARLARI
ALARM_SHORT_UPPER = 82
ALARM_LONG_LOWER = 22
ALARM_ATR_DROP = 1.0

TIME_FRAMES_RSI = ["1h"]
TIME_FRAMES_ATR = ["15m", "30m", "1h", "4h"]
FETCH_TFS = ["15m", "30m", "1h", "4h"]
TF_MINUTES = {"15m": 15, "30m": 30, "1h": 60, "4h": 240}

VOLUME_LOOKBACK = 20
VOLUME_MULT = 1.5
TP_PCT = 3.0
SL_PCT = 3.5
TIMEOUT_HOURS = 10

# 3. MÜKEMMEL FİBO AYARLARI
FIBO_LEVEL = 0.618
FIBO_SL = 2.0
MUK_FIBO_TRADES = {} 
MUK_FIBO_SETUPS = {} 

active_trades = {}
pending_trades = set()
instant_states = {}
atr_states = {}
earnings_cache = {}
fibo_cache = {}
pending_confirmations = {}
triple_ema_states = {}
active_triple_ema_trades = {}
pending_ema_crosses = {}
active_gap_hunter_trades = {}
gap_hunter_states = {}
is_scanning = False
background_tasks = set()

CACHE_5M = {}
CACHE_15M = {}
CACHE_TTL = 60

trade_lock = asyncio.Lock()

DATA_DIR = os.getenv("DATA_DIR", "/tmp")
TRADES_FILE = os.path.join(DATA_DIR, "active_trades.json")
TRIPLE_EMA_FILE = os.path.join(DATA_DIR, "active_triple_ema.json")
GAP_HUNTER_FILE = os.path.join(DATA_DIR, "active_gap_hunter.json")

EXECUTOR = ThreadPoolExecutor(max_workers=20)
FETCH_SEM = asyncio.Semaphore(16)

# ==========================================
# ATOMİK KAYIT VE YÜKLEME
# ==========================================
def save_trades():
    try:
        serializable_trades = {}
        for k, v in active_trades.items():
            trade_copy = v.copy()
            if isinstance(trade_copy.get("entry_time"), datetime): trade_copy["entry_time"] = trade_copy["entry_time"].isoformat()
            if isinstance(trade_copy.get("entered_time"), datetime): trade_copy["entered_time"] = trade_copy["entered_time"].isoformat()
            serializable_trades[k] = trade_copy
        with open(TRADES_FILE + ".tmp", "w", encoding="utf-8") as f:
            json.dump(serializable_trades, f, ensure_ascii=False, indent=2)
        os.replace(TRADES_FILE + ".tmp", TRADES_FILE)

        serializable_ema = {}
        for k, v in active_triple_ema_trades.items():
            trade_copy = v.copy()
            if isinstance(trade_copy.get("entry_time"), datetime): trade_copy["entry_time"] = trade_copy["entry_time"].isoformat()
            if isinstance(trade_copy.get("entered_time"), datetime): trade_copy["entered_time"] = trade_copy["entered_time"].isoformat()
            serializable_ema[k] = trade_copy
        with open(TRIPLE_EMA_FILE + ".tmp", "w", encoding="utf-8") as f:
            json.dump(serializable_ema, f, ensure_ascii=False, indent=2)
        os.replace(TRIPLE_EMA_FILE + ".tmp", TRIPLE_EMA_FILE)

        serializable_gap = {}
        for k, v in active_gap_hunter_trades.items():
            trade_copy = v.copy()
            if isinstance(trade_copy.get("entry_time"), datetime): trade_copy["entry_time"] = trade_copy["entry_time"].isoformat()
            if isinstance(trade_copy.get("entered_time"), datetime): trade_copy["entered_time"] = trade_copy["entered_time"].isoformat()
            serializable_gap[k] = trade_copy
        with open(GAP_HUNTER_FILE + ".tmp", "w", encoding="utf-8") as f:
            json.dump(serializable_gap, f, ensure_ascii=False, indent=2)
        os.replace(GAP_HUNTER_FILE + ".tmp", GAP_HUNTER_FILE)
    except Exception as e:
        logger.error(f"Trade kayıt hatası: {e}")

def load_trades():
    global active_trades, active_triple_ema_trades, active_gap_hunter_trades
    if os.path.exists(TRADES_FILE):
        try:
            with open(TRADES_FILE, "r", encoding="utf-8") as f:
                loaded = json.load(f)
                for k, v in loaded.items():
                    if "entry_time" in v and isinstance(v["entry_time"], str): v["entry_time"] = datetime.fromisoformat(v["entry_time"])
                    if "entered_time" in v and isinstance(v["entered_time"], str): v["entered_time"] = datetime.fromisoformat(v["entered_time"])
                    active_trades[k] = v
            logger.info(f"✅ {len(active_trades)} geçerli aktif ATR işlemi yüklendi.")
        except Exception as e: logger.error(f"Trade yükleme hatası: {e}")

    if os.path.exists(TRIPLE_EMA_FILE):
        try:
            with open(TRIPLE_EMA_FILE, "r", encoding="utf-8") as f:
                loaded = json.load(f)
                for k, v in loaded.items():
                    if "entry_time" in v and isinstance(v["entry_time"], str): v["entry_time"] = datetime.fromisoformat(v["entry_time"])
                    if "entered_time" in v and isinstance(v["entered_time"], str): v["entered_time"] = datetime.fromisoformat(v["entered_time"])
                    active_triple_ema_trades[k] = v
            logger.info(f"✅ {len(active_triple_ema_trades)} aktif Üçlü EMA işlemi yüklendi.")
        except Exception as e: logger.error(f"Üçlü EMA yükleme hatası: {e}")

    if os.path.exists(GAP_HUNTER_FILE):
        try:
            with open(GAP_HUNTER_FILE, "r", encoding="utf-8") as f:
                loaded = json.load(f)
                for k, v in loaded.items():
                    if "entry_time" in v and isinstance(v["entry_time"], str): v["entry_time"] = datetime.fromisoformat(v["entry_time"])
                    if "entered_time" in v and isinstance(v["entered_time"], str): v["entered_time"] = datetime.fromisoformat(v["entered_time"])
                    active_gap_hunter_trades[k] = v
            logger.info(f"✅ {len(active_gap_hunter_trades)} aktif Gap Avcısı işlemi yüklendi.")
        except Exception as e: logger.error(f"Gap Avcısı yükleme hatası: {e}")

# ==========================================
# YARDIMCI FONKSİYONLAR
# ==========================================
def is_arife(now):
    if now.month == 10 and now.day == 28: return True
    return False

def is_market_open(now=None):
    now = now or datetime.now(TIMEZONE)
    if now.weekday() > 4: return False
    if now.date() in TR_HOLIDAYS: return False
    if is_arife(now): return dt_time(9, 55) <= now.time() <= dt_time(13, 0)
    return dt_time(9, 55) <= now.time() <= dt_time(18, 10)

def is_gap_hunter_time(now=None):
    now = now or datetime.now(TIMEZONE)
    if now.weekday() > 4 or now.date() in TR_HOLIDAYS: return False
    if is_arife(now): return dt_time(16, 0) <= now.time() <= dt_time(17, 40)
    return dt_time(16, 0) <= now.time() <= dt_time(17, 40)

def get_effective_market_hours(entry_time, now_time):
    if now_time <= entry_time: return 0.0
    total_hours = 0.0
    current = entry_time
    market_open = dt_time(10, 0); market_close = dt_time(18, 0)
    while current.date() <= now_time.date():
        if current.weekday() > 4 or current.date() in TR_HOLIDAYS:
            current = TIMEZONE.localize(datetime.combine(current.date() + timedelta(days=1), market_open))
            continue
        day_close = dt_time(13, 0) if is_arife(current) else market_close
        day_open = TIMEZONE.localize(datetime.combine(current.date(), market_open))
        day_close_tz = TIMEZONE.localize(datetime.combine(current.date(), day_close))
        if now_time < day_open: break
        start_of_period = max(current, day_open); end_of_period = min(now_time, day_close_tz)
        if end_of_period > start_of_period: total_hours += (end_of_period - start_of_period).total_seconds() / 3600
        current = TIMEZONE.localize(datetime.combine(current.date() + timedelta(days=1), market_open))
    return total_hours

def _get_earnings_sync(symbol):
    today = datetime.now(TIMEZONE).date()
    try:
        ticker = bp.Ticker(symbol)
        cal = ticker.calendar
        if not isinstance(cal, pd.DataFrame) or cal.empty: return ""
        rapor_satirlari = cal[cal['Subject'].str.contains('Finansal Rapor', case=False, na=False)]
        if rapor_satirlari.empty: return ""
        tarih_str = rapor_satirlari.iloc[0]['EndDate']
        earnings_date = datetime.strptime(tarih_str, "%d.%m.%Y").date()
        days_left = (earnings_date - today).days
        if 0 <= days_left <= 5: return f"\n\n📅 <b>Bilanço Uyarısı:</b> Bilançoya {days_left} gün kaldı! (Dikkatli olun)"
    except Exception as e: logger.debug(f"Bilanço verisi alınamadı ({symbol}): {e}")
    return ""

async def get_earnings_note_async(symbol):
    today = datetime.now(TIMEZONE).date()
    cache_key = f"{symbol}_{today}"
    if cache_key in earnings_cache: return earnings_cache[cache_key]
    try:
        note = await asyncio.wait_for(asyncio.to_thread(_get_earnings_sync, symbol), timeout=20.0)
        if note: earnings_cache[cache_key] = note
        return note
    except asyncio.TimeoutError: return ""

def _get_fibo_sync(symbol, current_price):
    try:
        df = bp.Ticker(symbol).history(period="5 bar", interval="1d")
        if df is None or len(df) < 2: return ""
        df.columns = [str(c).lower() for c in df.columns]
        today = datetime.now(TIMEZONE).date()
        df_gecmis = df[df.index.date < today]
        if df_gecmis.empty: return ""
        onceki_gun = df_gecmis.iloc[-1]
        H, L, C = onceki_gun['high'], onceki_gun['low'], onceki_gun['close']
        pivot = (H + L + C) / 3; fark = H - L
        raw_levels = []
        for r in [0.236, 0.382, 0.5, 0.618, 1.0, 1.272, 1.618, 2.618]:
            raw_levels.append((pivot - fark * r, str(r)))
            raw_levels.append((pivot + fark * r, str(r)))
        yakin_seviyeler = [l for l in raw_levels if abs(l[0] - current_price) / current_price < 0.05]
        if not yakin_seviyeler: return ""
        en_yakin_seviye = min(yakin_seviyeler, key=lambda x: abs(x[0] - current_price))
        note = "\n🎯 <b>FİBO SEVİYELERİ (Günlük):</b>"
        if en_yakin_seviye[1] in ["0.5", "0.618"]:
            lvl_05 = min((l for l in yakin_seviyeler if l[1] == "0.5"), key=lambda x: abs(x[0] - current_price), default=None)
            lvl_0618 = min((l for l in yakin_seviyeler if l[1] == "0.618"), key=lambda x: abs(x[0] - current_price), default=None)
            if lvl_05 and lvl_0618: note += f"\n🎯 <b>0.5 Seviyesi:</b> {lvl_05[0]:,.2f}\n🎯 <b>0.618 Seviyesi:</b> {lvl_0618[0]:,.2f}"
            else: note += f"\n🎯 <b>En Yakın Seviye ({en_yakin_seviye[1]}):</b> {en_yakin_seviye[0]:,.2f}"
        else: note += f"\n🎯 <b>En Yakın Seviye ({en_yakin_seviye[1]}):</b> {en_yakin_seviye[0]:,.2f}"
        return note + "\n"
    except Exception as e: logger.debug(f"Fibo hatası ({symbol}): {e}"); return ""

async def get_nearest_fibo_levels_async(symbol, current_price):
    today = datetime.now(TIMEZONE).date()
    cache_key = f"{symbol}_{today}_{int(current_price)}"
    if cache_key in fibo_cache: return fibo_cache[cache_key]
    try:
        note = await asyncio.wait_for(asyncio.to_thread(_get_fibo_sync, symbol, current_price), timeout=20.0)
        if note: fibo_cache[cache_key] = note
        return note
    except asyncio.TimeoutError: return ""

def _get_daily_pivots_sync(symbol):
    try:
        df = bp.Ticker(symbol).history(period="5d", interval="1d")
        if df is None or len(df) < 2: return None
        df.columns = [str(c).lower() for c in df.columns]
        today = datetime.now(TIMEZONE).date()
        df_gecmis = df[df.index.date < today]
        if df_gecmis.empty: return None
        
        yesterday = df_gecmis.iloc[-1]
        H, L, C = yesterday['high'], yesterday['low'], yesterday['close']
        PP = (H + L + C) / 3
        Range = H - L
        
        R_0618 = PP + (Range * 0.618)
        S_0618 = PP - (Range * 0.618)
        return {"PP": PP, "R_0618": R_0618, "S_0618": S_0618}
    except Exception as e:
        logger.error(f"Günlük Pivot çekme hatası ({symbol}): {e}")
        return None

# ==========================================
# MATEMATİK
# ==========================================
def get_rsi(series, period=8):
    delta = series.diff(); gain = delta.where(delta > 0, 0); loss = -delta.where(delta < 0, 0)
    avg_gain = gain.ewm(alpha=1/period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1/period, adjust=False, min_periods=period).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def get_atr_series(df, period=10):
    if df is None or len(df) < period + 1: return pd.Series(index=df.index, dtype=float)
    tr1 = df['high'] - df['low']; tr2 = (df['high'] - df['close'].shift()).abs(); tr3 = (df['low'] - df['close'].shift()).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return tr.ewm(alpha=1/period, adjust=False, min_periods=period).mean()

def calculate_supertrend(df, period=10, multiplier=3):
    if df is None or len(df) < period + 1: return pd.Series(index=df.index, dtype=float)
    hl2 = (df['high'] + df['low']) / 2; atr = get_atr_series(df, period)
    upperband = hl2 + (multiplier * atr); lowerband = hl2 - (multiplier * atr)
    final_upperband = upperband.copy(); final_lowerband = lowerband.copy()
    supertrend = pd.Series(index=df.index, dtype=float); trend = pd.Series(index=df.index, dtype=int)
    trend.iloc[0] = 1
    for i in range(1, len(df)):
        if pd.isna(atr.iloc[i]): supertrend.iloc[i] = np.nan; trend.iloc[i] = 1; continue
        if df['close'].iloc[i-1] > final_upperband.iloc[i-1]: final_upperband.iloc[i] = upperband.iloc[i]
        else: final_upperband.iloc[i] = min(upperband.iloc[i], final_upperband.iloc[i-1])
        if df['close'].iloc[i-1] < final_lowerband.iloc[i-1]: final_lowerband.iloc[i] = lowerband.iloc[i]
        else: final_lowerband.iloc[i] = max(lowerband.iloc[i], final_lowerband.iloc[i-1])
        if df['close'].iloc[i] > final_upperband.iloc[i-1]: trend.iloc[i] = 1
        elif df['close'].iloc[i] < final_lowerband.iloc[i-1]: trend.iloc[i] = -1
        else: trend.iloc[i] = trend.iloc[i-1]
        if trend.iloc[i] == 1: supertrend.iloc[i] = final_lowerband.iloc[i]
        else: supertrend.iloc[i] = final_upperband.iloc[i]
    return supertrend

def fetch_ohlc(symbol, interval, limit=500):
    for attempt in range(3):
        try:
            df = bp.Ticker(symbol).history(period=f"{limit + 2} bar", interval=interval)
            if df is not None and not df.empty:
                df.columns = [str(c).lower() for c in df.columns]
                if not isinstance(df.index, pd.DatetimeIndex): df.index = pd.to_datetime(df.index)
                if df.index.tz is None: df.index = df.index.tz_localize(TIMEZONE)
                else: df.index = df.index.tz_convert(TIMEZONE)
                now = datetime.now(TIMEZONE); last_bar_time = df.index[-1]
                tf_mins = {"15m": 15, "30m": 30, "1h": 60, "4h": 240, "5m": 5, "1d": 1440}.get(interval, 0)
                if tf_mins and now < last_bar_time + timedelta(minutes=tf_mins): return df.iloc[:-1]
                return df
        except Exception as e: 
            logger.error(f"Veri çekme hatası ({symbol} {interval}): {e}")
            time_module.sleep(1)
    return None

async def fetch_ohlc_async(symbol, interval, limit=500):
    if interval in ["5m", "15m"]:
        cache = CACHE_5M if interval == "5m" else CACHE_15M
        cache_key = f"{symbol}_{interval}"
        if cache_key in cache:
            cached_df, cache_time = cache[cache_key]
            if time_module.time() - cache_time < CACHE_TTL: return cached_df.copy()
    try:
        async with FETCH_SEM:
            loop = asyncio.get_running_loop()
            df = await asyncio.wait_for(loop.run_in_executor(EXECUTOR, fetch_ohlc, symbol, interval, limit), timeout=15.0)
            if df is not None and interval in ["5m", "15m"]:
                cache = CACHE_5M if interval == "5m" else CACHE_15M
                cache[f"{symbol}_{interval}"] = (df, time_module.time())
            return df
    except asyncio.TimeoutError: logger.warning(f"Timeout: {symbol} {interval}"); return None
    except Exception as e: logger.error(f"Async Fetch Hatası ({symbol}): {e}"); return None

async def fetch_all_tfs_async(symbol):
    results = await asyncio.gather(*[fetch_ohlc_async(symbol, tf) for tf in FETCH_TFS])
    return dict(zip(FETCH_TFS, results))

# ==========================================
# SİNYAL MOTORLARI
# ==========================================
def get_volume_and_trend_tag(df_current, df_4h):
    vol_ma = df_current['volume'].rolling(VOLUME_LOOKBACK).mean().shift(1)
    current_vol = df_current['volume'].iloc[-1]; avg_vol = vol_ma.iloc[-1]
    vol_tag = "⚠️ <b>Hacimsiz</b>"
    if pd.notna(avg_vol) and avg_vol > 0 and current_vol >= (avg_vol * VOLUME_MULT): vol_tag = "🔥 <b>Hacim Onaylı</b>"
    trend_tag = ""
    if df_4h is not None and len(df_4h) > 200:
        ema50_4h = df_4h['close'].ewm(span=50, adjust=False).mean().iloc[-1]
        ema200_4h = df_4h['close'].ewm(span=200, adjust=False).mean().iloc[-1]
        trend_tag = "📈 <b>4H Trend: Yukarı</b>" if ema50_4h > ema200_4h else "📉 <b>4H Trend: Aşağı</b>"
    return f"{vol_tag} | {trend_tag}"

async def process_rsi_signals(symbol, dfs):
    msgs = []
    df_4h = dfs.get("4h")
    for tf in TIME_FRAMES_RSI:
        df = dfs.get(tf)
        if df is None or len(df) < 30: continue
        rsi = get_rsi(df["close"], period=RSI_PERIOD)
        if rsi is None or len(rsi) < 2: continue
        
        r_now = float(rsi.iloc[-1])
        bar_time = df.index[-1]
        gecikme_saniye = (datetime.now(TIMEZONE) - bar_time).total_seconds()
        
        direction = None
        if r_now >= INSTANT_SHORT: direction = "SHORT"
        elif r_now <= INSTANT_LONG: direction = "LONG"
        
        if direction:
            inst_key = f"inst_{symbol}_{direction}_{tf}"
            last_signal_bar = instant_states.get(inst_key)
            if last_signal_bar is not None and (bar_time - last_signal_bar) < timedelta(hours=RSI_COOLDOWN_HOURS):
                continue
            instant_states[inst_key] = bar_time

            tags = get_volume_and_trend_tag(df, df_4h); earnings_note = await get_earnings_note_async(symbol)
            current_price = float(df['close'].iloc[-1]); fibo_note = await get_nearest_fibo_levels_async(symbol, current_price)
            if direction == "SHORT":
                msg = f"✨ <b>RSI BÜYÜSÜ (1H)</b> ✨\n⚠️ <b>{symbol}</b> — AŞIRI ALIM BÖLGESİ\nRSI 8: {r_now:.1f} (75 Üzeri)\n📊 {tags}\n\n🚨 <b>Alarm Fiyatı:</b> {current_price:,.2f}\n🎯 <b>Olası Giriş:</b> {current_price * 1.02:,.2f} (Tahmini +%2)"
            else:
                msg = f"✨ <b>RSI BÜYÜSÜ (1H)</b> ✨\n⚠️ <b>{symbol}</b> — AŞIRI SATIM BÖLGESİ\nRSI 8: {r_now:.1f} (20 Altı)\n📊 {tags}\n\n🚨 <b>Alarm Fiyatı:</b> {current_price:,.2f}\n🎯 <b>Olası Giriş:</b> {current_price * 0.98:,.2f} (Tahmini -%2)"
            msg += fibo_note + "\n👀 <b>Grafiği kontrol edin. Teyit sinyali bekleniyor...</b>" + earnings_note
            msg += f"\n\n⏱ <b>Veri Gecikmesi:</b> {int(gecikme_saniye)} saniye" 
            msgs.append({"text": msg, "symbol": symbol, "tf": tf, "type": "alert_instant", "direction": direction})
    return msgs

async def process_atr_signals(symbol, dfs):
    msgs = []
    df_4h = dfs.get("4h")
    for tf in TIME_FRAMES_ATR:
        df = dfs.get(tf)
        if df is None or len(df) < 30: continue
        rsi = get_rsi(df["close"], period=RSI_PERIOD)
        if rsi is None or len(rsi) < 2: continue
        atr_series = get_atr_series(df, period=10)
        
        atr_key = f"atr_{symbol}_{tf}"
        if atr_key not in atr_states: 
            atr_states[atr_key] = {"state": "IDLE", "peak_rsi": 0, "peak_price": 0, "low_rsi": 100, "low_price": 0, "last_bar": None, "reject_count": 0}
        atr_data = atr_states[atr_key]
        
        if atr_data["last_bar"] == df.index[-1]:
            continue
        
        trade_key_short = f"{symbol}_SHORT_atr_{tf}"
        trade_key_long = f"{symbol}_LONG_atr_{tf}"
        if trade_key_short in active_trades or trade_key_long in active_trades:
            continue
            
        start_idx = len(df) - 1
        for i in range(start_idx, len(df)):
            idx = df.index[i]
            row = df.iloc[i]
            
            r_now = rsi.loc[idx] if idx in rsi.index else get_rsi(df.loc[:idx, 'close'], period=RSI_PERIOD).iloc[-1]
            if pd.isna(r_now): continue
            
            close_price = row['close']; candle_high = row['high']; candle_low = row['low']; state = atr_data["state"]
            atr_val = atr_series.loc[idx] if idx in atr_series.index else None
            if pd.isna(atr_val) or atr_val is None or atr_val <= 0: continue
            
            if state == "IDLE":
                if r_now > ALARM_SHORT_UPPER: atr_data.update({"state": "SHORT_ARMED", "peak_rsi": r_now, "peak_price": candle_high, "reject_count": 0})
                elif r_now < ALARM_LONG_LOWER: atr_data.update({"state": "LONG_ARMED", "low_rsi": r_now, "low_price": candle_low, "reject_count": 0})
            elif state == "SHORT_ARMED":
                if r_now > atr_data["peak_rsi"]: atr_data["peak_rsi"] = r_now; atr_data["peak_price"] = candle_high
                price_ok = (atr_data["peak_price"] - close_price) >= atr_val * ALARM_ATR_DROP; candle_ok = close_price < row['open']
                if price_ok and candle_ok:
                    gecikme_saniye = (datetime.now(TIMEZONE) - idx).total_seconds()
                    tags = get_volume_and_trend_tag(df, df_4h); earnings_note = await get_earnings_note_async(symbol); fibo_note = await get_nearest_fibo_levels_async(symbol, close_price)
                    entry_time = idx + timedelta(minutes=TF_MINUTES[tf])
                    sl = close_price * (1 + SL_PCT / 100); tp = close_price * (1 - TP_PCT / 100)
                    msg = f"✨ <b>ATR DÜNYASI (Short) - {symbol} [{tf.upper()}]</b> ✨\n🔴 RSI Aşırı Alım Zirvesi: {atr_data['peak_rsi']:.1f}\n📊 {tags}\n\n💰 <b>Olası Giriş:</b> {close_price:,.2f}\n🛑 <b>Stop:</b> {sl:,.2f} (+%{SL_PCT})\n🎯 <b>TP (%{TP_PCT}):</b> {tp:,.2f}\n⏳ <i>Zaman Aşımı: {TIMEOUT_HOURS} Seans Saati</i>"
                    msg += fibo_note; msg += earnings_note
                    msg += f"\n\n⏱ <b>Veri Gecikmesi:</b> {int(gecikme_saniye)} saniye" 
                    msgs.append({"text": msg, "symbol": symbol, "tf": tf, "type": "atr_dunyasi", "direction": "SHORT", "price": close_price, "sl": sl, "tp": tp, "bar_time": entry_time})
                    atr_data["state"] = "IDLE"
                    break 
                else:
                    atr_data["reject_count"] += 1
                    if atr_data["reject_count"] >= 20 or r_now < 60: atr_data.update({"state": "IDLE", "reject_count": 0})
            elif state == "LONG_ARMED":
                if r_now < atr_data["low_rsi"]: atr_data["low_rsi"] = r_now; atr_data["low_price"] = candle_low
                price_ok = (close_price - atr_data["low_price"]) >= atr_val * ALARM_ATR_DROP; candle_ok = close_price > row['open']
                if price_ok and candle_ok:
                    gecikme_saniye = (datetime.now(TIMEZONE) - idx).total_seconds()
                    tags = get_volume_and_trend_tag(df, df_4h); earnings_note = await get_earnings_note_async(symbol); fibo_note = await get_nearest_fibo_levels_async(symbol, close_price)
                    entry_time = idx + timedelta(minutes=TF_MINUTES[tf])
                    sl = close_price * (1 - SL_PCT / 100); tp = close_price * (1 + TP_PCT / 100)
                    msg = f"✨ <b>ATR DÜNYASI (Long) - {symbol} [{tf.upper()}]</b> ✨\n🟢 RSI Aşırı Satım Dibi: {atr_data['low_rsi']:.1f}\n📊 {tags}\n\n💰 <b>Olası Giriş:</b> {close_price:,.2f}\n🛑 <b>Stop:</b> {sl:,.2f} (-%{SL_PCT})\n🎯 <b>TP (%{TP_PCT}):</b> {tp:,.2f}\n⏳ <i>Zaman Aşımı: {TIMEOUT_HOURS} Seans Saati</i>"
                    msg += fibo_note; msg += earnings_note
                    msg += f"\n\n⏱ <b>Veri Gecikmesi:</b> {int(gecikme_saniye)} saniye" 
                    msgs.append({"text": msg, "symbol": symbol, "tf": tf, "type": "atr_dunyasi", "direction": "LONG", "price": close_price, "sl": sl, "tp": tp, "bar_time": entry_time})
                    atr_data["state"] = "IDLE"
                    break
                else:
                    atr_data["reject_count"] += 1
                    if atr_data["reject_count"] >= 20 or r_now > 40: atr_data.update({"state": "IDLE", "reject_count": 0})
        
        atr_data["last_bar"] = df.index[-1]

    return msgs

# ==========================================
# MÜKEMMEL FİBO MOTORU
# ==========================================
async def check_mukemmel_fibo():
    if not is_market_open(): return
    now = datetime.now(TIMEZONE)
    
    sem = asyncio.Semaphore(10)
    async def scan_single_fibo(sym):
        async with sem:
            try:
                pivots = await asyncio.to_thread(_get_daily_pivots_sync, sym)
                if not pivots: return
                
                R_0618 = pivots['R_0618']
                S_0618 = pivots['S_0618']
                
                df_1h = await fetch_ohlc_async(sym, "1h", limit=100)
                df_4h = await fetch_ohlc_async(sym, "4h", limit=100)
                
                if df_1h is None or df_4h is None: return
                
                # --- 4H KRAL ---
                rsi_4h = get_rsi(df_4h['close'], 14).iloc[-1]
                current_price_4h = float(df_4h['close'].iloc[-1])
                
                current_high_4h = float(df_4h['high'].iloc[-1])
                current_low_4h = float(df_4h['low'].iloc[-1])
                
                is_4h_short_ready = rsi_4h >= 75
                is_4h_long_ready = rsi_4h <= 25
                
                setup_key_4h_short = f"setup_4h_{sym}_SHORT"
                setup_key_4h_long = f"setup_4h_{sym}_LONG"
                
                if is_4h_short_ready and current_price_4h < R_0618 and setup_key_4h_short not in MUK_FIBO_SETUPS:
                    msg = (f"⚠️ <b>MÜKEMMEL FİBO HAZIRLIK [4H KRAL]</b> ⚠️\n"
                           f"📉 <b>{sym}</b> — RSI(14) aşırı alımda: <b>{rsi_4h:.1f}</b>\n"
                           f"Fiyat: {current_price_4h:,.2f}\n\n"
                           f"🎯 <b>Dikkat Edilecek Direnç (0.618 Fibo):</b> {R_0618:,.2f}\n"
                           f"💡 <i>Fiyat bu seviyeye iğne attığında SHORT sinyali üretilecek.</i>")
                    for cid in CHAT_IDS: await bot_app.bot.send_message(chat_id=cid, text=msg, parse_mode="HTML")
                    MUK_FIBO_SETUPS[setup_key_4h_short] = True
                    
                if is_4h_long_ready and current_price_4h > S_0618 and setup_key_4h_long not in MUK_FIBO_SETUPS:
                    msg = (f"⚠️ <b>MÜKEMMEL FİBO HAZIRLIK [4H KRAL]</b> ⚠️\n"
                           f"📈 <b>{sym}</b> — RSI(14) aşırı satımda: <b>{rsi_4h:.1f}</b>\n"
                           f"Fiyat: {current_price_4h:,.2f}\n\n"
                           f"🎯 <b>Dikkat Edilecek Destek (0.618 Fibo):</b> {S_0618:,.2f}\n"
                           f"💡 <i>Fiyat bu seviyeye iğne attığında LONG sinyali üretilecek.</i>")
                    for cid in CHAT_IDS: await bot_app.bot.send_message(chat_id=cid, text=msg, parse_mode="HTML")
                    MUK_FIBO_SETUPS[setup_key_4h_long] = True

                sig_key_4h_short = f"sig_4h_{sym}_SHORT"
                if is_4h_short_ready and (current_high_4h >= R_0618 or current_price_4h >= R_0618):
                    if sig_key_4h_short not in MUK_FIBO_TRADES or (now - MUK_FIBO_TRADES[sig_key_4h_short]).total_seconds() > 7200:
                        sl = current_price_4h * (1 + FIBO_SL / 100)
                        tp = current_price_4h * (1 - 3.0 / 100)
                        msg = (f"🔴 <b>MÜKEMMEL FİBO SİNYALİ [4H KRAL]</b> 🔴\n"
                               f"📉 <b>{sym}</b> — SHORT (Tepe Avı)\n"
                               f"Fiyat 0.618 Direncine Değdi: <b>{R_0618:,.2f}</b>\n"
                               f"RSI(14): {rsi_4h:.1f} (Aşırı Alım)\n\n"
                               f"💰 <b>Giriş:</b> {current_price_4h:,.2f}\n"
                               f"🛑 <b>Stop (SL %2):</b> {sl:,.2f}\n"
                               f"🎯 <b>Hedef (TP %3):</b> {tp:,.2f}\n"
                               f"💡 <i>4H Kral Stratejisi - Fiyat dirençten iğne attı.</i>")
                        for cid in CHAT_IDS: await bot_app.bot.send_message(chat_id=cid, text=msg, parse_mode="HTML")
                        MUK_FIBO_TRADES[sig_key_4h_short] = now
                        if setup_key_4h_short in MUK_FIBO_SETUPS: del MUK_FIBO_SETUPS[setup_key_4h_short]

                sig_key_4h_long = f"sig_4h_{sym}_LONG"
                if is_4h_long_ready and (current_low_4h <= S_0618 or current_price_4h <= S_0618):
                    if sig_key_4h_long not in MUK_FIBO_TRADES or (now - MUK_FIBO_TRADES[sig_key_4h_long]).total_seconds() > 7200:
                        sl = current_price_4h * (1 - FIBO_SL / 100)
                        tp = current_price_4h * (1 + 3.0 / 100)
                        msg = (f"🟢 <b>MÜKEMMEL FİBO SİNYALİ [4H KRAL]</b> 🟢\n"
                               f"📈 <b>{sym}</b> — LONG (Dip Avı)\n"
                               f"Fiyat 0.618 Desteğe Değdi: <b>{S_0618:,.2f}</b>\n"
                               f"RSI(14): {rsi_4h:.1f} (Aşırı Satım)\n\n"
                               f"💰 <b>Giriş:</b> {current_price_4h:,.2f}\n"
                               f"🛑 <b>Stop (SL %2):</b> {sl:,.2f}\n"
                               f"🎯 <b>Hedef (TP %3):</b> {tp:,.2f}\n"
                               f"💡 <i>4H Kral Stratejisi - Fiyat destekten iğne attı.</i>")
                        for cid in CHAT_IDS: await bot_app.bot.send_message(chat_id=cid, text=msg, parse_mode="HTML")
                        MUK_FIBO_TRADES[sig_key_4h_long] = now
                        if setup_key_4h_long in MUK_FIBO_SETUPS: del MUK_FIBO_SETUPS[setup_key_4h_long]

                # --- 1H FIRSATÇI ---
                rsi_1h = get_rsi(df_1h['close'], 8).iloc[-1]
                current_price_1h = float(df_1h['close'].iloc[-1])
                
                current_high_1h = float(df_1h['high'].iloc[-1])
                current_low_1h = float(df_1h['low'].iloc[-1])
                
                is_1h_short_ready = rsi_1h >= 70
                is_1h_long_ready = rsi_1h <= 30
                
                setup_key_1h_short = f"setup_1h_{sym}_SHORT"
                setup_key_1h_long = f"setup_1h_{sym}_LONG"
                
                if is_1h_short_ready and current_price_1h < R_0618 and setup_key_1h_short not in MUK_FIBO_SETUPS:
                    msg = (f"⚠️ <b>MÜKEMMEL FİBO HAZIRLIK [1H FIRSATÇI]</b> ⚠️\n"
                           f"📉 <b>{sym}</b> — RSI(8) aşırı alımda: <b>{rsi_1h:.1f}</b>\n"
                           f"Fiyat: {current_price_1h:,.2f}\n\n"
                           f"🎯 <b>Dikkat Edilecek Direnç (0.618 Fibo):</b> {R_0618:,.2f}\n"
                           f"💡 <i>Fiyat bu seviyeye iğne attığında SHORT sinyali üretilecek.</i>")
                    for cid in CHAT_IDS: await bot_app.bot.send_message(chat_id=cid, text=msg, parse_mode="HTML")
                    MUK_FIBO_SETUPS[setup_key_1h_short] = True
                    
                if is_1h_long_ready and current_price_1h > S_0618 and setup_key_1h_long not in MUK_FIBO_SETUPS:
                    msg = (f"⚠️ <b>MÜKEMMEL FİBO HAZIRLIK [1H FIRSATÇI]</b> ⚠️\n"
                           f"📈 <b>{sym}</b> — RSI(8) aşırı satımda: <b>{rsi_1h:.1f}</b>\n"
                           f"Fiyat: {current_price_1h:,.2f}\n\n"
                           f"🎯 <b>Dikkat Edilecek Destek (0.618 Fibo):</b> {S_0618:,.2f}\n"
                           f"💡 <i>Fiyat bu seviyeye iğne attığında LONG sinyali üretilecek.</i>")
                    for cid in CHAT_IDS: await bot_app.bot.send_message(chat_id=cid, text=msg, parse_mode="HTML")
                    MUK_FIBO_SETUPS[setup_key_1h_long] = True

                sig_key_1h_short = f"sig_1h_{sym}_SHORT"
                if is_1h_short_ready and (current_high_1h >= R_0618 or current_price_1h >= R_0618):
                    if sig_key_1h_short not in MUK_FIBO_TRADES or (now - MUK_FIBO_TRADES[sig_key_1h_short]).total_seconds() > 7200:
                        sl = current_price_1h * (1 + FIBO_SL / 100)
                        tp = current_price_1h * (1 - 5.0 / 100)
                        msg = (f"🔴 <b>MÜKEMMEL FİBO SİNYALİ [1H FIRSATÇI]</b> 🔴\n"
                               f"📉 <b>{sym}</b> — SHORT (Tepe Avı)\n"
                               f"Fiyat 0.618 Direncine Değdi: <b>{R_0618:,.2f}</b>\n"
                               f"RSI(8): {rsi_1h:.1f} (Aşırı Alım)\n\n"
                               f"💰 <b>Giriş:</b> {current_price_1h:,.2f}\n"
                               f"🛑 <b>Stop (SL %2):</b> {sl:,.2f}\n"
                               f"🎯 <b>Hedef (TP %5):</b> {tp:,.2f}\n"
                               f"💡 <i>1H Fırsatçı Stratejisi - Fiyat dirençten iğne attı.</i>")
                        for cid in CHAT_IDS: await bot_app.bot.send_message(chat_id=cid, text=msg, parse_mode="HTML")
                        MUK_FIBO_TRADES[sig_key_1h_short] = now
                        if setup_key_1h_short in MUK_FIBO_SETUPS: del MUK_FIBO_SETUPS[setup_key_1h_short]

                sig_key_1h_long = f"sig_1h_{sym}_LONG"
                if is_1h_long_ready and (current_low_1h <= S_0618 or current_price_1h <= S_0618):
                    if sig_key_1h_long not in MUK_FIBO_TRADES or (now - MUK_FIBO_TRADES[sig_key_1h_long]).total_seconds() > 7200:
                        sl = current_price_1h * (1 - FIBO_SL / 100)
                        tp = current_price_1h * (1 + 5.0 / 100)
                        msg = (f"🟢 <b>MÜKEMMEL FİBO SİNYALİ [1H FIRSATÇI]</b> 🟢\n"
                               f"📈 <b>{sym}</b> — LONG (Dip Avı)\n"
                               f"Fiyat 0.618 Desteğe Değdi: <b>{S_0618:,.2f}</b>\n"
                               f"RSI(8): {rsi_1h:.1f} (Aşırı Satım)\n\n"
                               f"💰 <b>Giriş:</b> {current_price_1h:,.2f}\n"
                               f"🛑 <b>Stop (SL %2):</b> {sl:,.2f}\n"
                               f"🎯 <b>Hedef (TP %5):</b> {tp:,.2f}\n"
                               f"💡 <i>1H Fırsatçı Stratejisi - Fiyat destekten iğne attı.</i>")
                        for cid in CHAT_IDS: await bot_app.bot.send_message(chat_id=cid, text=msg, parse_mode="HTML")
                        MUK_FIBO_TRADES[sig_key_1h_long] = now
                        if setup_key_1h_long in MUK_FIBO_SETUPS: del MUK_FIBO_SETUPS[setup_key_1h_long]

            except Exception as e:
                logger.error(f"Mükemmel Fibo Hatası ({sym}): {e}")

    tasks = [scan_single_fibo(sym) for sym in SYMBOLS]
    await asyncio.gather(*tasks)


# ==========================================
# ATR İŞLEM TAKİBİ
# ==========================================
async def check_active_trades():
    global active_trades
    if not active_trades: return
    sem = asyncio.Semaphore(10)
    
    async def check_single_trade(trade_key, trade):
        async with sem:
            try:
                sym = trade["symbol"]; direction = trade["type"]; tf = trade.get("tf", "15m")
                df = await fetch_ohlc_async(sym, tf, limit=100)
                if df is None or len(df) < 2: return
                
                entry_time = trade["entry_time"]
                if isinstance(entry_time, str): entry_time = datetime.fromisoformat(entry_time)
                if entry_time.tzinfo is None: entry_time = TIMEZONE.localize(entry_time)
                
                df_valid = df[df.index >= entry_time]
                if df_valid.empty: return
                
                max_high = float(df_valid['high'].max())
                min_low = float(df_valid['low'].min())
                last_close = float(df_valid['close'].iloc[-1])
                
                entry = trade["entry"]; sl = trade["sl"]; tp = trade["tp"]
                closed = False; pnl_pct = 0; close_reason = ""
                
                if direction == "LONG":
                    if min_low <= sl:
                        pnl_pct = ((sl - entry) / entry) * 100; close_reason = f"🛑 <b>ATR STOP - {sym} [{tf.upper()}]</b>\nFiyat Stop seviyesine ({sl:,.2f}) değdi.\n<b>Net K/Z:</b> %{pnl_pct:+.2f}"; closed = True
                    elif max_high >= tp:
                        pnl_pct = ((tp - entry) / entry) * 100; close_reason = f"🎯 <b>ATR TP - {sym} [{tf.upper()}]</b>\nFiyat Hedef seviyesine ({tp:,.2f}) ulaştı.\n<b>Net K/Z:</b> %{pnl_pct:+.2f}"; closed = True
                else:
                    if max_high >= sl:
                        pnl_pct = ((entry - sl) / entry) * 100; close_reason = f"🛑 <b>ATR STOP - {sym} [{tf.upper()}]</b>\nFiyat Stop seviyesine ({sl:,.2f}) değdi.\n<b>Net K/Z:</b> %{pnl_pct:+.2f}"; closed = True
                    elif min_low <= tp:
                        pnl_pct = ((entry - tp) / entry) * 100; close_reason = f"🎯 <b>ATR TP - {sym} [{tf.upper()}]</b>\nFiyat Hedef seviyesine ({tp:,.2f}) ulaştı.\n<b>Net K/Z:</b> %{pnl_pct:+.2f}"; closed = True
                        
                if not closed:
                    now = datetime.now(TIMEZONE); hours_passed = get_effective_market_hours(entry_time, now)
                    if hours_passed >= TIMEOUT_HOURS:
                        if direction == "LONG": pnl_pct = ((last_close - entry) / entry) * 100
                        else: pnl_pct = ((entry - last_close) / entry) * 100
                        close_reason = f"⏰ <b>ATR ZAMAN AŞIMI - {sym} [{tf.upper()}]</b>\n{TIMEOUT_HOURS} seans saati doldu.\n<b>Net K/Z:</b> %{pnl_pct:+.2f}"; closed = True
                        
                if closed:
                    for cid in CHAT_IDS:
                        try: await bot_app.bot.send_message(chat_id=cid, text=close_reason, parse_mode="HTML")
                        except Exception as e: logger.error(f"İşlem kapatma mesaj hatası: {e}")
                    if trade_key in active_trades: del active_trades[trade_key]
                    save_trades()
                    
                    atr_key = f"atr_{sym}_{tf}"
                    if atr_key in atr_states:
                        atr_states[atr_key]["last_bar"] = None 
                        atr_states[atr_key]["state"] = "IDLE" 
            except Exception as e:
                logger.error(f"ATR trade kontrol hatası ({trade_key}): {e}")

    tasks = [check_single_trade(k, v) for k, v in list(active_trades.items())]
    await asyncio.gather(*tasks)

# ==========================================
# ÜÇLÜ EMA + 15M SUPERTREND SİSTEMİ
# ==========================================
async def check_triple_ema():
    global active_triple_ema_trades, pending_ema_crosses
    if not is_market_open(): return
    
    sem_open = asyncio.Semaphore(10)
    async def check_open_trade(sym, trade):
        async with sem_open:
            try:
                df = await fetch_ohlc_async(sym, "15m", limit=100) 
                if df is None or len(df) < 20: return
                
                current_high = float(df['high'].iloc[-1]); current_low = float(df['low'].iloc[-1])
                
                if not trade.get("entered", False):
                    if (datetime.now(TIMEZONE) - trade["entry_time"]).total_seconds() / 60 > 150:
                        if sym in active_triple_ema_trades: del active_triple_ema_trades[sym]
                        save_trades(); return
                    
                    entry = trade["entry"]; direction = trade["type"]
                    entry_hit = False
                    if direction == "LONG" and current_low <= entry: entry_hit = True
                    elif direction == "SHORT" and current_high >= entry: entry_hit = True
                        
                    if entry_hit:
                        trade["entered"] = True
                        trade["entered_time"] = datetime.now(TIMEZONE)
                        save_trades()
                        msg = f"✅ <b>ÜÇLÜ EMA GİRİŞ TETİKLENDİ - {sym} [15M]</b>\nFiyat Süper Trend seviyesine ({entry:,.2f}) geri çekildi. Pozisyon açıldı.\n🛑 <b>Stop:</b> {trade['sl']:,.2f}\n🎯 <b>Hedef:</b> {trade['tp']:,.2f}"
                        for cid in CHAT_IDS:
                            try: await bot_app.bot.send_message(chat_id=cid, text=msg, parse_mode="HTML")
                            except: pass
                    return
                    
                entered_time = trade.get("entered_time")
                if entered_time is None: return
                if isinstance(entered_time, str): entered_time = datetime.fromisoformat(entered_time)
                if entered_time.tzinfo is None: entered_time = TIMEZONE.localize(entered_time)
                
                df_valid = df[df.index >= entered_time]
                if df_valid.empty: return
                
                max_high = float(df_valid['high'].max())
                min_low = float(df_valid['low'].min())
                
                entry = trade["entry"]; sl = trade["sl"]; tp = trade["tp"]; direction = trade["type"]
                closed = False; pnl_pct = 0; msg = ""
                
                if direction == "LONG":
                    if min_low <= sl: pnl_pct = ((sl - entry) / entry) * 100; msg = f"🛑 <b>ÜÇLÜ EMA STOP - {sym} [15M]</b>\nFiyat Stop seviyesine ({sl:,.2f}) değdi.\n<b>Net K/Z:</b> %{pnl_pct:+.2f}"; closed = True
                    elif max_high >= tp: pnl_pct = ((tp - entry) / entry) * 100; msg = f"🎯 <b>ÜÇLÜ EMA TP - {sym} [15M]</b>\nFiyat Hedef seviyesine ({tp:,.2f}) ulaştı.\n<b>Net K/Z:</b> %{pnl_pct:+.2f}"; closed = True
                else:
                    if max_high >= sl: pnl_pct = ((entry - sl) / entry) * 100; msg = f"🛑 <b>ÜÇLÜ EMA STOP - {sym} [15M]</b>\nFiyat Stop seviyesine ({sl:,.2f}) değdi.\n<b>Net K/Z:</b> %{pnl_pct:+.2f}"; closed = True
                    elif min_low <= tp: pnl_pct = ((entry - tp) / entry) * 100; msg = f"🎯 <b>ÜÇLÜ EMA TP - {sym} [15M]</b>\nFiyat Hedef seviyesine ({tp:,.2f}) ulaştı.\n<b>Net K/Z:</b> %{pnl_pct:+.2f}"; closed = True
                        
                if closed:
                    for cid in CHAT_IDS:
                        try: await bot_app.bot.send_message(chat_id=cid, text=msg, parse_mode="HTML")
                        except: pass
                    if sym in active_triple_ema_trades: del active_triple_ema_trades[sym]
                    save_trades()
            except Exception as e:
                logger.error(f"Triple EMA trade kontrol hatası ({sym}): {e}")

    if active_triple_ema_trades:
        tasks_open = [check_open_trade(sym, trade) for sym, trade in list(active_triple_ema_trades.items())]
        await asyncio.gather(*tasks_open)

    sem_pending = asyncio.Semaphore(10)
    async def check_pending(sym, data):
        async with sem_pending:
            try:
                if (datetime.now(TIMEZONE) - data["time"]).total_seconds() / 60 > 120:
                    if sym in pending_ema_crosses: del pending_ema_crosses[sym]
                    return
                    
                df_15m = await fetch_ohlc_async(sym, "15m", limit=100)
                if df_15m is None or len(df_15m) < 20: return
                
                st_series = calculate_supertrend(df_15m, period=10, multiplier=3)
                if len(st_series) < 2: return
                
                cross_time = data["time"]; df_after_cross = df_15m[df_15m.index >= cross_time]
                if df_after_cross.empty: return
                
                flipped = False; st_val = None
                for i in range(1, len(df_after_cross)):
                    curr_idx = df_15m.index.get_loc(df_after_cross.index[i]); prev_idx = curr_idx - 1
                    prev_st = st_series.iloc[prev_idx]; curr_st = st_series.iloc[curr_idx]
                    prev_close = df_15m['close'].iloc[prev_idx]; curr_close = df_15m['close'].iloc[curr_idx]
                    if pd.isna(prev_st) or pd.isna(curr_st): continue
                    prev_trend = "LONG" if prev_close > prev_st else "SHORT"; curr_trend = "LONG" if curr_close > curr_st else "SHORT"
                    if prev_trend != curr_trend and curr_trend == data["type"]:
                        flipped = True; st_val = curr_st; break
                        
                if flipped and st_val is not None:
                    direction = data["type"]; entry_price = st_val
                    if direction == "LONG": sl = entry_price * (1 - 0.015); tp = entry_price * (1 + 0.03)
                    else: sl = entry_price * (1 + 0.015); tp = entry_price * (1 - 0.03)
                        
                    emoji = "📈" if direction == "LONG" else "📉"
                    msg = f"{emoji} <b>15M SUPERTREND TEYİDİ - {sym}</b> {emoji}\n✅ 15M Süper Trend <b>{direction}</b> yönüne kırılım yaptı.\n\n💰 <b>Olası Giriş (Süper Trend Seviyesi):</b> {entry_price:,.2f}\n🛑 <b>Stop (Girişin %1.5'i):</b> {sl:,.2f}\n🎯 <b>Hedef (%3):</b> {tp:,.2f}\n\n💡 <i>Fiyat 15M Süper Trend seviyesine ({entry_price:,.2f}) geri çekildiğinde pozisyon açılacaktır.</i>"
                    
                    for cid in CHAT_IDS:
                        reply_id = data["msg_ids"].get(str(cid))
                        try: await bot_app.bot.send_message(chat_id=cid, text=msg, parse_mode="HTML", reply_to_message_id=reply_id)
                        except:
                            try: await bot_app.bot.send_message(chat_id=cid, text=msg, parse_mode="HTML")
                            except Exception as e: logger.error(f"Mesaj hatası: {e}")
                            
                    active_triple_ema_trades[sym] = {"symbol": sym, "type": direction, "entry": entry_price, "sl": sl, "tp": tp, "entered": False, "entry_time": datetime.now(TIMEZONE)}
                    save_trades()
                    if sym in pending_ema_crosses: del pending_ema_crosses[sym]
            except Exception as e:
                logger.error(f"Triple EMA pending hatası ({sym}): {e}")

    if pending_ema_crosses:
        tasks_pending = [check_pending(sym, data) for sym, data in list(pending_ema_crosses.items())]
        await asyncio.gather(*tasks_pending)

    sem_scan = asyncio.Semaphore(10)
    async def scan_single(sym):
        async with sem_scan:
            try:
                df_5m = await fetch_ohlc_async(sym, "5m", limit=500)
                if df_5m is None or len(df_5m) < 200: return
                
                ema21 = df_5m['close'].ewm(span=21, adjust=False).mean(); ema200 = df_5m['close'].ewm(span=200, adjust=False).mean()
                bar_time = df_5m.index[-1]; delay_min = (datetime.now(TIMEZONE) - bar_time).total_seconds() / 60
                if delay_min > 40: return  
                
                state_key = f"triple_ema_{sym}"
                if state_key not in triple_ema_states: triple_ema_states[state_key] = {"last_bar": None}
                    
                if triple_ema_states[state_key]["last_bar"] != bar_time:
                    curr_21 = ema21.iloc[-1]; curr_200 = ema200.iloc[-1]; prev_21 = ema21.iloc[-2]; prev_200 = ema200.iloc[-2]
                    direction = None
                    if prev_21 <= prev_200 and curr_21 > curr_200: direction = "LONG"
                    elif prev_21 >= prev_200 and curr_21 < curr_200: direction = "SHORT"
                    
                    if direction:
                        triple_ema_states[state_key]["last_bar"] = bar_time
                        existing = active_triple_ema_trades.get(sym)
                        if existing and existing["type"] == direction: return 
                        if sym in pending_ema_crosses: return
                        
                        emoji = "📈" if direction == "LONG" else "📉"
                        msg = f"🔔 <b>ÜÇLÜ EMA KESİŞİM UYARISI - {sym} [5M]</b> {emoji}\n🚨 <b>Yön:</b> {direction}\n21 EMA, 200 EMA'yı {'yukarı' if direction == 'LONG' else 'aşağı'} kesti.\n\n👀 <i>Şimdi 15M SuperTrend'in {direction} yönüne dönmesi bekleniyor... (Grafiği kontrol edin)</i>"
                        
                        msg_ids = {}
                        for cid in CHAT_IDS:
                            try:
                                sent = await bot_app.bot.send_message(chat_id=cid, text=msg, parse_mode="HTML")
                                msg_ids[str(cid)] = sent.message_id
                            except Exception as e: logger.error(f"Uyarı Mesaj hatası: {e}")
                                
                        if msg_ids:
                            pending_ema_crosses[sym] = {"type": direction, "msg_ids": msg_ids, "time": bar_time}
            except Exception as e:
                logger.error(f"Triple EMA scan hatası ({sym}): {e}")

    tasks_scan = [scan_single(sym) for sym in SYMBOLS]
    await asyncio.gather(*tasks_scan)

# ==========================================
# GAP AVCISI (16:00 - 17:40)
# ==========================================
async def check_gap_hunter():
    if not is_gap_hunter_time(): return
    
    sem = asyncio.Semaphore(10)
    async def scan_single_gap(sym):
        async with sem:
            try:
                df_15m = await fetch_ohlc_async(sym, "15m", limit=100)
                if df_15m is None or len(df_15m) < 50: return
                
                df_4h = await fetch_ohlc_async(sym, "4h", limit=100)
                if df_4h is None: return

                df_15m['ema21'] = df_15m['close'].ewm(span=21, adjust=False).mean()
                df_15m['ema50'] = df_15m['close'].ewm(span=50, adjust=False).mean()
                df_15m['rsi'] = get_rsi(df_15m['close'], 14)
                
                df_15m['typical_price'] = (df_15m['high'] + df_15m['low'] + df_15m['close']) / 3
                df_15m['vwap'] = (df_15m['typical_price'] * df_15m['volume']).groupby(df_15m.index.date).cumsum() / df_15m['volume'].groupby(df_15m.index.date).cumsum()
                
                exp1 = df_15m['close'].ewm(span=12, adjust=False).mean(); exp2 = df_15m['close'].ewm(span=26, adjust=False).mean()
                df_15m['macd'] = exp1 - exp2
                df_15m['macd_signal'] = df_15m['macd'].ewm(span=9, adjust=False).mean()
                df_15m['vol_ma'] = df_15m['volume'].rolling(20).mean()
                
                df_4h['ema200_4h'] = df_4h['close'].ewm(span=200, adjust=False).mean()
                df_4h['trend_4h'] = np.where(df_4h['close'] > df_4h['ema200_4h'], 1, -1)
                
                df_merged = pd.merge_asof(df_15m.sort_index(), df_4h[['trend_4h']].sort_index(), left_index=True, right_index=True, direction='backward')
                df_merged['trend_4h'] = df_merged['trend_4h'].fillna(0)

                if len(df_merged) < 2: return
                last_row = df_merged.iloc[-1]; prev_row = df_merged.iloc[-2]
                bar_time = df_merged.index[-1]
                
                state_key = f"gap_{sym}"
                if state_key not in gap_hunter_states: gap_hunter_states[state_key] = {"last_bar": None}
                
                if gap_hunter_states[state_key]["last_bar"] == bar_time: return

                long_cond_now = (last_row['trend_4h'] == 1) and (last_row['close'] > last_row['vwap']) and \
                                (last_row['ema21'] > last_row['ema50']) and (55 < last_row['rsi'] < 85) and \
                                (last_row['macd'] > last_row['macd_signal']) and (last_row['volume'] > last_row['vol_ma'] * 1.5)
                long_cond_prev = (prev_row['trend_4h'] == 1) and (prev_row['close'] > prev_row['vwap']) and \
                                 (prev_row['ema21'] > prev_row['ema50']) and (55 < prev_row['rsi'] < 85) and \
                                 (prev_row['macd'] > prev_row['macd_signal']) and (prev_row['volume'] > prev_row['vol_ma'] * 1.5)

                short_cond_now = (last_row['trend_4h'] == -1) and (last_row['close'] < last_row['vwap']) and \
                                 (last_row['ema21'] < last_row['ema50']) and (15 < last_row['rsi'] < 45) and \
                                 (last_row['macd'] < last_row['macd_signal']) and (last_row['volume'] > last_row['vol_ma'] * 1.5)
                short_cond_prev = (prev_row['trend_4h'] == -1) and (prev_row['close'] < prev_row['vwap']) and \
                                  (prev_row['ema21'] < prev_row['ema50']) and (15 < prev_row['rsi'] < 45) and \
                                  (prev_row['macd'] < prev_row['macd_signal']) and (prev_row['volume'] > prev_row['vol_ma'] * 1.5)

                if long_cond_now and not long_cond_prev:
                    gap_hunter_states[state_key]["last_bar"] = bar_time
                    entry_price = last_row['close']; sl = entry_price * 0.99; tp = entry_price * 1.03
                    
                    msg = f"🌙 <b>GAP AVCISI (LONG) - {sym}</b> 🌙\n"
                    msg += f"━━━━━━━━━━━━━━━━━━━━\n\n"
                    msg += f"📈 <i>16:00 Sonrası Güçlü Kapanış Tespit Edildi!</i>\n\n"
                    msg += f"💰 <b>Giriş (Mum Kapanışı):</b> {entry_price:,.2f}\n"
                    msg += f"🛑 <b>Stop (SL: %1):</b> {sl:,.2f}\n"
                    msg += f"🎯 <b>Hedef (TP: %3):</b> {tp:,.2f}\n\n"
                    msg += f"📊 <b>Hacim:</b> 🔥 Hacim Onaylı (1.5x+)\n"
                    msg += f"📉 <b>4H Trend:</b> Yukarı\n\n"
                    msg += f"💡 <b>Strateji:</b> Ertesi gün gaplı açılış bekleniyor.\n"
                    msg += f"⏳ <i>Gün içi TP vurulmazsa, ertesi gün açılış fiyatından (Gap) çıkılacaktır.</i>\n"
                    msg += f"━━━━━━━━━━━━━━━━━━━━"
                    
                    for cid in CHAT_IDS:
                        try: await bot_app.bot.send_message(chat_id=cid, text=msg, parse_mode="HTML")
                        except: pass
                        
                elif short_cond_now and not short_cond_prev:
                    gap_hunter_states[state_key]["last_bar"] = bar_time
                    entry_price = last_row['close']; sl = entry_price * 1.01; tp = entry_price * 0.97
                    
                    msg = f"🌙 <b>GAP AVCISI (SHORT) - {sym}</b> 🌙\n"
                    msg += f"━━━━━━━━━━━━━━━━━━━━\n\n"
                    msg += f"📉 <i>16:00 Sonrası Güçlü Kapanış Tespit Edildi!</i>\n\n"
                    msg += f"💰 <b>Giriş (Mum Kapanışı):</b> {entry_price:,.2f}\n"
                    msg += f"🛑 <b>Stop (SL: %1):</b> {sl:,.2f}\n"
                    msg += f"🎯 <b>Hedef (TP: %3):</b> {tp:,.2f}\n\n"
                    msg += f"📊 <b>Hacim:</b> 🔥 Hacim Onaylı (1.5x+)\n"
                    msg += f"📉 <b>4H Trend:</b> Aşağı\n\n"
                    msg += f"💡 <b>Strateji:</b> Ertesi gün gaplı açılış bekleniyor.\n"
                    msg += f"⏳ <i>Gün içi TP vurulmazsa, ertesi gün açılış fiyatından (Gap) çıkılacaktır.</i>\n"
                    msg += f"━━━━━━━━━━━━━━━━━━━━"
                    
                    for cid in CHAT_IDS:
                        try: await bot_app.bot.send_message(chat_id=cid, text=msg, parse_mode="HTML")
                        except: pass
            except Exception as e:
                logger.error(f"Gap Hunter hatası ({sym}): {e}")

    tasks = [scan_single_gap(sym) for sym in SYMBOLS]
    await asyncio.gather(*tasks)

# ==========================================
# 5M TEYİT TAKİBİ
# ==========================================
async def check_5m_confirmations():
    if not pending_confirmations: return
    now = datetime.now(TIMEZONE)
    
    for key, data in list(pending_confirmations.items()):
        sym = data["symbol"]; direction = data["direction"]; msg_id = data["msg_id"]; alert_time = data["time"]
        if (now - alert_time).total_seconds() / 60 > 120:
            del pending_confirmations[key]; continue
            
        df_5m = await fetch_ohlc_async(sym, "5m", limit=500)
        if df_5m is None or len(df_5m) < 200: continue
        
        ema21 = df_5m['close'].ewm(span=21, adjust=False).mean(); ema200 = df_5m['close'].ewm(span=200, adjust=False).mean()
        df_after_alert = df_5m[df_5m.index >= alert_time]
        if df_after_alert.empty: continue
        
        confirmed_21 = False; confirmed_200 = False; last_ema21 = 0; last_ema200 = 0
        for i in range(1, len(df_after_alert)):
            curr_idx = df_5m.index.get_loc(df_after_alert.index[i]); prev_idx = curr_idx - 1
            last_close = df_5m['close'].iloc[curr_idx]; prev_close = df_5m['close'].iloc[prev_idx]
            last_ema21 = ema21.iloc[curr_idx]; prev_ema21 = ema21.iloc[prev_idx]
            last_ema200 = ema200.iloc[curr_idx]; prev_ema200 = ema200.iloc[prev_idx]
            
            if direction == "SHORT":
                if prev_close >= prev_ema21 and last_close < last_ema21: confirmed_21 = True
                if prev_close >= prev_ema200 and last_close < last_ema200: confirmed_200 = True
            elif direction == "LONG":
                if prev_close <= prev_ema21 and last_close > last_ema21: confirmed_21 = True
                if prev_close <= prev_ema200 and last_close > last_ema200: confirmed_200 = True
            if confirmed_21 or confirmed_200: break
                
        if confirmed_21 or confirmed_200:
            ema_txt = ""
            if confirmed_21 and confirmed_200: ema_txt = "EMA 21 ve EMA 200"
            elif confirmed_21: ema_txt = "EMA 21"
            elif confirmed_200: ema_txt = "EMA 200"
            
            msg = f"✅ <b>Teyit Sinyali (5M) - {sym}</b>\n5 Dk mumu <b>{ema_txt}</b> seviyesini {'aşağı' if direction == 'SHORT' else 'yukarı'} kırdı.\n📊 <b>EMA 21:</b> {last_ema21:,.2f} | <b>EMA 200:</b> {last_ema200:,.2f}\n👀 İlk RSI uyarınız teyit edildi, işlem değerlendirebilirsiniz."
            
            for cid in CHAT_IDS:
                reply_id = msg_id.get(str(cid))
                try: await bot_app.bot.send_message(chat_id=cid, text=msg, parse_mode="HTML", reply_to_message_id=reply_id)
                except: 
                    try: await bot_app.bot.send_message(chat_id=cid, text=msg, parse_mode="HTML")
                    except Exception as e: logger.error(f"Teyit mesaj hatası: {e}")
            del pending_confirmations[key]

# ==========================================
# BIST 30 ÖZETİ
# ==========================================
async def generate_market_summary(session_name):
    try:
        df_30m, df_1h = await asyncio.gather(fetch_ohlc_async("XU030", "30m", limit=500), fetch_ohlc_async("XU030", "1h", limit=500))
        if df_30m is None or df_1h is None: 
            logger.warning("XU030 özeti için veri çekilemedi. (API Hatası)")
            return
        
        today = datetime.now(TIMEZONE).date(); df_today = df_30m[df_30m.index.date == today]
        if df_today.empty: 
            logger.warning("XU030 özeti için bugünün verisi bulunamadı.")
            return
        
        if "Sabah" in session_name: session_df = df_today.between_time('09:55', '14:00'); time_str = "09:55 - 14:00"
        else: session_df = df_today.between_time('14:01', '18:05'); time_str = "14:01 - 18:05"
            
        if session_df.empty: 
            logger.warning(f"{session_name} özeti için seans verisi boş!")
            return
            
        open_price = session_df['open'].iloc[0]; close_price = session_df['close'].iloc[-1]
        high_price = session_df['high'].max(); low_price = session_df['low'].min()
        pct_change = ((close_price - open_price) / open_price) * 100
        dist_to_high = ((high_price - close_price) / high_price) * 100; dist_to_low = ((close_price - low_price) / low_price) * 100
        if dist_to_high < 0.2: close_status = "🟢 Zirvede Kapattı (Güçlü)"
        elif dist_to_low < 0.2: close_status = "🔴 Dipte Kapattı (Zayıf)"
        else: close_status = "⚪ Orta Bölgede Kapattı"
        
        session_vol = session_df['volume'].sum(); past_vols = []
        for i in range(1, 4):
            past_date = today - timedelta(days=i); past_df = df_30m[df_30m.index.date == past_date]
            if not past_df.empty:
                if "Sabah" in session_name: p_vol = past_df.between_time('09:55', '14:00')['volume'].sum()
                else: p_vol = past_df.between_time('14:01', '17:54')['volume'].sum()
                past_vols.append(p_vol)
                
        avg_3d_vol = sum(past_vols) / len(past_vols) if past_vols else session_vol
        vol_change = ((session_vol - avg_3d_vol) / avg_3d_vol) * 100 if avg_3d_vol > 0 else 0
        
        if vol_change > 10: vol_status = f"🟢 Son 3 günün %{vol_change:.1f} ÜZERİNDE (Hacimli)"
        elif vol_change < -10: vol_status = f"🔴 Son 3 günün %{abs(vol_change):.1f} ALTINDA (Sessiz)"
        else: vol_status = "⚪ Normal Seviyede"
        
        stock_changes = {}; green_count = 0; red_count = 0
        def _inner_change(s):
            try:
                h = bp.Ticker(s).history(period="5d", interval="1d")
                if h is None or h.empty: return s, None
                h.columns = [str(c).lower() for c in h.columns] 
                if len(h) >= 2: return s, ((h['close'].iloc[-1] - h['close'].iloc[-2]) / h['close'].iloc[-2]) * 100
            except Exception as e: pass
            return s, None

        sem_summary = asyncio.Semaphore(8)
        async def limited_change(s):
            async with sem_summary: return await asyncio.to_thread(_inner_change, s)
                
        results = await asyncio.gather(*[limited_change(s) for s in BIST_STOCKS])
        for sym, change in results:
            if change is None: continue
            stock_changes[sym] = change
            if change > 0: green_count += 1
            else: red_count += 1
            
        sector_lines = []
        for sector_name, stocks in SECTORS.items():
            sector_stocks = [s for s in stocks if s in stock_changes]
            if not sector_stocks: continue
            greens = sum(1 for s in sector_stocks if stock_changes[s] > 0); reds = len(sector_stocks) - greens
            avg_chg = sum(stock_changes[s] for s in sector_stocks) / len(sector_stocks)
            emoji = "🟢" if greens > reds else ("🔴" if reds > greens else "⚪")
            sector_lines.append((avg_chg, f"{emoji} {sector_name}: {greens}Y/{reds}K (Ort: %{avg_chg:+.2f})"))
        sector_lines.sort(key=lambda x: x[0], reverse=True)
        sector_text = "\n".join([line for _, line in sector_lines])
        
        rsi_1h = get_rsi(df_1h['close'], RSI_PERIOD).iloc[-1]
        ema50_30m = df_30m['close'].ewm(span=50, adjust=False).mean().iloc[-1]
        trend_status = "🟢 EMA 50 Üzerinde" if close_price > ema50_30m else "🔴 EMA 50 Altında"
        best_sector = sector_lines[0][1].split(":")[0] if sector_lines else ""
        worst_sector = sector_lines[-1][1].split(":")[0] if sector_lines else ""
        
        if pct_change > 0 and vol_change > 0 and green_count > red_count: yorum = f"Alıcılar baskın. Özellikle {best_sector} sektörü öne çıkıyor. Long işlemler ön planda olabilir."
        elif pct_change < 0 and vol_change > 0 and red_count > green_count: yorum = f"Satıcılar baskın. {best_sector} sektörü direniyor ama genel görünüm zayıf. Short işlemler düşünülebilir."
        elif green_count > red_count and pct_change < 0: yorum = f"Endeks düşse de hisselerin çoğu yeşil. {best_sector} öne çıkıyor. Dönüş sinyali olabilir."
        elif red_count > green_count and pct_change > 0: yorum = f"Endeks yükselse de hisselerin çoğu kırmızı. {worst_sector} zayıf. Yükseliş inandırıcı değil."
        else: yorum = "Piyasa kararsız. Yön netleşene kadar beklemede kalmak mantıklı."
        
        msg = f"🏛️ <b>BIST 30 (XU030) — {session_name} Özeti</b>\n⏱ ({time_str} Arası)\n\n📈 <b>Fiyat:</b> {close_price:,.2f} (%{pct_change:+.2f})\n📍 <b>Kapanış:</b> {close_status}\n\n📊 <b>Hacim:</b> {vol_status}\n\n🌡️ <b>Piyasa Genişliği:</b> {green_count} Yeşil / {red_count} Kırmızı\n\n🧭 <b>Trend:</b> 1H RSI: {rsi_1h:.1f} | 30m EMA 50: {ema50_30m:,.2f} ({trend_status})\n\n🏭 <b>Sektör Performansı:</b>\n{sector_text}\n\n💡 <b>Yorum:</b> {yorum}"
        for cid in CHAT_IDS:
            try: await bot_app.bot.send_message(chat_id=cid, text=msg, parse_mode="HTML")
            except Exception as e: 
                logger.error(f"Özet mesaj hatası: {e}")
    except Exception as e: 
        logger.error(f"Özet fonksiyonu hatası: {e}")

async def analyze_symbol(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id not in CHAT_IDS: return
    text = update.message.text.upper().replace("/", "").strip()
    if not text.isalnum() or len(text) < 2 or len(text) > 10: return
    if text in ["START", "HELP", "YARDIM", "TARA"]: return
    if text not in SYMBOLS:
        await update.message.reply_text(f"❌ '{text}' tarama listesinde bulunamadı."); return
    await update.message.reply_text(f"🔍 {text} analiz ediliyor, lütfen bekleyin...")
    try:
        df_30m, df_1h, df_4h, df_1d = await asyncio.gather(fetch_ohlc_async(text, "30m", limit=500), fetch_ohlc_async(text, "1h", limit=500), fetch_ohlc_async(text, "4h", limit=500), fetch_ohlc_async(text, "1d", limit=500))
        if df_1h is None or df_4h is None:
            await update.message.reply_text("❌ Veri çekilemedi."); return
        msg = f"🤖 <b>{text} — Kapsamlı Analiz</b>\n"
        price = df_1h['close'].iloc[-1]; rsi_1h = get_rsi(df_1h['close'], RSI_PERIOD).iloc[-1]
        trend_1h = "🟢 Yukarı" if price > df_1h['close'].ewm(span=200, adjust=False).mean().iloc[-1] else "🔴 Aşağı"
        msg += f"\n📊 <b>1 Saatlik (1H)</b>\n• Fiyat: {price:,.2f}\n• RSI(8): {rsi_1h:.1f}\n• Trend: {trend_1h} (EMA 200)\n"
        rsi_4h = get_rsi(df_4h['close'], RSI_PERIOD).iloc[-1]
        trend_4h = "🟢 Yukarı" if price > df_4h['close'].ewm(span=200, adjust=False).mean().iloc[-1] else "🔴 Aşağı"
        msg += f"\n📊 <b>4 Saatlik (4H)</b>\n• RSI(8): {rsi_4h:.1f}\n• Trend: {trend_4h} (EMA 200)\n"
        if df_1d is not None: msg += f"\n📊 <b>Günlük (1D)</b>\n• RSI(14): {get_rsi(df_1d['close'], 14).iloc[-1]:.1f}\n"
        if df_30m is not None:
            ema50_30m = df_30m['close'].ewm(span=50, adjust=False).mean().iloc[-1]
            msg += f"\n⚓ <b>30 Dakikalık (30M)</b>\n• EMA 50: {ema50_30m:,.2f}\n"
        else: ema50_30m = 0; msg += f"\n⚓ <b>30 Dakikalık (30M)</b>\n• EMA 50: Veri Yok\n"
        
        fibo_note = await get_nearest_fibo_levels_async(text, price); msg += fibo_note
        msg += f"\n💡 <b>Yorum ve Strateji:</b>\n"
        if trend_4h == "🟢 Yukarı":
            msg += f"Ana trend YUKARI. Alım yönlü işlemler düşünülebilir.\n"
            if df_30m is not None: msg += f"🛒 <b>Olası Alım Yeri:</b> 30m EMA 50 seviyesi olan <b>{ema50_30m:,.2f}</b>."
        else:
            msg += f"Ana trend AŞAĞI. Satış yönlü işlemler düşünülebilir.\n"
            if df_30m is not None: msg += f"🛒 <b>Olası Satım Yeri:</b> 30m EMA 50 seviyesi olan <b>{ema50_30m:,.2f}</b>."
        await update.message.reply_text(msg, parse_mode="HTML")
    except Exception as e:
        logger.error(f"Analiz hatası: {e}"); await update.message.reply_text(f"❌ Analiz hatası: {e}")

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id not in CHAT_IDS: return
    await update.message.reply_text("✅ Sistem çalışıyor. Analiz için /ASELS gibi komut yazabilirsiniz. Manuel tarama için /tara yazın.")

async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id not in CHAT_IDS: return
    msg = "🤖 <b>Yardım Menüsü</b>\n\n• Hisse analizi için: <code>/ASELS</code> veya <code>ASELS</code>\n• Endeks analizi için: <code>/XU030</code>\n• Tüm piyasayı anında taramak için: <code>/tara</code>\n\nSistem, RSI Büyüsü, ATR Dünyası, Üçlü EMA, Gap Avcısı (16:00-17:40) ve <b>Mükemmel Fibo (1H & 4H)</b> sinyallerini otomatik olarak takip eder."
    await update.message.reply_text(msg, parse_mode="HTML")

async def cmd_tara(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    global is_scanning
    if update.effective_chat.id not in CHAT_IDS: return
    if is_scanning: await update.message.reply_text("⏳ Şu an arka planda bir tarama devam ediyor. Lütfen bitmesini bekleyin..."); return
    await update.message.reply_text("🔄 Manuel tarama arka planda başlatıldı. Tüm hisseler kontrol ediliyor...")
    async def run_manual_scan():
        await tick(force=True)
        for cid in CHAT_IDS:
            try: await bot_app.bot.send_message(chat_id=cid, text="✅ Tarama tamamlandı. Eğer sinyal koşulu oluştuysa mesajınız gelmiştir.", parse_mode="HTML")
            except: pass
    task = asyncio.create_task(run_manual_scan()); background_tasks.add(task); task.add_done_callback(background_tasks.discard)

# ==========================================
# BOT BAŞLATMA VE HANDLER'LAR
# ==========================================
bot_app = Application.builder().token(BOT_TOKEN).build()

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error(f"🚨 Telegram Hatası: {context.error}")
    if update and update.effective_chat:
        try: await context.bot.send_message(chat_id=update.effective_chat.id, text="⚠️ Beklenmeyen bir hata oluştu, lütfen tekrar deneyin.")
        except: pass

bot_app.add_error_handler(error_handler)

async def send_signal_message(msg_data):
    text = msg_data["text"]; symbol = msg_data["symbol"]; tf = msg_data.get("tf"); msg_type = msg_data["type"]
    trade_key = None
    if msg_type == "atr_dunyasi" and "direction" in msg_data:
        trade_key = f"{symbol}_{msg_data['direction']}_atr_{tf}"
        async with trade_lock:
            if trade_key in active_trades or trade_key in pending_trades: return
            pending_trades.add(trade_key)
    try:
        msg_ids = {}
        for cid in CHAT_IDS:
            try:
                sent = await bot_app.bot.send_message(chat_id=cid, text=text, parse_mode="HTML")
                if "direction" in msg_data: msg_ids[str(cid)] = sent.message_id
            except Exception as e: logger.error(f"Telegram gönderim hatası: {e}")
        if msg_type == "atr_dunyasi" and "direction" in msg_data:
            async with trade_lock:
                if not msg_ids: return
                trade_info = {"symbol": symbol, "type": msg_data["direction"], "entry": msg_data["price"], "msg_ids": msg_ids, "signal_type": msg_type, "sl": msg_data.get("sl"), "tp": msg_data.get("tp"), "tf": tf, "entry_time": msg_data["bar_time"], "last_checked_bar": msg_data["bar_time"]}
                active_trades[trade_key] = trade_info; save_trades()
        if msg_type == "alert_instant":
            key = f"{symbol}_{msg_data['direction']}"
            pending_confirmations[key] = {"symbol": symbol, "direction": msg_data['direction'], "msg_id": msg_ids, "time": datetime.now(TIMEZONE)}
    finally:
        if trade_key:
            async with trade_lock: pending_trades.discard(trade_key)

auth_filter = filters.User(user_id=CHAT_IDS)
bot_app.add_handler(CommandHandler("start", cmd_start, filters=auth_filter))
bot_app.add_handler(CommandHandler(["help", "yardim", "durum"], cmd_help, filters=auth_filter))
bot_app.add_handler(CommandHandler("tara", cmd_tara, filters=auth_filter))
bot_app.add_handler(MessageHandler(filters.TEXT & auth_filter, analyze_symbol))

async def tick(force=False):
    global is_scanning
    if not force and not is_market_open(): return
    if is_scanning: return
    is_scanning = True
    logger.info(f"🔄 Hızlı Tarama başladı. (Zaman: {datetime.now(TIMEZONE).strftime('%H:%M')})")
    failed_count = 0
    try:
        sem = asyncio.Semaphore(10)
        async def limited_fetch(sym):
            async with sem:
                try:
                    dfs = await fetch_all_tfs_async(sym)
                    return sym, dfs
                except Exception as e:
                    logger.error(f"Fetch hatası ({sym}): {e}")
                    return sym, None

        tasks = [limited_fetch(sym) for sym in SYMBOLS]

        for coro in asyncio.as_completed(tasks):
            try:
                sym, dfs = await coro
                if dfs is None or not dfs:
                    failed_count += 1
                    continue
                try:
                    rsi_msgs = await process_rsi_signals(sym, dfs)
                    for msg_data in rsi_msgs: await send_signal_message(msg_data)
                    atr_msgs = await process_atr_signals(sym, dfs)
                    for msg_data in atr_msgs: await send_signal_message(msg_data)
                except Exception as e:
                    logger.error(f"Sinyal işleme hatası ({sym}): {e}")
            except Exception as e:
                failed_count += 1
                logger.error(f"Beklenmeyen tarama hatası: {e}")

        if failed_count >= 15:
            await send_admin_alert(
                f"⚠️ <b>Tarama Sorunu</b>\n{failed_count}/{len(SYMBOLS)} hisse için veri alınamadı. Veri sağlayıcı yavaş olabilir. Tarama kısmi çalıştı.",
                "scan_partial_fail"
            )
    finally:
        is_scanning = False

async def tracker_tick():
    if not is_market_open(): return
    try: await check_active_trades()
    except Exception as e: logger.error(f"check_active_trades hatası: {e}")
    try: await check_5m_confirmations()
    except Exception as e: logger.error(f"check_5m_confirmations hatası: {e}")
    try: await check_triple_ema()
    except Exception as e: logger.error(f"check_triple_ema hatası: {e}")
    try: await check_gap_hunter() 
    except Exception as e: logger.error(f"check_gap_hunter hatası: {e}")
    try: await check_mukemmel_fibo()
    except Exception as e: logger.error(f"check_mukemmel_fibo hatası: {e}")

async def market_open_scan():
    if not is_market_open(): return
    for cid in CHAT_IDS:
        try: 
            await bot_app.bot.send_message(chat_id=cid, text="🟢 Seans açıldı, tarama ve takip başladı.", parse_mode="HTML")
            await bot_app.bot.send_message(chat_id=cid, text="Sistem her zaman senin için çalışıyor, kazanmak istiyorsan sadece sistemle kal... MZB 😉", parse_mode="HTML")
        except Exception as e: pass
    await tick(force=True)

async def clear_daily_state():
    async with trade_lock:
        instant_states.clear(); atr_states.clear(); earnings_cache.clear(); fibo_cache.clear()
        pending_confirmations.clear(); pending_trades.clear(); CACHE_5M.clear(); CACHE_15M.clear(); pending_ema_crosses.clear()
        gap_hunter_states.clear(); MUK_FIBO_TRADES.clear(); MUK_FIBO_SETUPS.clear()

async def task_morning_summary(): await generate_market_summary("Sabah")
async def task_afternoon_summary(): await generate_market_summary("Öğleden Sonra")

class DummyHandler(BaseHTTPRequestHandler):
    def do_GET(self): self.send_response(200); self.end_headers(); self.wfile.write(b"Alive")
    def do_HEAD(self): self.send_response(200); self.end_headers()
    def log_message(self, format, *args): return

def run_dummy_server(): HTTPServer(("0.0.0.0", int(os.environ.get("PORT", 8080))), DummyHandler).serve_forever()

async def main():
    load_trades() 
    threading.Thread(target=run_dummy_server, daemon=True).start()
    await bot_app.initialize(); await bot_app.start(); await bot_app.updater.start_polling(drop_pending_updates=True)
    
    sched = AsyncIOScheduler(timezone=TIMEZONE, job_defaults={'coalesce': True, 'max_instances': 1, 'misfire_grace_time': 60})
    
    sched.add_job(market_open_scan, CronTrigger(day_of_week="mon-fri", hour=9, minute=55, timezone=TIMEZONE))
    sched.add_job(tick, CronTrigger(day_of_week="mon-fri", hour="10-17", minute="*/5", timezone=TIMEZONE))
    sched.add_job(tracker_tick, CronTrigger(day_of_week="mon-fri", hour="10-17", minute="*/5", timezone=TIMEZONE))
    sched.add_job(tracker_tick, CronTrigger(day_of_week="mon-fri", hour=17, minute=58, timezone=TIMEZONE))
    sched.add_job(tick, CronTrigger(day_of_week="mon-fri", hour=18, minute=10, timezone=TIMEZONE), kwargs={'force': True})
    sched.add_job(clear_daily_state, CronTrigger(day_of_week="mon-fri", hour=18, minute=30, timezone=TIMEZONE))
    sched.add_job(task_morning_summary, CronTrigger(day_of_week="mon-fri", hour=14, minute=15, timezone=TIMEZONE))
    sched.add_job(task_afternoon_summary, CronTrigger(day_of_week="mon-fri", hour=18, minute=15, timezone=TIMEZONE))
    
    sched.start()
    logger.info("✅ Bot çalışıyor. Mükemmel Fibo Modülü Eklendi. Loglar açık.")
    while True: await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())