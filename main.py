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

RSI_PERIOD = 8
INSTANT_SHORT = 87.5     
INSTANT_LONG = 13        
ALARM_SHORT_UPPER = 85   
ALARM_LONG_LOWER = 15    
ALARM_ATR_DROP = 2.0

TIME_FRAMES_RSI = ["1h"]            
TIME_FRAMES_ATR = ["15m", "30m", "1h", "4h"] 
FETCH_TFS = ["15m", "30m", "1h", "4h"]
TF_MINUTES = {"15m": 15, "30m": 30, "1h": 60, "4h": 240}

VOLUME_LOOKBACK = 20
VOLUME_MULT = 1.5
TP_PCT = 3.0
SL_PCT = 3.5 
TIMEOUT_HOURS = 10 

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
active_gap_hunter_trades = {} # YENİ
gap_hunter_states = {} # YENİ
is_scanning = False
background_tasks = set()

CACHE_5M = {}
CACHE_15M = {}
CACHE_TTL = 60 

trade_lock = asyncio.Lock()

DATA_DIR = os.getenv("DATA_DIR", "/tmp")
TRADES_FILE = os.path.join(DATA_DIR, "active_trades.json")
TRIPLE_EMA_FILE = os.path.join(DATA_DIR, "active_triple_ema.json")
GAP_HUNTER_FILE = os.path.join(DATA_DIR, "active_gap_hunter.json") # YENİ

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
            if isinstance(trade_copy["entry_time"], datetime):
                trade_copy["entry_time"] = trade_copy["entry_time"].isoformat()
            serializable_trades[k] = trade_copy
        with open(TRADES_FILE + ".tmp", "w", encoding="utf-8") as f:
            json.dump(serializable_trades, f, ensure_ascii=False, indent=2)
        os.replace(TRADES_FILE + ".tmp", TRADES_FILE)
            
        serializable_ema = {}
        for k, v in active_triple_ema_trades.items():
            trade_copy = v.copy()
            if isinstance(trade_copy["entry_time"], datetime):
                trade_copy["entry_time"] = trade_copy["entry_time"].isoformat()
            serializable_ema[k] = trade_copy
        with open(TRIPLE_EMA_FILE + ".tmp", "w", encoding="utf-8") as f:
            json.dump(serializable_ema, f, ensure_ascii=False, indent=2)
        os.replace(TRIPLE_EMA_FILE + ".tmp", TRIPLE_EMA_FILE)

        serializable_gap = {}
        for k, v in active_gap_hunter_trades.items():
            trade_copy = v.copy()
            if isinstance(trade_copy["entry_time"], datetime):
                trade_copy["entry_time"] = trade_copy["entry_time"].isoformat()
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
                    if "entry_time" in v and isinstance(v["entry_time"], str):
                        v["entry_time"] = datetime.fromisoformat(v["entry_time"])
                    active_trades[k] = v
            logger.info(f"✅ {len(active_trades)} geçerli aktif ATR işlemi yüklendi.")
        except Exception as e: logger.error(f"Trade yükleme hatası: {e}")
            
    if os.path.exists(TRIPLE_EMA_FILE):
        try:
            with open(TRIPLE_EMA_FILE, "r", encoding="utf-8") as f:
                loaded = json.load(f)
                for k, v in loaded.items():
                    if "entry_time" in v and isinstance(v["entry_time"], str):
                        v["entry_time"] = datetime.fromisoformat(v["entry_time"])
                    active_triple_ema_trades[k] = v
            logger.info(f"✅ {len(active_triple_ema_trades)} aktif Üçlü EMA işlemi yüklendi.")
        except Exception as e: logger.error(f"Üçlü EMA yükleme hatası: {e}")

    if os.path.exists(GAP_HUNTER_FILE):
        try:
            with open(GAP_HUNTER_FILE, "r", encoding="utf-8") as f:
                loaded = json.load(f)
                for k, v in loaded.items():
                    if "entry_time" in v and isinstance(v["entry_time"], str):
                        v["entry_time"] = datetime.fromisoformat(v["entry_time"])
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
    except Exception as e: pass
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
    except Exception as e: return ""

async def get_nearest_fibo_levels_async(symbol, current_price):
    today = datetime.now(TIMEZONE).date()
    cache_key = f"{symbol}_{today}_{int(current_price)}"
    if cache_key in fibo_cache: return fibo_cache[cache_key]
    try:
        note = await asyncio.wait_for(asyncio.to_thread(_get_fibo_sync, symbol, current_price), timeout=20.0)
        if note: fibo_cache[cache_key] = note
        return note
    except asyncio.TimeoutError: return ""

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
        except Exception as e: logger.error(f"Veri çekme hatası ({symbol} {interval}): {e}"); time_module.sleep(1)
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
# SİNYAL MOTORLARI (RSI VE ATR)
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

async def process_signals(symbol, dfs):
    msgs = []
    df_4h = dfs.get("4h")
    for tf in TIME_FRAMES_RSI:
        df = dfs.get(tf)
        if df is None or len(df) < 30: continue
        rsi = get_rsi(df["close"], period=RSI_PERIOD)
        if rsi is None or len(rsi) < 2: continue
        r_now, r_prev = float(rsi.iloc[-1]), float(rsi.iloc[-2])
        bar_time = df.index[-1]; inst_key = f"inst_{symbol}_{tf}"
        if inst_key not in instant_states: instant_states[inst_key] = {"last_bar": None}
        if instant_states[inst_key]["last_bar"] != bar_time:
            delay_min = (datetime.now(TIMEZONE) - bar_time).total_seconds() / 60
            if delay_min > 180: continue
            direction = None
            if r_prev <= INSTANT_SHORT < r_now: direction = "SHORT"
            elif r_prev >= INSTANT_LONG > r_now: direction = "LONG"
            if direction:
                instant_states[inst_key]["last_bar"] = bar_time
                tags = get_volume_and_trend_tag(df, df_4h); earnings_note = await get_earnings_note_async(symbol)
                current_price = float(df['close'].iloc[-1]); fibo_note = await get_nearest_fibo_levels_async(symbol, current_price)
                if direction == "SHORT":
                    msg = f"✨ <b>RSI BÜYÜSÜ (1H)</b> ✨\n⚠️ <b>{symbol}</b> — İLK RSI UYARISI\nRSI 8: {r_prev:.1f} → {r_now:.1f} (Aşırı Alım)\n📊 {tags}\n\n🚨 <b>Alarm Fiyatı:</b> {current_price:,.2f}\n🎯 <b>Olası Giriş:</b> {current_price * 1.02:,.2f} (Tahmini +%2)"
                else:
                    msg = f"✨ <b>RSI BÜYÜSÜ (1H)</b> ✨\n⚠️ <b>{symbol}</b> — İLK RSI UYARISI\nRSI 8: {r_prev:.1f} → {r_now:.1f} (Aşırı Satım)\n📊 {tags}\n\n🚨 <b>Alarm Fiyatı:</b> {current_price:,.2f}\n🎯 <b>Olası Giriş:</b> {current_price * 0.98:,.2f} (Tahmini -%2)"
                msg += fibo_note + "\n👀 <b>Grafiği kontrol edin. Teyit sinyali bekleniyor...</b>" + earnings_note
                msgs.append({"text": msg, "symbol": symbol, "tf": tf, "type": "alert_instant", "direction": direction})

    for tf in TIME_FRAMES_ATR:
        df = dfs.get(tf)
        if df is None or len(df) < 30: continue
        rsi = get_rsi(df["close"], period=RSI_PERIOD)
        if rsi is None or len(rsi) < 2: continue
        atr_series = get_atr_series(df, period=10)
        bar_time = df.index[-1]; atr_key = f"atr_{symbol}_{tf}"
        if atr_key not in atr_states: atr_states[atr_key] = {"state": "IDLE", "peak_rsi": 0, "peak_price": 0, "low_rsi": 100, "low_price": 0, "last_bar": None, "reject_count": 0}
        atr_data = atr_states[atr_key]
        if atr_data.get("last_bar") != bar_time:
            new_bars = df[df.index > atr_data["last_bar"]] if atr_data.get("last_bar") else df.tail(1)
            for idx, row in new_bars.iterrows():
                delay_min = (datetime.now(TIMEZONE) - idx).total_seconds() / 60
                max_delay = 60 if tf == "15m" else (90 if tf == "30m" else (180 if tf == "1h" else 300))
                if delay_min > max_delay: continue
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
                        tags = get_volume_and_trend_tag(df, df_4h); earnings_note = await get_earnings_note_async(symbol); fibo_note = await get_nearest_fibo_levels_async(symbol, close_price)
                        entry_time = idx + timedelta(minutes=TF_MINUTES[tf])
                        sl = close_price * (1 + SL_PCT / 100); tp = close_price * (1 - TP_PCT / 100)
                        msg = f"✨ <b>ATR DÜNYASI (Short) - {symbol} [{tf.upper()}]</b> ✨\n🔴 RSI Aşırı Alım Zirvesi: {atr_data['peak_rsi']:.1f}\n📊 {tags}\n\n💰 <b>Olası Giriş:</b> {close_price:,.2f}\n🛑 <b>Stop:</b> {sl:,.2f} (+%{SL_PCT})\n🎯 <b>TP (%{TP_PCT}):</b> {tp:,.2f}\n⏳ <i>Zaman Aşımı: {TIMEOUT_HOURS} Seans Saati</i>"
                        msg += fibo_note; msg += earnings_note
                        msgs.append({"text": msg, "symbol": symbol, "tf": tf, "type": "atr_dunyasi", "direction": "SHORT", "price": close_price, "sl": sl, "tp": tp, "bar_time": entry_time})
                        atr_data["state"] = "IDLE"
                    else:
                        atr_data["reject_count"] += 1
                        if atr_data["reject_count"] >= 10 or r_now < 60: atr_data.update({"state": "IDLE", "reject_count": 0})
                elif state == "LONG_ARMED":
                    if r_now < atr_data["low_rsi"]: atr_data["low_rsi"] = r_now; atr_data["low_price"] = candle_low
                    price_ok = (close_price - atr_data["low_price"]) >= atr_val * ALARM_ATR_DROP; candle_ok = close_price > row['open']
                    if price_ok and candle_ok:
                        tags = get_volume_and_trend_tag(df, df_4h); earnings_note = await get_earnings_note_async(symbol); fibo_note = await get_nearest_fibo_levels_async(symbol, close_price)
                        entry_time = idx + timedelta(minutes=TF_MINUTES[tf])
                        sl = close_price * (1 - SL