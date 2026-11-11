import json, os
import asyncio
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from datetime import datetime, timedelta, time as dt_time
import time as time_module
from concurrent.futures import ThreadPoolExecutor
import numpy as np

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

if not BOT_TOKEN:
    raise ValueError("❌ HATA: BOT_TOKEN bulunamadı!")

if TV_SESSION and TV_SESSION_SIGN:
    try:
        bp.set_tradingview_auth(session=TV_SESSION, session_sign=TV_SESSION_SIGN)
        print("✅ Borsapy: TradingView canlı veri yetkisi alındı!")
    except Exception as e:
        print(f"⚠️ TradingView Auth Hatası: {e}")
else:
    print("ℹ️ TradingView çerezleri girilmedi. Gecikmeli veri.")

TIMEZONE = pytz.timezone("Europe/Istanbul")
TR_HOLIDAYS = holidays.Turkey(years=range(2024, 2031))

SYMBOLS = [
    "TOASO", "TRALT", "ASTOR", "GUBRF", "TRMET", "TCELL", "GARAN", "YKBNK", "TAVHL", "ENKAI",
    "TTKOM", "SAHOL", "SASA", "AEFES", "ASELS", "MGROS", "TUPRS", "AKBNK", "KCHOL", "KRDMD",
    "ISCTR", "THYAO", "BIMAS", "EREGL", "EKGYO", "PGSUS", "FROTO", "SISE", "VAKBN", "ALARK",
    "AKSEN", "ARCLK", "BRSAN", "CIMSA", "DOAS", "DOHOL", "ENJSA", "OYAKC", "PETKM", "SOKM",
    "TKFEN", "ULKER", "VESTL", "XU030"
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
is_scanning = False
background_tasks = set()

CACHE_5M = {}
CACHE_TTL = 60 

trade_lock = asyncio.Lock()

DATA_DIR = os.getenv("DATA_DIR", "/tmp")
TRADES_FILE = os.path.join(DATA_DIR, "active_trades.json")
TRIPLE_EMA_FILE = os.path.join(DATA_DIR, "active_triple_ema.json")

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
            if isinstance(trade_copy.get("last_checked_bar"), datetime):
                trade_copy["last_checked_bar"] = trade_copy["last_checked_bar"].isoformat()
            serializable_trades[k] = trade_copy
        temp_file = TRADES_FILE + ".tmp"
        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump(serializable_trades, f, ensure_ascii=False, indent=2)
        os.replace(temp_file, TRADES_FILE)
            
        serializable_ema = {}
        for k, v in active_triple_ema_trades.items():
            trade_copy = v.copy()
            if isinstance(trade_copy["entry_time"], datetime):
                trade_copy["entry_time"] = trade_copy["entry_time"].isoformat()
            serializable_ema[k] = trade_copy
        temp_file_ema = TRIPLE_EMA_FILE + ".tmp"
        with open(temp_file_ema, "w", encoding="utf-8") as f:
            json.dump(serializable_ema, f, ensure_ascii=False, indent=2)
        os.replace(temp_file_ema, TRIPLE_EMA_FILE)
    except Exception as e:
        print(f"⚠️ Trade kayıt hatası: {e}")

def load_trades():
    global active_trades, active_triple_ema_trades
    if os.path.exists(TRADES_FILE):
        try:
            with open(TRADES_FILE, "r", encoding="utf-8") as f:
                loaded = json.load(f)
                for k, v in loaded.items():
                    if "tp" not in v or "sl" not in v:
                        continue
                    if "entry_time" in v and isinstance(v["entry_time"], str):
                        v["entry_time"] = datetime.fromisoformat(v["entry_time"])
                    if "last_checked_bar" in v and isinstance(v["last_checked_bar"], str):
                        v["last_checked_bar"] = datetime.fromisoformat(v["last_checked_bar"])
                    active_trades[k] = v
            print(f"✅ {len(active_trades)} geçerli aktif ATR işlemi yüklendi.")
        except Exception as e:
            print(f"⚠️ Trade yükleme hatası: {e}")
            
    if os.path.exists(TRIPLE_EMA_FILE):
        try:
            with open(TRIPLE_EMA_FILE, "r", encoding="utf-8") as f:
                loaded = json.load(f)
                for k, v in loaded.items():
                    if "entry_time" in v and isinstance(v["entry_time"], str):
                        v["entry_time"] = datetime.fromisoformat(v["entry_time"])
                    active_triple_ema_trades[k] = v
            print(f"✅ {len(active_triple_ema_trades)} aktif Üçlü EMA işlemi yüklendi.")
        except Exception as e:
            print(f"⚠️ Üçlü EMA yükleme hatası: {e}")

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
    return dt_time(9, 55) <= now.time() <= dt_time(18, 0)

def get_effective_market_hours(entry_time, now_time):
    if now_time <= entry_time: return 0.0
    total_hours = 0.0
    current = entry_time
    market_open = dt_time(10, 0)
    market_close = dt_time(18, 0)
    
    while current.date() <= now_time.date():
        if current.weekday() > 4 or current.date() in TR_HOLIDAYS:
            current = TIMEZONE.localize(datetime.combine(current.date() + timedelta(days=1), market_open))
            continue
        
        day_close = dt_time(13, 0) if is_arife(current) else market_close
        day_open = TIMEZONE.localize(datetime.combine(current.date(), market_open))
        day_close_tz = TIMEZONE.localize(datetime.combine(current.date(), day_close))
        
        if now_time < day_open: break
        start_of_period = max(current, day_open)
        end_of_period = min(now_time, day_close_tz)
        if end_of_period > start_of_period:
            total_hours += (end_of_period - start_of_period).total_seconds() / 3600
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
        if 0 <= days_left <= 5:
            return f"\n\n📅 <b>Bilanço Uyarısı:</b> Bilançoya {days_left} gün kaldı! (Dikkatli olun)"
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
        
        pivot = (H + L + C) / 3
        fark = H - L
        
        raw_levels = []
        ratios = [0.236, 0.382, 0.5, 0.618, 1.0, 1.272, 1.618, 2.618]
        for r in ratios:
            raw_levels.append((pivot - fark * r, str(r)))
            raw_levels.append((pivot + fark * r, str(r)))
            
        yakin_seviyeler = [l for l in raw_levels if abs(l[0] - current_price) / current_price < 0.05]
        if not yakin_seviyeler: return ""
            
        en_yakin_seviye = min(yakin_seviyeler, key=lambda x: abs(x[0] - current_price))
        note = "\n🎯 <b>FİBO SEVİYELERİ (Günlük):</b>"
        
        if en_yakin_seviye[1] in ["0.5", "0.618"]:
            lvl_05 = min((l for l in yakin_seviyeler if l[1] == "0.5"), key=lambda x: abs(x[0] - current_price), default=None)
            lvl_0618 = min((l for l in yakin_seviyeler if l[1] == "0.618"), key=lambda x: abs(x[0] - current_price), default=None)
            if lvl_05 and lvl_0618:
                note += f"\n🎯 <b>0.5 Seviyesi:</b> {lvl_05[0]:,.2f}"
                note += f"\n🎯 <b>0.618 Seviyesi:</b> {lvl_0618[0]:,.2f}"
            else:
                note += f"\n🎯 <b>En Yakın Seviye ({en_yakin_seviye[1]}):</b> {en_yakin_seviye[0]:,.2f}"
        else:
            note += f"\n🎯 <b>En Yakın Seviye ({en_yakin_seviye[1]}):</b> {en_yakin_seviye[0]:,.2f}"
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
# MATEMATİK (Wilder's Smoothing + SuperTrend Pine Script Birebir)
# ==========================================
def get_rsi(series, period=8):
    delta = series.diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    avg_gain = gain.ewm(alpha=1/period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1/period, adjust=False, min_periods=period).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def get_atr_series(df, period=10):
    if df is None or len(df) < period + 1: return pd.Series(index=df.index, dtype=float)
    tr1 = df['high'] - df['low']
    tr2 = (df['high'] - df['close'].shift()).abs()
    tr3 = (df['low'] - df['close'].shift()).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return tr.ewm(alpha=1/period, adjust=False, min_periods=period).mean()

def calculate_supertrend(df, period=10, multiplier=3):
    if df is None or len(df) < period + 1: return pd.Series(index=df.index, dtype=float)
    
    hl2 = (df['high'] + df['low']) / 2
    atr = get_atr_series(df, period)
    
    upperband = hl2 + (multiplier * atr)
    lowerband = hl2 - (multiplier * atr)
    
    final_upperband = upperband.copy()
    final_lowerband = lowerband.copy()
    supertrend = pd.Series(index=df.index, dtype=float)
    trend = pd.Series(index=df.index, dtype=int)
    
    trend.iloc[0] = 1
    
    for i in range(1, len(df)):
        if pd.isna(atr.iloc[i]):
            supertrend.iloc[i] = np.nan
            trend.iloc[i] = 1
            continue
            
        # Final Bant Hesaplamaları (Pine Script Birebir)
        if df['close'].iloc[i-1] > final_upperband.iloc[i-1]:
            final_upperband.iloc[i] = upperband.iloc[i]
        else:
            final_upperband.iloc[i] = min(upperband.iloc[i], final_upperband.iloc[i-1])
            
        if df['close'].iloc[i-1] < final_lowerband.iloc[i-1]:
            final_lowerband.iloc[i] = lowerband.iloc[i]
        else:
            final_lowerband.iloc[i] = max(lowerband.iloc[i], final_lowerband.iloc[i-1])
            
        # Trend Yönü
        if df['close'].iloc[i] > final_upperband.iloc[i-1]:
            trend.iloc[i] = 1
        elif df['close'].iloc[i] < final_lowerband.iloc[i-1]:
            trend.iloc[i] = -1
        else:
            trend.iloc[i] = trend.iloc[i-1]
            
        # SuperTrend Değeri
        if trend.iloc[i] == 1:
            supertrend.iloc[i] = final_lowerband.iloc[i]
        else:
            supertrend.iloc[i] = final_upperband.iloc[i]
            
    return supertrend

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
                
                now = datetime.now(TIMEZONE)
                last_bar_time = df.index[-1]
                tf_mins = {"15m": 15, "30m": 30, "1h": 60, "4h": 240, "5m": 5, "1d": 1440}.get(interval, 0)
                if tf_mins and now < last_bar_time + timedelta(minutes=tf_mins):
                    return df.iloc[:-1]
                return df
        except Exception as e:
            print(f"⚠️ Veri çekme hatası ({symbol} {interval}): {e}")
            time_module.sleep(1)
    return None

async def fetch_ohlc_async(symbol, interval, limit=500):
    if interval == "5m":
        cache_key = f"{symbol}_{interval}"
        if cache_key in CACHE_5M:
            cached_df, cache_time = CACHE_5M[cache_key]
            if time_module.time() - cache_time < CACHE_TTL:
                return cached_df.copy()

    try:
        async with FETCH_SEM:
            loop = asyncio.get_running_loop()
            df = await asyncio.wait_for(
                loop.run_in_executor(EXECUTOR, fetch_ohlc, symbol, interval, limit),
                timeout=15.0
            )
            
            if interval == "5m" and df is not None:
                CACHE_5M[f"{symbol}_{interval}"] = (df, time_module.time())
                
            return df
    except asyncio.TimeoutError:
        print(f"⏳ Timeout: {symbol} {interval}")
        return None
    except Exception as e:
        print(f"❌ Async Fetch Hatası ({symbol}): {e}")
        return None

async def fetch_all_tfs_async(symbol):
    results = await asyncio.gather(*[fetch_ohlc_async(symbol, tf) for tf in FETCH_TFS])
    return dict(zip(FETCH_TFS, results))

# ==========================================
# SİNYAL MOTORLARI
# ==========================================
def get_volume_and_trend_tag(df_current, df_4h):
    vol_ma = df_current['volume'].rolling(VOLUME_LOOKBACK).mean().shift(1)
    current_vol = df_current['volume'].iloc[-1]
    avg_vol = vol_ma.iloc[-1]
    vol_tag = "⚠️ <b>Hacimsiz</b>"
    if pd.notna(avg_vol) and avg_vol > 0 and current_vol >= (avg_vol * VOLUME_MULT):
        vol_tag = "🔥 <b>Hacim Onaylı</b>"
        
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
        bar_time = df.index[-1]
        inst_key = f"inst_{symbol}_{tf}"
        if inst_key not in instant_states:
            instant_states[inst_key] = {"last_bar": None}
        if instant_states[inst_key]["last_bar"] != bar_time:
            delay_min = (datetime.now(TIMEZONE) - bar_time).total_seconds() / 60
            if delay_min > 180: continue
            direction = None
            if r_prev <= INSTANT_SHORT < r_now: direction = "SHORT"
            elif r_prev >= INSTANT_LONG > r_now: direction = "LONG"
            if direction:
                instant_states[inst_key]["last_bar"] = bar_time
                tags = get_volume_and_trend_tag(df, df_4h)
                earnings_note = await get_earnings_note_async(symbol)
                current_price = float(df['close'].iloc[-1])
                fibo_note = await get_nearest_fibo_levels_async(symbol, current_price)
                
                if direction == "SHORT":
                    msg = f"✨ <b>RSI BÜYÜSÜ (1H)</b> ✨\n⚠️ <b>{symbol}</b> — İLK RSI UYARISI\nRSI 8: {r_prev:.1f} → {r_now:.1f} (Aşırı Alım)\n📊 {tags}\n\n🚨 <b>Alarm Fiyatı:</b> {current_price:,.2f}\n🎯 <b>Olası Giriş:</b> {current_price * 1.02:,.2f} (Tahmini +%2)"
                else:
                    msg = f"✨ <b>RSI BÜYÜSÜ (1H)</b> ✨\n⚠️ <b>{symbol}</b> — İLK RSI UYARISI\nRSI 8: {r_prev:.1f} → {r_now:.1f} (Aşırı Satım)\n📊 {tags}\n\n🚨 <b>Alarm Fiyatı:</b> {current_price:,.2f}\n🎯 <b>Olası Giriş:</b> {current_price * 0.98:,.2f} (Tahmini -%2)"
                
                msg += fibo_note
                msg += "\n👀 <b>Grafiği kontrol edin. Teyit sinyali bekleniyor...</b>"
                msg += earnings_note
                msgs.append({"text": msg, "symbol": symbol, "tf": tf, "type": "alert_instant", "direction": direction})

    for tf in TIME_FRAMES_ATR:
        df = dfs.get(tf)
        if df is None or len(df) < 30: continue
        rsi = get_rsi(df["close"], period=RSI_PERIOD)
        if rsi is None or len(rsi) < 2: continue
        atr_series = get_atr_series(df, period=10)
        
        bar_time = df.index[-1]
        atr_key = f"atr_{symbol}_{tf}"
        if atr_key not in atr_states:
            atr_states[atr_key] = {"state": "IDLE", "peak_rsi": 0, "peak_price": 0, "low_rsi": 100, "low_price": 0, "last_bar": None, "reject_count": 0}
        
        atr_data = atr_states[atr_key]
        if atr_data.get("last_bar") != bar_time:
            new_bars = df[df.index > atr_data["last_bar"]] if atr_data.get("last_bar") else df.tail(1)
            
            for idx, row in new_bars.iterrows():
                delay_min = (datetime.now(TIMEZONE) - idx).total_seconds() / 60
                max_delay = 60 if tf == "15m" else (90 if tf == "30m" else (180 if tf == "1h" else 300))
                if delay_min > max_delay: continue

                r_now = rsi.loc[idx] if idx in rsi.index else get_rsi(df.loc[:idx, 'close'], period=RSI_PERIOD).iloc[-1]
                if pd.isna(r_now): continue
                
                close_price = row['close']
                candle_high = row['high']
                candle_low = row['low']
                state = atr_data["state"]
                
                atr_val = atr_series.loc[idx] if idx in atr_series.index else None
                if pd.isna(atr_val) or atr_val is None or atr_val <= 0: continue
                
                if state == "IDLE":
                    if r_now > ALARM_SHORT_UPPER:
                        atr_data.update({"state": "SHORT_ARMED", "peak_rsi": r_now, "peak_price": candle_high, "reject_count": 0})
                    elif r_now < ALARM_LONG_LOWER:
                        atr_data.update({"state": "LONG_ARMED", "low_rsi": r_now, "low_price": candle_low, "reject_count": 0})
                elif state == "SHORT_ARMED":
                    if r_now > atr_data["peak_rsi"]:
                        atr_data["peak_rsi"] = r_now
                        atr_data["peak_price"] = candle_high
                    price_ok = (atr_data["peak_price"] - close_price) >= atr_val * ALARM_ATR_DROP
                    candle_ok = close_price < row['open']
                    
                    if price_ok and candle_ok:
                        tags = get_volume_and_trend_tag(df, df_4h)
                        earnings_note = await get_earnings_note_async(symbol)
                        fibo_note = await get_nearest_fibo_levels_async(symbol, close_price)
                        entry_time = idx + timedelta(minutes=TF_MINUTES[tf])
                        sl = close_price * (1 + SL_PCT / 100)
                        tp = close_price * (1 - TP_PCT / 100)
                        sl_pct = ((sl - close_price) / close_price) * 100
                        
                        msg = f"✨ <b>ATR DÜNYASI (Short) - {symbol} [{tf.upper()}]</b> ✨\n🔴 RSI Aşırı Alım Zirvesi: {atr_data['peak_rsi']:.1f}\n📊 {tags}\n\n💰 <b>Olası Giriş (Mum Kapanışı):</b> {close_price:,.2f}\n🛑 <b>Stop:</b> {sl:,.2f} (-%{sl_pct:.2f})\n🎯 <b>TP (%{TP_PCT}):</b> {tp:,.2f}\n⏳ <i>Zaman Aşımı: {TIMEOUT_HOURS} Seans Saati</i>"
                        msg += fibo_note; msg += earnings_note
                        msgs.append({"text": msg, "symbol": symbol, "tf": tf, "type": "atr_dunyasi", "direction": "SHORT", "price": close_price, "sl": sl, "tp": tp, "bar_time": entry_time})
                        atr_data["state"] = "IDLE"
                    else:
                        atr_data["reject_count"] += 1
                        if atr_data["reject_count"] >= 10 or r_now < 60: atr_data.update({"state": "IDLE", "reject_count": 0})
                elif state == "LONG_ARMED":
                    if r_now < atr_data["low_rsi"]:
                        atr_data["low_rsi"] = r_now
                        atr_data["low_price"] = candle_low
                    price_ok = (close_price - atr_data["low_price"]) >= atr_val * ALARM_ATR_DROP
                    candle_ok = close_price > row['open']
                    
                    if price_ok and candle_ok:
                        tags = get_volume_and_trend_tag(df, df_4h)
                        earnings_note = await get_earnings_note_async(symbol)
                        fibo_note = await get_nearest_fibo_levels_async(symbol, close_price)
                        entry_time = idx + timedelta(minutes=TF_MINUTES[tf])
                        sl = close_price * (1 - SL_PCT / 100)
                        tp = close_price * (1 + TP_PCT / 100)
                        sl_pct = ((close_price - sl) / close_price) * 100
                        
                        msg = f"✨ <b>ATR DÜNYASI (Long) - {symbol} [{tf.upper()}]</b> ✨\n🟢 RSI Aşırı Satım Dibi: {atr_data['low_rsi']:.1f}\n📊 {tags}\n\n💰 <b>Olası Giriş (Mum Kapanışı):</b> {close_price:,.2f}\n🛑 <b>Stop:</b> {sl:,.2f} (-%{sl_pct:.2f})\n🎯 <b>TP (%{TP_PCT}):</b> {tp:,.2f}\n⏳ <i>Zaman Aşımı: {TIMEOUT_HOURS} Seans Saati</i>"
                        msg += fibo_note; msg += earnings_note
                        msgs.append({"text": msg, "symbol": symbol, "tf": tf, "type": "atr_dunyasi", "direction": "LONG", "price": close_price, "sl": sl, "tp": tp, "bar_time": entry_time})
                        atr_data["state"] = "IDLE"
                    else:
                        atr_data["reject_count"] += 1
                        if atr_data["reject_count"] >= 10 or r_now > 40: atr_data.update({"state": "IDLE", "reject_count": 0})
            
            atr_data["last_bar"] = bar_time
    return msgs

# ==========================================
# ÜÇLÜ EMA + 15M SUPERTREND GİRİŞ/STOP SİSTEMİ
# ==========================================
async def check_triple_ema():
    global active_triple_ema_trades
    if not is_market_open(): return
    
    # 1. AÇIK İŞLEMLERİN KONTROLÜ (Giriş tetiklendi mi? SL/TP vuruldu mu?)
    for sym, trade in list(active_triple_ema_trades.items()):
        df = await fetch_ohlc_async(sym, "15m", limit=100) 
        if df is None or len(df) < 20: continue
        
        current_price = df['close'].iloc[-1]
        current_high = df['high'].iloc[-1]
        current_low = df['low'].iloc[-1]
        
        entry = trade["entry"]
        sl = trade["sl"]
        tp = trade["tp"]
        direction = trade["type"]
        
        # Giriş yapılmadıysa fiyatın giriş seviyesine değip değmediğini kontrol et
        if not trade.get("entered", False):
            entry_hit = False
            if direction == "LONG" and current_low <= entry:
                entry_hit = True
            elif direction == "SHORT" and current_high >= entry:
                entry_hit = True
                
            if entry_hit:
                trade["entered"] = True
                save_trades()
                msg = f"✅ <b>ÜÇLÜ EMA GİRİŞ TETİKLENDİ - {sym} [15M]</b>\nFiyat Süper Trend seviyesine ({entry:,.2f}) geri çekildi. Pozisyon açıldı.\n🛑 <b>Stop:</b> {sl:,.2f}\n🎯 <b>Hedef:</b> {tp:,.2f}"
                for cid in CHAT_IDS:
                    try: await bot_app.bot.send_message(chat_id=cid, text=msg, parse_mode="HTML")
                    except: pass
            continue # Giriş yapılmadıysa SL/TP kontrolüne geçme
            
        closed = False
        pnl_pct = 0
        
        # Giriş yapıldıysa SL/TP kontrol et
        if direction == "LONG":
            if current_low <= sl:
                pnl_pct = ((sl - entry) / entry) * 100
                msg = f"🛑 <b>ÜÇLÜ EMA STOP - {sym} [15M]</b>\nFiyat Stop seviyesine ({sl:,.2f}) değdi.\n<b>Net K/Z:</b> %{pnl_pct:+.2f}"
                closed = True
            elif current_high >= tp:
                pnl_pct = ((tp - entry) / entry) * 100
                msg = f"🎯 <b>ÜÇLÜ EMA TP - {sym} [15M]</b>\nFiyat Hedef seviyesine ({tp:,.2f}) ulaştı.\n<b>Net K/Z:</b> %{pnl_pct:+.2f}"
                closed = True
        else:
            if current_high >= sl:
                pnl_pct = ((entry - sl) / entry) * 100
                msg = f"🛑 <b>ÜÇLÜ EMA STOP - {sym} [15M]</b>\nFiyat Stop seviyesine ({sl:,.2f}) değdi.\n<b>Net K/Z:</b> %{pnl_pct:+.2f}"
                closed = True
            elif current_low <= tp:
                pnl_pct = ((entry - tp) / entry) * 100
                msg = f"🎯 <b>ÜÇLÜ EMA TP - {sym} [15M]</b>\nFiyat Hedef seviyesine ({tp:,.2f}) ulaştı.\n<b>Net K/Z:</b> %{pnl_pct:+.2f}"
                closed = True
                
        if closed:
            for cid in CHAT_IDS:
                try: await bot_app.bot.send_message(chat_id=cid, text=msg, parse_mode="HTML")
                except: pass
            del active_triple_ema_trades[sym]
            save_trades()

    # 2. YENİ SİNYAL TARAMASI (5M EMA Kesişimi)
    sem = asyncio.Semaphore(10)
    async def scan_single(sym):
        async with sem:
            df_5m = await fetch_ohlc_async(sym, "5m", limit=500)
            if df_5m is None or len(df_5m) < 200: return
            
            ema21 = df_5m['close'].ewm(span=21, adjust=False).mean()
            ema200 = df_5m['close'].ewm(span=200, adjust=False).mean()
            
            bar_time = df_5m.index[-1]
            delay_min = (datetime.now(TIMEZONE) - bar_time).total_seconds() / 60
            if delay_min > 25: return 
            
            state_key = f"triple_ema_{sym}"
            if state_key not in triple_ema_states:
                triple_ema_states[state_key] = {"last_bar": None}
                
            if triple_ema_states[state_key]["last_bar"] != bar_time:
                curr_21 = ema21.iloc[-1]
                curr_200 = ema200.iloc[-1]
                prev_21 = ema21.iloc[-2]
                prev_200 = ema200.iloc[-2]
                
                direction = None
                if prev_21 <= prev_200 and curr_21 > curr_200: direction = "LONG"
                elif prev_21 >= prev_200 and curr_21 < curr_200: direction = "SHORT"
                
                if direction:
                    triple_ema_states[state_key]["last_bar"] = bar_time
                    
                    existing = active_triple_ema_trades.get(sym)
                    if existing and existing["type"] == direction:
                        return 
                    
                    if existing and existing["type"] != direction:
                        del active_triple_ema_trades[sym]
                        save_trades()
                    
                    # 15 DAKİKALIK SUPERTREND GİRİŞ & STOP HESABI
                    df_15m = await fetch_ohlc_async(sym, "15m", limit=100)
                    if df_15m is None or len(df_15m) < 20: return
                    
                    st_series = calculate_supertrend(df_15m, period=10, multiplier=3)
                    st_val = st_series.iloc[-1]
                    
                    if pd.isna(st_val): return
                    
                    entry_price = st_val
                    
                    if direction == "LONG":
                        sl = entry_price * (1 - 0.015) # %1.5 Alt Stop
                        tp = entry_price * (1 + 0.03)  # %3 Kâr Al
                    else:
                        sl = entry_price * (1 + 0.015) # %1.5 Üst Stop
                        tp = entry_price * (1 - 0.03)  # %3 Kâr Al
                        
                    emoji = "📈" if direction == "LONG" else "📉"
                    msg = f"{emoji} <b>ÜÇLÜ EMA SİNYALİ - {sym} [5M]</b> {emoji}\n"
                    msg += f"🚨 <b>Yön:</b> {direction}\n"
                    msg += f"21 EMA, 200 EMA'yı {'yukarı' if direction == 'LONG' else 'aşağı'} kesti.\n\n"
                    msg += f"💰 <b>Olası Giriş (15M Süper Trend):</b> {entry_price:,.2f}\n"
                    msg += f"🛑 <b>Stop (Girişin %1.5'i):</b> {sl:,.2f}\n"
                    msg += f"🎯 <b>Hedef (%3):</b> {tp:,.2f}\n"
                    msg += f"\n💡 <i>Fiyat 15M Süper Trend seviyesine ({entry_price:,.2f}) geri çekildiğinde pozisyon açılacaktır.</i>"
                    
                    for cid in CHAT_IDS:
                        try: await bot_app.bot.send_message(chat_id=cid, text=msg, parse_mode="HTML")
                        except Exception as e: print(f"Mesaj hatası: {e}")
                        
                    active_triple_ema_trades[sym] = {
                        "symbol": sym, 
                        "type": direction, 
                        "entry": entry_price, 
                        "sl": sl,
                        "tp": tp,
                        "entered": False,
                        "entry_time": bar_time
                    }
                    save_trades()

    tasks = [scan_single(sym) for sym in SYMBOLS]
    await asyncio.gather(*tasks)

# ==========================================
# 5M TEYİT TAKİBİ
# ==========================================
async def check_5m_confirmations():
    if not pending_confirmations: return
    now = datetime.now(TIMEZONE)
    
    for key, data in list(pending_confirmations.items()):
        sym = data["symbol"]
        direction = data["direction"]
        msg_id = data["msg_id"]
        alert_time = data["time"]
        
        if (now - alert_time).total_seconds() / 60 > 120:
            del pending_confirmations[key]
            continue
            
        df_5m = await fetch_ohlc_async(sym, "5m", limit=500)
        if df_5m is None or len(df_5m) < 200: continue
        
        ema21 = df_5m['close'].ewm(span=21, adjust=False).mean()
        ema200 = df_5m['close'].ewm(span=200, adjust=False).mean()
        
        df_after_alert = df_5m[df_5m.index >= alert_time]
        if df_after_alert.empty: continue
        
        confirmed_21 = False
        confirmed_200 = False
        last_ema21 = 0
        last_ema200 = 0
        
        for i in range(1, len(df_after_alert)):
            curr_idx = df_5m.index.get_loc(df_after_alert.index[i])
            prev_idx = curr_idx - 1
            
            last_close = df_5m['close'].iloc[curr_idx]
            prev_close = df_5m['close'].iloc[prev_idx]
            last_ema21 = ema21.iloc[curr_idx]
            prev_ema21 = ema21.iloc[prev_idx]
            last_ema200 = ema200.iloc[curr_idx]
            prev_ema200 = ema200.iloc[prev_idx]
            
            if direction == "SHORT":
                if prev_close >= prev_ema21 and last_close < last_ema21: confirmed_21 = True
                if prev_close >= prev_ema200 and last_close < last_ema200: confirmed_200 = True
            elif direction == "LONG":
                if prev_close <= prev_ema21 and last_close > last_ema21: confirmed_21 = True
                if prev_close <= prev_ema200 and last_close > last_ema200: confirmed_200 = True
            
            if confirmed_21 or confirmed_200:
                break
                
        if confirmed_21 or confirmed_200:
            ema_txt = ""
            if confirmed_21 and confirmed_200: ema_txt = "EMA 21 ve EMA 200"
            elif confirmed_21: ema_txt = "EMA 21"
            elif confirmed_200: ema_txt = "EMA 200"
            
            msg = f"✅ <b>Teyit Sinyali (5M) - {sym}</b>\n"
            msg += f"5 Dk mumu <b>{ema_txt}</b> seviyesini {'aşağı' if direction == 'SHORT' else 'yukarı'} kırdı.\n"
            msg += f"📊 <b>EMA 21:</b> {last_ema21:,.2f} | <b>EMA 200:</b> {last_ema200:,.2f}\n"
            msg += f"👀 İlk RSI uyarınız teyit edildi, işlem değerlendirebilirsiniz."
            
            for cid in CHAT_IDS:
                reply_id = msg_id.get(str(cid))
                try: await bot_app.bot.send_message(chat_id=cid, text=msg, parse_mode="HTML", reply_to_message_id=reply_id)
                except: 
                    try: await bot_app.bot.send_message(chat_id=cid, text=msg, parse_mode="HTML")
                    except Exception as e: print(f"Teyit mesaj hatası: {e}")
                    
            del pending_confirmations[key]

# ==========================================
# BIST 30 ÖZETİ
# ==========================================
async def generate_market_summary(session_name):
    try:
        df_30m, df_1h = await asyncio.gather(fetch_ohlc_async("XU030", "30m", limit=500), fetch_ohlc_async("XU030", "1h", limit=500))
        if df_30m is None or df_1h is None: return
        today = datetime.now(TIMEZONE).date()
        df_today = df_30m[df_30m.index.date == today]
        if df_today.empty: return
        
        if "Sabah" in session_name:
            session_df = df_today.between_time('09:55', '14:00'); time_str = "09:55 - 14:00"
        else:
            session_df = df_today.between_time('14:01', '18:05'); time_str = "14:01 - 18:05"
            
        if session_df.empty: return
        open_price = session_df['open'].iloc[0]; close_price = session_df['close'].iloc[-1]
        high_price = session_df['high'].max(); low_price = session_df['low'].min()
        pct_change = ((close_price - open_price) / open_price) * 100
        dist_to_high = ((high_price - close_price) / high_price) * 100; dist_to_low = ((close_price - low_price) / low_price) * 100
        if dist_to_high < 0.2: close_status = "🟢 Zirvede Kapattı (Güçlü)"
        elif dist_to_low < 0.2: close_status = "🔴 Dipte Kapattı (Zayıf)"
        else: close_status = "⚪ Orta Bölgede Kapattı"
        
        session_vol = session_df['volume'].sum(); past_vols = []
        for i in range(1, 4):
            past_date = today - timedelta(days=i)
            past_df = df_30m[df_30m.index.date == past_date]
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
                if len(h) >= 2:
                    return s, ((h['close'].iloc[-1] - h['close'].iloc[-2]) / h['close'].iloc[-2]) * 100
            except Exception as e: pass
            return s, None

        sem_summary = asyncio.Semaphore(8)
        async def limited_change(s):
            async with sem_summary:
                return await asyncio.to_thread(_inner_change, s)
                
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
            greens = sum(1 for s in sector_stocks if stock_changes[s] > 0)
            reds = len(sector_stocks) - greens
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
            except Exception as e: pass
    except Exception as e: pass

async def analyze_symbol(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id not in CHAT_IDS: return
    text = update.message.text.upper().replace("/", "").strip()
    if not text.isalnum() or len(text) < 2 or len(text) > 10: return
    if text in ["START", "HELP", "YARDIM", "TARA"]: return
    if text not in SYMBOLS:
        await update.message.reply_text(f"❌ '{text}' tarama listesinde bulunamadı.")
        return
    await update.message.reply_text(f"🔍 {text} analiz ediliyor, lütfen bekleyin...")
    try:
        df_30m, df_1h, df_4h, df_1d = await asyncio.gather(fetch_ohlc_async(text, "30m", limit=500), fetch_ohlc_async(text, "1h", limit=500), fetch_ohlc_async(text, "4h", limit=500), fetch_ohlc_async(text, "1d", limit=500))
        if df_1h is None or df_4h is None:
            await update.message.reply_text("❌ Veri çekilemedi."); return
        msg = f"🤖 <b>{text} — Kapsamlı Analiz</b>\n"
        price = df_1h['close'].iloc[-1]
        rsi_1h = get_rsi(df_1h['close'], RSI_PERIOD).iloc[-1]
        trend_1h = "🟢 Yukarı" if price > df_1h['close'].ewm(span=200, adjust=False).mean().iloc[-1] else "🔴 Aşağı"
        msg += f"\n📊 <b>1 Saatlik (1H)</b>\n• Fiyat: {price:,.2f}\n• RSI(8): {rsi_1h:.1f}\n• Trend: {trend_1h} (EMA 200)\n"
        rsi_4h = get_rsi(df_4h['close'], RSI_PERIOD).iloc[-1]
        trend_4h = "🟢 Yukarı" if price > df_4h['close'].ewm(span=200, adjust=False).mean().iloc[-1] else "🔴 Aşağı"
        msg += f"\n📊 <b>4 Saatlik (4H)</b>\n• RSI(8): {rsi_4h:.1f}\n• Trend: {trend_4h} (EMA 200)\n"
        if df_1d is not None: msg += f"\n📊 <b>Günlük (1D)</b>\n• RSI(14): {get_rsi(df_1d['close'], 14).iloc[-1]:.1f}\n"
        if df_30m is not None:
            ema50_30m = df_30m['close'].ewm(span=50, adjust=False).mean().iloc[-1]
            msg += f"\n⚓ <b>30 Dakikalık (30M)</b>\n• EMA 50: {ema50_30m:,.2f}\n"
        else:
            ema50_30m = 0
            msg += f"\n⚓ <b>30 Dakikalık (30M)</b>\n• EMA 50: Veri Yok\n"
        
        fibo_note = await get_nearest_fibo_levels_async(text, price)
        msg += fibo_note
        
        msg += f"\n💡 <b>Yorum ve Strateji:</b>\n"
        if trend_4h == "🟢 Yukarı":
            msg += f"Ana trend YUKARI. Alım yönlü işlemler düşünülebilir.\n"
            if df_30m is not None: msg += f"🛒 <b>Olası Alım Yeri:</b> 30m EMA 50 seviyesi olan <b>{ema50_30m:,.2f}</b>."
        else:
            msg += f"Ana trend AŞAĞI. Satış yönlü işlemler düşünülebilir.\n"
            if df_30m is not None: msg += f"🛒 <b>Olası Satım Yeri:</b> 30m EMA 50 seviyesi olan <b>{ema50_30m:,.2f}</b>."
        await update.message.reply_text(msg, parse_mode="HTML")
    except Exception as e:
        print(f"⚠️ Analiz hatası: {e}")
        await update.message.reply_text(f"❌ Analiz hatası: {e}")

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id not in CHAT_IDS: return
    await update.message.reply_text("✅ Sistem çalışıyor. Analiz için /ASELS gibi komut yazabilirsiniz. Manuel tarama için /tara yazın.")

async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id not in CHAT_IDS: return
    msg = (
        "🤖 <b>Yardım Menüsü</b>\n\n"
        "• Hisse analizi için: <code>/ASELS</code> veya <code>ASELS</code>\n"
        "• Endeks analizi için: <code>/XU030</code>\n"
        "• Tüm piyasayı anında taramak için: <code>/tara</code>\n\n"
        "Sistem, RSI Büyüsü ve ATR Dünyası sinyallerini otomatik olarak takip eder ve sonuçları size bildirir. Ayrıca 5m Üçlü EMA sinyalleri üretir."
    )
    await update.message.reply_text(msg, parse_mode="HTML")

async def cmd_tara(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    global is_scanning
    if update.effective_chat.id not in CHAT_IDS: return
    if is_scanning:
        await update.message.reply_text("⏳ Şu an arka planda bir tarama devam ediyor. Lütfen bitmesini bekleyin...")
        return
    
    await update.message.reply_text("🔄 Manuel tarama arka planda başlatıldı. Tüm hisseler kontrol ediliyor...")
    
    async def run_manual_scan():
        await tick(force=True)
        for cid in CHAT_IDS:
            try:
                await bot_app.bot.send_message(
                    chat_id=cid, 
                    text="✅ Tarama tamamlandı. Eğer sinyal koşulu oluştuysa mesajınız gelmiştir.", 
                    parse_mode="HTML"
                )
            except: pass

    task = asyncio.create_task(run_manual_scan())
    background_tasks.add(task)
    task.add_done_callback(background_tasks.discard)

# ==========================================
# BOT BAŞLATMA VE HANDLER'LAR
# ==========================================
bot_app = Application.builder().token(BOT_TOKEN).build()

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    print(f"🚨 Telegram Hatası: {context.error}")
    if update and update.effective_chat:
        try:
            await context.bot.send_message(chat_id=update.effective_chat.id, text="⚠️ Beklenmeyen bir hata oluştu, lütfen tekrar deneyin.")
        except: pass

bot_app.add_error_handler(error_handler)

async def send_signal_message(msg_data):
    text = msg_data["text"]
    symbol = msg_data["symbol"]
    tf = msg_data.get("tf")
    msg_type = msg_data["type"]
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
            except Exception as e: pass
        if msg_type == "atr_dunyasi" and "direction" in msg_data:
            async with trade_lock:
                if not msg_ids: return
                trade_info = {
                    "symbol": symbol, "type": msg_data["direction"], "entry": msg_data["price"],
                    "msg_ids": msg_ids, "signal_type": msg_type,
                    "sl": msg_data.get("sl"), "tp": msg_data.get("tp"),
                    "tf": tf, "entry_time": msg_data["bar_time"],
                    "last_checked_bar": msg_data["bar_time"]
                }
                active_trades[trade_key] = trade_info
                save_trades()
        if msg_type == "alert_instant":
            key = f"{symbol}_{msg_data['direction']}"
            pending_confirmations[key] = {
                "symbol": symbol,
                "direction": msg_data['direction'],
                "msg_id": msg_ids,
                "time": datetime.now(TIMEZONE)
            }
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
    if is_scanning:
        return
    is_scanning = True
    print(f"🔄 Hızlı Tarama başladı. (Zaman: {datetime.now(TIMEZONE).strftime('%H:%M')})")
    try:
        sem = asyncio.Semaphore(10)
        async def limited_fetch(sym):
            async with sem:
                return await fetch_all_tfs_async(sym)
        tasks = [limited_fetch(sym) for sym in SYMBOLS]
        
        try:
            results = await asyncio.wait_for(asyncio.gather(*tasks), timeout=300.0)
        except asyncio.TimeoutError:
            results = []
            
        for sym, dfs in zip(SYMBOLS, results):
            try:
                if dfs is None or not dfs: continue
                msgs = await process_signals(sym, dfs)
                for msg_data in msgs:
                    await send_signal_message(msg_data)
            except Exception as e: 
                pass 
    finally:
        is_scanning = False

async def tracker_tick():
    if not is_market_open(): return
    try: await check_active_trades()
    except Exception as e: pass
    try: await check_5m_confirmations()
    except Exception as e: pass
    try: await check_triple_ema()
    except Exception as e: pass

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
        instant_states.clear()
        atr_states.clear()
        earnings_cache.clear()
        fibo_cache.clear()
        pending_confirmations.clear()
        pending_trades.clear()
        CACHE_5M.clear()

async def task_morning_summary(): 
    await generate_market_summary("Sabah")

async def task_afternoon_summary(): 
    await generate_market_summary("Öğleden Sonra")

class DummyHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200); self.end_headers(); self.wfile.write(b"Alive")
    def do_HEAD(self):
        self.send_response(200); self.end_headers()
    def log_message(self, format, *args): return

def run_dummy_server():
    HTTPServer(("0.0.0.0", int(os.environ.get("PORT", 8080))), DummyHandler).serve_forever()

async def main():
    load_trades() 
    threading.Thread(target=run_dummy_server, daemon=True).start()
    await bot_app.initialize(); await bot_app.start()
    await bot_app.updater.start_polling(drop_pending_updates=True)
    
    sched = AsyncIOScheduler(timezone=TIMEZONE, job_defaults={'coalesce': True, 'max_instances': 1, 'misfire_grace_time': 60})
    
    sched.add_job(market_open_scan, CronTrigger(day_of_week="mon-fri", hour=9, minute=55, timezone=TIMEZONE))
    sched.add_job(tick, CronTrigger(day_of_week="mon-fri", hour="10-17", minute="1,16,31,46", timezone=TIMEZONE))
    sched.add_job(tracker_tick, CronTrigger(day_of_week="mon-fri", hour="10-17", minute="*/5", timezone=TIMEZONE))
    sched.add_job(tracker_tick, CronTrigger(day_of_week="mon-fri", hour=17, minute=58, timezone=TIMEZONE))
    
    sched.add_job(tick, CronTrigger(day_of_week="mon-fri", hour=18, minute=10, timezone=TIMEZONE), kwargs={'force': True})
    
    sched.add_job(clear_daily_state, CronTrigger(day_of_week="mon-fri", hour=18, minute=30, timezone=TIMEZONE))
    sched.add_job(task_morning_summary, CronTrigger(day_of_week="mon-fri", hour=14, minute=15, timezone=TIMEZONE))
    sched.add_job(task_afternoon_summary, CronTrigger(day_of_week="mon-fri", hour=18, minute=15, timezone=TIMEZONE))
    
    sched.start()
    print("✅ Bot çalışıyor. Çakışma koruması aktif. Loglar açık.")
    while True: await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())