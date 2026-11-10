import json, os
import asyncio
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from datetime import datetime, timedelta, time as dt_time
import time as time_module

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

CHAT_IDS = [5066109900, 8776376951]
TIMEZONE = pytz.timezone("Europe/Istanbul")

SYMBOLS = [
    "TOASO", "TRALT", "ASTOR", "GUBRF", "TRMET", "TCELL", "GARAN", "YKBNK", "TAVHL", "ENKAI",
    "TTKOM", "SAHOL", "SASA", "AEFES", "ASELS", "MGROS", "TUPRS", "AKBNK", "KCHOL", "KRDMD",
    "ISCTR", "THYAO", "BIMAS", "EREGL", "EKGYO", "PGSUS", "FROTO", "SISE", "VAKBN", "ALARK",
    "AKSEN", "ARCLK", "BRSAN", "CIMSA", "DOAS", "DOHOL", "ENJSA", "OYAKC", "PETKM", "SOKM",
    "TKFEN", "ULKER", "VESTL", "XU030", "XU100"
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

VOLUME_LOOKBACK = 20
VOLUME_MULT = 1.5
ATR_TP1_PCT = 1.5
ATR_TP2_PCT = 3.0
SL_PCT = 3.5 
TIMEOUT_HOURS = 10 

SWING_TARGET_PCT = 10.0 
SWING_TIMEOUT_HOURS = 120 

active_trades = {}
pending_trades = set() 
closed_trades = [] 
daily_alerts = []  
instant_states = {}       
atr_states = {}
earnings_cache = {}
fibo_cache = {}

trade_lock = asyncio.Lock()

DATA_DIR = os.getenv("DATA_DIR", "/tmp")
TRADES_FILE = os.path.join(DATA_DIR, "active_trades.json")

def save_trades():
    try:
        serializable_trades = {}
        for k, v in active_trades.items():
            trade_copy = v.copy()
            if isinstance(trade_copy["entry_time"], datetime):
                trade_copy["entry_time"] = trade_copy["entry_time"].isoformat()
            serializable_trades[k] = trade_copy
        with open(TRADES_FILE, "w", encoding="utf-8") as f:
            json.dump(serializable_trades, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"⚠️ Trade kayıt hatası: {e}")

def load_trades():
    global active_trades
    if os.path.exists(TRADES_FILE):
        try:
            with open(TRADES_FILE, "r", encoding="utf-8") as f:
                loaded = json.load(f)
                for k, v in loaded.items():
                    if "entry_time" in v and isinstance(v["entry_time"], str):
                        v["entry_time"] = datetime.fromisoformat(v["entry_time"])
                    active_trades[k] = v
            print(f"✅ {len(active_trades)} aktif işlem hafızadan yüklendi.")
        except Exception as e:
            print(f"⚠️ Trade yükleme hatası: {e}")
            active_trades = {}

def is_market_open(now=None):
    now = now or datetime.now(TIMEZONE)
    if now.weekday() > 4: return False
    tr_holidays = holidays.Turkey()
    if now.date() in tr_holidays: return False
    return dt_time(9, 55) <= now.time() <= dt_time(18, 0)

def get_effective_market_hours(entry_time, now_time):
    if now_time <= entry_time: return 0.0
    total_hours = 0.0
    current = entry_time
    market_open = dt_time(10, 0)
    market_close = dt_time(18, 0)
    while current.date() <= now_time.date():
        if current.weekday() > 4:
            current = TIMEZONE.localize(datetime.combine(current.date() + timedelta(days=1), market_open))
            continue
        day_open = TIMEZONE.localize(datetime.combine(current.date(), market_open))
        day_close = TIMEZONE.localize(datetime.combine(current.date(), market_close))
        if now_time < day_open: break
        start_of_period = max(current, day_open)
        end_of_period = min(now_time, day_close)
        if end_of_period > start_of_period:
            total_hours += (end_of_period - start_of_period).total_seconds() / 3600
        current = TIMEZONE.localize(datetime.combine(current.date() + timedelta(days=1), market_open))
    return total_hours

def get_earnings_note(symbol):
    today = datetime.now(TIMEZONE).date()
    cache_key = f"{symbol}_{today}"
    if cache_key in earnings_cache:
        return earnings_cache[cache_key]
    
    try:
        ticker = bp.Ticker(symbol)
        cal = ticker.calendar
        if not isinstance(cal, pd.DataFrame) or cal.empty:
            earnings_cache[cache_key] = ""
            return ""
        rapor_satirlari = cal[cal['Subject'].str.contains('Finansal Rapor', case=False, na=False)]
        if rapor_satirlari.empty:
            earnings_cache[cache_key] = ""
            return ""
        tarih_str = rapor_satirlari.iloc[0]['EndDate']
        earnings_date = datetime.strptime(tarih_str, "%d.%m.%Y").date()
        days_left = (earnings_date - today).days
        if 0 <= days_left <= 5:
            note = f"\n\n📅 <b>Bilanço Uyarısı:</b> Bilançoya {days_left} gün kaldı! (Dikkatli olun)"
            earnings_cache[cache_key] = note
            return note
    except Exception as e: 
        print(f"Bilanço okuma hatası ({symbol}): {e}")
    earnings_cache[cache_key] = ""
    return ""

def get_nearest_fibo_levels(symbol, current_price):
    today = datetime.now(TIMEZONE).date()
    cache_key = f"{symbol}_{today}_{int(current_price)}"
    if cache_key in fibo_cache:
        return fibo_cache[cache_key]

    try:
        df = bp.Ticker(symbol).history(period="3 bar", interval="1wk")
        if df is None or len(df) < 2:
            fibo_cache[cache_key] = ""
            return ""
        
        df.columns = [str(c).lower() for c in df.columns]
        gecen_hafta = df.iloc[-2]
        H, L, C = gecen_hafta['high'], gecen_hafta['low'], gecen_hafta['close']
        
        pivot = (H + L + C) / 3
        fark = H - L
        
        raw_levels = []
        ratios = [0.236, 0.382, 0.5, 0.618, 1.0, 1.272, 1.618, 2.618]
        for r in ratios:
            raw_levels.append((pivot - fark * r, str(r)))
            raw_levels.append((pivot + fark * r, str(r)))
            
        en_yakin_seviye = min(raw_levels, key=lambda x: abs(x[0] - current_price))
        
        note = "\n🎯 <b>FİBO SEVİYELERİ:</b>"
        
        if en_yakin_seviye[1] in ["0.5", "0.618"]:
            lvl_05 = next(l for l in raw_levels if l[1] == "0.5" and abs(l[0] - current_price) < fark * 2) 
            lvl_0618 = next(l for l in raw_levels if l[1] == "0.618" and abs(l[0] - current_price) < fark * 2) 
            
            if lvl_05 and lvl_0618:
                note += f"\n🎯 <b>0.5 Seviyesi:</b> {lvl_05[0]:,.2f}"
                note += f"\n🎯 <b>0.618 Seviyesi:</b> {lvl_0618[0]:,.2f}"
            else:
                note += f"\n🎯 <b>En Yakın Seviye ({en_yakin_seviye[1]}):</b> {en_yakin_seviye[0]:,.2f}"
        else:
            note += f"\n🎯 <b>En Yakın Seviye ({en_yakin_seviye[1]}):</b> {en_yakin_seviye[0]:,.2f}"
                
        note += "\n"
        fibo_cache[cache_key] = note
        return note
    except Exception as e:
        print(f"Fibo hesaplama hatası ({symbol}): {e}")
        fibo_cache[cache_key] = ""
        return ""

def get_volume_and_trend_tag(df_current, df_4h):
    vol_ma = df_current['volume'].rolling(VOLUME_LOOKBACK).mean()
    current_vol = df_current['volume'].iloc[-1]
    avg_vol = vol_ma.iloc[-1]
    vol_tag = "⚠️ <b>Hacimsiz</b>"
    if pd.notna(avg_vol) and avg_vol > 0 and current_vol >= (avg_vol * VOLUME_MULT):
        vol_tag = "🔥 <b>Hacim Onaylı</b>"
    trend_tag = ""
    if df_4h is not None and len(df_4h) > 10:
        rsi_4h = get_rsi(df_4h['close'], RSI_PERIOD).iloc[-1]
        if pd.notna(rsi_4h):
            trend_tag = "📈 <b>4H Trend: Yukarı</b>" if rsi_4h > 50 else "📉 <b>4H Trend: Aşağı</b>"
    return f"{vol_tag} | {trend_tag}"

def fetch_ohlc(symbol, interval, limit=300):
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
                return df.iloc[:-1] 
        except Exception as e:
            print(f"⚠️ Veri çekme hatası ({symbol} {interval}): {e}")
            time_module.sleep(1)
    return None

async def fetch_ohlc_async(symbol, interval, limit=300):
    return await asyncio.to_thread(fetch_ohlc, symbol, interval, limit)

async def fetch_all_tfs_async(symbol):
    results = await asyncio.gather(*[fetch_ohlc_async(symbol, tf) for tf in FETCH_TFS])
    return dict(zip(FETCH_TFS, results))

def get_rsi(series, period=8):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def calculate_atr(df, period=10):
    if df is None or len(df) < period + 1: return None
    tr1 = df['high'] - df['low']
    tr2 = (df['high'] - df['close'].shift()).abs()
    tr3 = (df['low'] - df['close'].shift()).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.rolling(window=period).mean()
    return float(atr.iloc[-1])

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
                earnings_note = get_earnings_note(symbol)
                current_price = float(df['close'].iloc[-1])
                fibo_note = get_nearest_fibo_levels(symbol, current_price)
                
                if direction == "SHORT":
                    hedef_fiyat = current_price * 1.02
                    msg = f"✨ <b>RSI BÜYÜSÜ (1H)</b> ✨\n⚠️ <b>{symbol}</b> — İLK RSI UYARISI\nRSI 8: {r_prev:.1f} → {r_now:.1f} (Aşırı Alım)\n📊 {tags}\n\n🚨 <b>Alarm Fiyatı:</b> {current_price:,.2f}\n🎯 <b>Olası Giriş:</b> {hedef_fiyat:,.2f} (Tahmini +%2)"
                else:
                    hedef_fiyat = current_price * 0.98
                    msg = f"✨ <b>RSI BÜYÜSÜ (1H)</b> ✨\n⚠️ <b>{symbol}</b> — İLK RSI UYARISI\nRSI 8: {r_prev:.1f} → {r_now:.1f} (Aşırı Satım)\n📊 {tags}\n\n🚨 <b>Alarm Fiyatı:</b> {current_price:,.2f}\n🎯 <b>Olası Giriş:</b> {hedef_fiyat:,.2f} (Tahmini -%2)"
                
                msg += fibo_note
                msg += "\n👀 <b>Grafiği kontrol edin. Teyit sinyali bekleniyor...</b>"
                msg += earnings_note
                msgs.append({"text": msg, "symbol": symbol, "tf": tf, "type": "alert_instant", "direction": direction})

    for tf in TIME_FRAMES_ATR:
        df = dfs.get(tf)
        if df is None or len(df) < 30: continue
        rsi = get_rsi(df["close"], period=RSI_PERIOD)
        if rsi is None or len(rsi) < 2: continue
        r_now = float(rsi.iloc[-1])
        atr = calculate_atr(df, period=10)
        if atr is None or atr <= 0: continue
        close_price = float(df["close"].iloc[-1])
        bar_time = df.index[-1]
        atr_key = f"atr_{symbol}_{tf}"
        if atr_key not in atr_states:
            atr_states[atr_key] = {"state": "IDLE", "peak_rsi": 0, "peak_price": 0, "low_rsi": 100, "low_price": 0, "last_bar": None, "reject_count": 0}
        atr_data = atr_states[atr_key]
        if atr_data.get("last_bar") != bar_time:
            delay_min = (datetime.now(TIMEZONE) - bar_time).total_seconds() / 60
            max_delay = 60 if tf == "15m" else (90 if tf == "30m" else (180 if tf == "1h" else 300))
            if delay_min > max_delay: continue
            atr_data["last_bar"] = bar_time
            state = atr_data["state"]
            if state == "IDLE":
                if r_now > ALARM_SHORT_UPPER:
                    atr_states[atr_key] = {"state": "SHORT_ARMED", "peak_rsi": r_now, "peak_price": close_price, "low_rsi": 100, "low_price": 0, "last_bar": bar_time, "reject_count": 0}
                elif r_now < ALARM_LONG_LOWER:
                    atr_states[atr_key] = {"state": "LONG_ARMED", "peak_rsi": 0, "peak_price": 0, "low_rsi": r_now, "low_price": close_price, "last_bar": bar_time, "reject_count": 0}
            elif state == "SHORT_ARMED":
                if r_now > atr_data["peak_rsi"]:
                    atr_data["peak_rsi"] = r_now
                    atr_data["peak_price"] = close_price
                price_ok = (atr_data["peak_price"] - close_price) >= atr * ALARM_ATR_DROP
                candle_ok = close_price < float(df['open'].iloc[-1])
                if price_ok and candle_ok:
                    tags = get_volume_and_trend_tag(df, df_4h)
                    earnings_note = get_earnings_note(symbol)
                    fibo_note = get_nearest_fibo_levels(symbol, close_price)
                    sl = close_price * (1 + SL_PCT / 100)
                    tp1 = close_price * (1 - ATR_TP1_PCT / 100)
                    tp2 = close_price * (1 - ATR_TP2_PCT / 100)
                    sl_pct = ((sl - close_price) / close_price) * 100
                    timeout_msg = "10 Seans Saati" if tf in ['15m', '30m'] else "Swing (Uzun Vade)"
                    msg = f"✨ <b>ATR DÜNYASI (Short) - {symbol} [{tf.upper()}]</b> ✨\n🔴 RSI Aşırı Alım Zirvesi: {atr_data['peak_rsi']:.1f}\n📊 {tags}\n\n💰 <b>Olası Giriş (Mum Kapanışı):</b> {close_price:,.2f}\n🛑 <b>Stop:</b> {sl:,.2f} (-%{sl_pct:.2f})\n🎯 <b>TP1 (%{ATR_TP1_PCT}):</b> {tp1:,.2f}\n🎯 <b>TP2 (%{ATR_TP2_PCT}):</b> {tp2:,.2f}\n⏳ <i>Zaman Aşımı: {timeout_msg}</i>"
                    msg += fibo_note
                    msg += earnings_note
                    msgs.append({"text": msg, "symbol": symbol, "tf": tf, "type": "atr_dunyasi", "direction": "SHORT", "price": close_price, "sl": sl, "tp1": tp1, "tp2": tp2})
                    atr_states[atr_key]["state"] = "IDLE"
                else:
                    atr_data["reject_count"] += 1
                    if atr_data["reject_count"] >= 10: atr_states[atr_key]["state"] = "IDLE"
                    elif r_now < ALARM_LONG_LOWER:
                        atr_states[atr_key] = {"state": "LONG_ARMED", "peak_rsi": 0, "peak_price": 0, "low_rsi": r_now, "low_price": close_price, "last_bar": bar_time, "reject_count": 0}
            elif state == "LONG_ARMED":
                if r_now < atr_data["low_rsi"]:
                    atr_data["low_rsi"] = r_now
                    atr_data["low_price"] = close_price
                price_ok = (close_price - atr_data["low_price"]) >= atr * ALARM_ATR_DROP
                candle_ok = close_price > float(df['open'].iloc[-1])
                if price_ok and candle_ok:
                    tags = get_volume_and_trend_tag(df, df_4h)
                    earnings_note = get_earnings_note(symbol)
                    fibo_note = get_nearest_fibo_levels(symbol, close_price)
                    sl = close_price * (1 - SL_PCT / 100)
                    tp1 = close_price * (1 + ATR_TP1_PCT / 100)
                    tp2 = close_price * (1 + ATR_TP2_PCT / 100)
                    sl_pct = ((close_price - sl) / close_price) * 100
                    timeout_msg = "10 Seans Saati" if tf in ['15m', '30m'] else "Swing (Uzun Vade)"
                    msg = f"✨ <b>ATR DÜNYASI (Long) - {symbol} [{tf.upper()}]</b> ✨\n🟢 RSI Aşırı Satım Dibi: {atr_data['low_rsi']:.1f}\n📊 {tags}\n\n💰 <b>Olası Giriş (Mum Kapanışı):</b> {close_price:,.2f}\n🛑 <b>Stop:</b> {sl:,.2f} (-%{sl_pct:.2f})\n🎯 <b>TP1 (%{ATR_TP1_PCT}):</b> {tp1:,.2f}\n🎯 <b>TP2 (%{ATR_TP2_PCT}):</b> {tp2:,.2f}\n⏳ <i>Zaman Aşımı: {timeout_msg}</i>"
                    msg += fibo_note
                    msg += earnings_note
                    msgs.append({"text": msg, "symbol": symbol, "tf": tf, "type": "atr_dunyasi", "direction": "LONG", "price": close_price, "sl": sl, "tp1": tp1, "tp2": tp2})
                    atr_states[atr_key]["state"] = "IDLE"
                else:
                    atr_data["reject_count"] += 1
                    if atr_data["reject_count"] >= 10: atr_states[atr_key]["state"] = "IDLE"
                    elif r_now > ALARM_SHORT_UPPER:
                        atr_states[atr_key] = {"state": "SHORT_ARMED", "peak_rsi": r_now, "peak_price": close_price, "low_rsi": 100, "low_price": 0, "last_bar": bar_time, "reject_count": 0}
    return msgs

async def _fetch_stock_change(sym):
    def _inner():
        try:
            h = bp.Ticker(sym).history(period="5d", interval="1d")
            h.columns = [str(c).lower() for c in h.columns] 
            if len(h) >= 2:
                prev = h['close'].iloc[-2]
                last = h['close'].iloc[-1]
                return sym, ((last - prev) / prev) * 100
        except Exception: pass
        return sym, None
    return await asyncio.to_thread(_inner)

async def generate_market_summary(session_name):
    try:
        df_30m, df_1h = await asyncio.gather(fetch_ohlc_async("XU030", "30m", limit=250), fetch_ohlc_async("XU030", "1h", limit=250))
        if df_30m is None or df_1h is None: return
        today = datetime.now(TIMEZONE).date()
        df_today = df_30m[df_30m.index.date == today]
        if df_today.empty: return
        if "Sabah" in session_name:
            session_df = df_today.between_time('09:55', '14:00'); time_str = "09:55 - 14:00"
        else:
            session_df = df_today.between_time('14:01', '17:54'); time_str = "14:01 - 17:54"
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
        results = await asyncio.gather(*[_fetch_stock_change(s) for s in BIST_STOCKS])
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
        ema50_30m = df_30m['close'].ewm(span=50).mean().iloc[-1]
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
            except Exception as e: print(f"⚠️ Özet gönderme hatası: {e}")
    except Exception as e: print(f"⚠️ Market summary hatası: {e}")

async def analyze_symbol(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.upper().replace("/", "").strip()
    if not text.isalnum() or len(text) < 2 or len(text) > 10: return
    if text in ["START", "HELP", "YARDIM", "TARA"]: return
    if text not in SYMBOLS:
        await update.message.reply_text(f"❌ '{text}' tarama listesinde bulunamadı.")
        return
    await update.message.reply_text(f"🔍 {text} analiz ediliyor, lütfen bekleyin...")
    try:
        df_30m, df_1h, df_4h, df_1d = await asyncio.gather(fetch_ohlc_async(text, "30m", limit=250), fetch_ohlc_async(text, "1h", limit=250), fetch_ohlc_async(text, "4h", limit=250), fetch_ohlc_async(text, "1d", limit=250))
        if df_1h is None or df_4h is None:
            await update.message.reply_text("❌ Veri çekilemedi."); return
        msg = f"🤖 <b>{text} — Kapsamlı Analiz</b>\n"
        price = df_1h['close'].iloc[-1]
        rsi_1h = get_rsi(df_1h['close'], RSI_PERIOD).iloc[-1]
        trend_1h = "🟢 Yukarı" if price > df_1h['close'].ewm(span=200).mean().iloc[-1] else "🔴 Aşağı"
        msg += f"\n📊 <b>1 Saatlik (1H)</b>\n• Fiyat: {price:,.2f}\n• RSI(8): {rsi_1h:.1f}\n• Trend: {trend_1h} (EMA 200)\n"
        rsi_4h = get_rsi(df_4h['close'], RSI_PERIOD).iloc[-1]
        trend_4h = "🟢 Yukarı" if price > df_4h['close'].ewm(span=200).mean().iloc[-1] else "🔴 Aşağı"
        msg += f"\n📊 <b>4 Saatlik (4H)</b>\n• RSI(8): {rsi_4h:.1f}\n• Trend: {trend_4h} (EMA 200)\n"
        if df_1d is not None: msg += f"\n📊 <b>Günlük (1D)</b>\n• RSI(14): {get_rsi(df_1d['close'], 14).iloc[-1]:.1f}\n"
        if df_30m is not None:
            ema50_30m = df_30m['close'].ewm(span=50).mean().iloc[-1]
            msg += f"\n⚓ <b>30 Dakikalık (30M)</b>\n• EMA 50: {ema50_30m:,.2f}\n"
        else:
            ema50_30m = 0
            msg += f"\n⚓ <b>30 Dakikalık (30M)</b>\n• EMA 50: Veri Yok\n"
        
        fibo_note = get_nearest_fibo_levels(text, price)
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

async def check_active_trades():
    global active_trades, closed_trades
    if not active_trades: return
    trades_snapshot = list(active_trades.items())
    price_data = {}
    for trade_key, trade in trades_snapshot:
        sym = trade["symbol"]
        df_15m = await fetch_ohlc_async(sym, "15m", limit=100)
        if df_15m is not None and len(df_15m) >= 50:
            price_data[trade_key] = df_15m
    notifications = [] 
    async with trade_lock:
        dirty = False 
        for trade_key, df_15m in price_data.items():
            if trade_key not in active_trades: continue 
            trade = active_trades[trade_key]
            current_price = float(df_15m['close'].iloc[-1])
            current_high = float(df_15m['high'].iloc[-1])
            current_low = float(df_15m['low'].iloc[-1])
            t_type = trade["type"]
            entry = trade["entry"]
            tp1 = trade["tp1"]
            tp2 = trade["tp2"]
            sl_orig = trade["sl"]
            hit_tp1 = trade.get("hit_tp1", False)
            hit_tp2 = trade.get("tp2_hit", False)
            entry_time = trade["entry_time"]
            tf = trade["tf"]
            effective_hours = get_effective_market_hours(entry_time, datetime.now(TIMEZONE))
            if tf == "15m" and not trade.get("hit_1pct_alert_sent", False):
                profit_pct = ((current_price - entry) / entry) * 100 if t_type == "LONG" else ((entry - current_price) / entry) * 100
                if profit_pct >= 1.0:
                    trade["hit_1pct_alert_sent"] = True
                    dirty = True
                    msg = f"🔔 <b>15M Kâr Koruması - {sym}</b>\nFiyat %1 kâra ulaştı! (Mevcut: {current_price:,.2f})\n👉 <b>Stop seviyenizi maliyete (Giriş: {entry:,.2f}) çekin.</b>"
                    notifications.append((trade_key, msg, trade.get("msg_ids", {})))
            if tf in ["1h", "4h"] and hit_tp2:
                swing_sl = entry * (1 + 3.0/100) if t_type == "LONG" else entry * (1 - 3.0/100)
                swing_tp = entry * (1 + SWING_TARGET_PCT/100) if t_type == "LONG" else entry * (1 - SWING_TARGET_PCT/100)
                if effective_hours >= SWING_TIMEOUT_HOURS:
                    msg = f"⏳ <b>SWING ZAMAN AŞIMI - {sym} [{tf.upper()}]</b>\nSüre doldu. Lütfen kapatın.\n👉 <b>Mevcut Fiyat: {current_price:,.2f}</b>"
                    notifications.append((trade_key, msg, trade.get("msg_ids", {})))
                    closed_trades.append({"sym": sym, "type": t_type, "tf": tf, "entry": entry, "result": "TIMEOUT", "pct": 3.0, "time": datetime.now(TIMEZONE)})
                    del active_trades[trade_key]
                    dirty = True
                    continue
                if (t_type == "LONG" and current_low <= swing_sl) or (t_type == "SHORT" and current_high >= swing_sl):
                    msg = f"🛑 <b>SWING STOP (Kâr Korundu) - {sym} [{tf.upper()}]</b>\nTrend geri döndü, %3 kârla çıkış yapıldı."
                    notifications.append((trade_key, msg, trade.get("msg_ids", {})))
                    closed_trades.append({"sym": sym, "type": t_type, "tf": tf, "entry": entry, "result": "SWING_STOP", "pct": 3.0, "time": datetime.now(TIMEZONE)})
                    del active_trades[trade_key]
                    dirty = True
                    continue
                if (t_type == "LONG" and current_high >= swing_tp) or (t_type == "SHORT" and current_low <= swing_tp):
                    msg = f"🎯🎯 <b>SWING HEDEFİ (%10) VURULDU - {sym} [{tf.upper()}]</b>\nHarika bir trend yakaladınız! Kârı cebe alın."
                    notifications.append((trade_key, msg, trade.get("msg_ids", {})))
                    closed_trades.append({"sym": sym, "type": t_type, "tf": tf, "entry": entry, "result": "SWING_TP", "pct": SWING_TARGET_PCT, "time": datetime.now(TIMEZONE)})
                    del active_trades[trade_key]
                    dirty = True
                    continue
                continue 
            if effective_hours >= TIMEOUT_HOURS:
                pct = ATR_TP1_PCT / 2 if hit_tp1 else 0.0
                msg = f"⏳ <b>ZAMAN AŞIMI (10. Seans Saati) - {sym} [{tf.upper()}]</b>\nSüre doldu. Lütfen kapatın.\n👉 <b>Mevcut Fiyat: {current_price:,.2f}</b>"
                notifications.append((trade_key, msg, trade.get("msg_ids", {})))
                closed_trades.append({"sym": sym, "type": t_type, "tf": tf, "entry": entry, "result": "TIMEOUT", "pct": pct, "time": datetime.now(TIMEZONE)})
                del active_trades[trade_key]
                dirty = True
                continue
            current_sl = entry if (hit_tp1 or trade.get("hit_1pct_alert_sent")) else sl_orig
            if (t_type == "LONG" and current_low <= current_sl) or (t_type == "SHORT" and current_high >= current_sl):
                res_type = "SL"
                pct = 0.0
                if hit_tp1:
                    msg = f"🛑 <b>STOP (Breakeven) - {sym} [{tf.upper()}]</b>\nTP1 sonrası SL maliyete çekilmişti. Kalan pozisyon kapatıldı.\n<b>Net kâr:</b> +%{ATR_TP1_PCT/2:.2f} (TP1'den)"
                    pct = ATR_TP1_PCT/2
                    res_type = "BREAKEVEN"
                elif trade.get("hit_1pct_alert_sent"):
                    msg = f"🛑 <b>STOP (Breakeven) - {sym} [{tf.upper()}]</b>\n%1 kâr sonrası SL maliyete çekilmişti. Pozisyon kapatıldı.\n<b>Net kâr:</b> 0 (Sıfır zarar)"
                    pct = 0.0
                    res_type = "BREAKEVEN"
                else:
                    msg = f"🛑 <b>STOP OLDU - {sym} [{tf.upper()}]</b>\nİlk Stop ({sl_orig:,.2f}) seviyesine değdi. Pozisyon kapatıldı."
                    pct = -SL_PCT
                    res_type = "SL"
                notifications.append((trade_key, msg, trade.get("msg_ids", {})))
                closed_trades.append({"sym": sym, "type": t_type, "tf": tf, "entry": entry, "result": res_type, "pct": pct, "time": datetime.now(TIMEZONE)})
                del active_trades[trade_key]
                dirty = True
                continue
            if not hit_tp1:
                if (t_type == "LONG" and current_high >= tp1) or (t_type == "SHORT" and current_low <= tp1):
                    trade["hit_tp1"] = True
                    dirty = True
                    msg = f"🎯 <b>TP1 VURULDU (%{ATR_TP1_PCT}) - {sym} [{tf.upper()}]</b>\n👉 <b>Yarısını kapat.</b>\n🛑 <b>SL'yi maliyete çek:</b> {entry:,.2f}"
                    notifications.append((trade_key, msg, trade.get("msg_ids", {})))
                    continue
            if hit_tp1 and not hit_tp2:
                if (t_type == "LONG" and current_high >= tp2) or (t_type == "SHORT" and current_low <= tp2):
                    if tf in ["1h", "4h"]:
                        trade["tp2_hit"] = True
                        dirty = True
                        msg = f"🔥 <b>TREND BAŞLADI (Swing Modu) - {sym} [{tf.upper()}]</b>\nFiyat TP2 (%3) hedefine ulaştı. İşlemi kapatmayın!\n🛑 <b>Stopu %3 kâra çekin</b> (Giriş: {entry:,.2f})\n🎯 <b>Yeni Hedef:</b> %{SWING_TARGET_PCT} (Uzun Vade)\n⏳ <i>Bu işlem artık 1 hafta boyunca takip edilecek.</i>"
                        notifications.append((trade_key, msg, trade.get("msg_ids", {})))
                    else:
                        msg = f"🎯🎯 <b>TP2 VURULDU (%{ATR_TP2_PCT}) - {sym} [{tf.upper()}]</b>\n👉 <b>Kalanı kapat.</b>\n<b>Net kâr:</b> ~%{(ATR_TP1_PCT+ATR_TP2_PCT)/2:.2f}"
                        notifications.append((trade_key, msg, trade.get("msg_ids", {})))
                        pct = (ATR_TP1_PCT+ATR_TP2_PCT)/2
                        closed_trades.append({"sym": sym, "type": t_type, "tf": tf, "entry": entry, "result": "TP2", "pct": pct, "time": datetime.now(TIMEZONE)})
                        del active_trades[trade_key]
                        dirty = True
                    continue
        if dirty: save_trades()
    for trade_key, msg, msg_ids_snapshot in notifications:
        for cid in CHAT_IDS:
            reply_id = msg_ids_snapshot.get(str(cid))
            try: await bot_app.bot.send_message(chat_id=cid, text=msg, parse_mode="HTML", reply_to_message_id=reply_id)
            except: await bot_app.bot.send_message(chat_id=cid, text=msg, parse_mode="HTML")

async def daily_summary():
    global active_trades, closed_trades, daily_alerts
    async with trade_lock:
        snapshot_closed = list(closed_trades)
        snapshot_active = dict(active_trades)
        snapshot_alerts = list(daily_alerts)
    msg_rsi = f"📅 <b>GÜN SONU ÖZETİ - RSI BÜYÜSÜ</b> ({datetime.now(TIMEZONE).strftime('%d.%m.%Y')})\n\n"
    msg_rsi += "⚡ <b>Bugün Gelen İlk Uyarılar:</b>\n"
    if snapshot_alerts:
        for alert in snapshot_alerts:
            yon = "SHORT" if alert['direction'] == "SHORT" else "LONG"
            msg_rsi += f"• {alert['sym']} ({yon})\n"
        msg_rsi += f"\n<i>Bugün toplam {len(snapshot_alerts)} ilk uyarı verildi. İşleme dönüşenleri ATR Dünyası özetinden takip edebilirsiniz.</i>"
    else: msg_rsi += "<i>Bugün RSI Büyüsü ilk uyarısı verilmedi.</i>"
    for cid in CHAT_IDS:
        try: await bot_app.bot.send_message(chat_id=cid, text=msg_rsi, parse_mode="HTML")
        except: pass
    msg_atr = f"📅 <b>GÜN SONU ÖZETİ - ATR DÜNYASI</b> ({datetime.now(TIMEZONE).strftime('%d.%m.%Y')})\n\n"
    if snapshot_closed:
        msg_atr += "📈 <b>Kapanan İşlemler:</b>\n"
        total_pct = 0
        for tr in snapshot_closed:
            if tr['result'] in ["TP2", "SWING_TP"]: emoji, res_txt = "🟢", f"{tr['result']} (%{tr['pct']:.1f} Kâr)"
            elif tr['result'] in ["SL"]: emoji, res_txt = "🔴", f"SL (%{tr['pct']:.1f} Zarar)"
            elif tr['result'] in ["BREAKEVEN", "SWING_STOP", "TIMEOUT"]: 
                emoji = "⚪" if "BREAKEVEN" in tr['result'] or "SWING_STOP" in tr['result'] else "⏳"
                res_txt = f"{tr['result']} (%{tr['pct']:.2f})"
            else: emoji, res_txt = "⏳", "Zaman Aşımı (0)"
            msg_atr += f"{emoji} {tr['sym']} [{tr['tf'].upper()}] -> {res_txt}\n"
            total_pct += tr['pct']
        avg_pct = total_pct / len(snapshot_closed)
        msg_atr += f"<b>Kapanan İşlem Özeti:</b> Ortalama %{avg_pct:.2f}\n\n"
    else: msg_atr += "<i>Bugün kapanan ATR işlemi yok.</i>\n\n"
    if snapshot_active:
        msg_atr += f"⏳ <b>Devam Eden İşlemler ({len(snapshot_active)}):</b>\n"
        for key, tr in snapshot_active.items():
            eff_h = get_effective_market_hours(tr['entry_time'], datetime.now(TIMEZONE))
            swing_txt = " [🔥 SWING MODU]" if tr.get("tp2_hit") else ""
            msg_atr += f"• {tr['symbol']} [{tr['tf'].upper()}]{swing_txt} -> {eff_h:.0f} saat geçti\n"
        msg_atr += f"\n<i>Yarın en iyi kâr alma yerine gelecek olanlar var, takip etmeye devam!</i>"
    else: msg_atr += "<i>Devam eden ATR işlemi yok.</i>"
    for cid in CHAT_IDS:
        try: await bot_app.bot.send_message(chat_id=cid, text=msg_atr, parse_mode="HTML")
        except: pass

bot_app = Application.builder().token(BOT_TOKEN).build()

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
            except Exception as e: print(f"⚠️ Telegram Mesaj Hatası ({cid}): {e}")
        if msg_type == "atr_dunyasi" and "direction" in msg_data:
            async with trade_lock:
                if not msg_ids: return
                trade_info = {
                    "symbol": symbol, "type": msg_data["direction"], "entry": msg_data["price"],
                    "msg_ids": msg_ids, "signal_type": msg_type,
                    "sl": msg_data.get("sl"), "tp1": msg_data.get("tp1"), "tp2": msg_data.get("tp2"),
                    "hit_tp1": False, "tp2_hit": False, "tf": tf, "entry_time": datetime.now(TIMEZONE),
                    "hit_1pct_alert_sent": False
                }
                active_trades[trade_key] = trade_info
                save_trades()
        if msg_type == "alert_instant":
            async with trade_lock:
                daily_alerts.append({"sym": symbol, "direction": msg_data["direction"]})
    finally:
        if trade_key:
            async with trade_lock: pending_trades.discard(trade_key)

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("✅ Sistem çalışıyor. Analiz için /ASELS gibi komut yazabilirsiniz. Manuel tarama için /tara yazın.")

async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = (
        "🤖 <b>Yardım Menüsü</b>\n\n"
        "• Hisse analizi için: <code>/ASELS</code> veya <code>ASELS</code>\n"
        "• Endeks analizi için: <code>/XU030</code>\n"
        "• Tüm piyasayı anında taramak için: <code>/tara</code>\n\n"
        "Sistem, RSI Büyüsü ve ATR Dünyası sinyallerini otomatik olarak takip eder ve sonuçları size bildirir."
    )
    await update.message.reply_text(msg, parse_mode="HTML")

async def cmd_tara(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🔄 Manuel tarama başlatıldı. Tüm hisseler kontrol ediliyor, lütfen bekleyin...")
    await tick(force=True)
    await update.message.reply_text("✅ Tarama tamamlandı. Eğer sinyal koşulu oluştuysa mesajınız gelmiştir.")

auth_filter = filters.User(user_id=CHAT_IDS)

bot_app.add_handler(CommandHandler("start", cmd_start, filters=auth_filter))
bot_app.add_handler(CommandHandler(["help", "yardim", "durum"], cmd_help, filters=auth_filter))
bot_app.add_handler(CommandHandler("tara", cmd_tara, filters=auth_filter))
bot_app.add_handler(MessageHandler(filters.TEXT & auth_filter, analyze_symbol))

async def tick(force=False):
    if not force and not is_market_open(): return
    print(f"🔄 Hızlı Tarama başladı. (Zaman: {datetime.now(TIMEZONE).strftime('%H:%M')})")
    
    # Tüm hisseleri AYNI ANDA (paralel) tarıyoruz
    tasks = [fetch_all_tfs_async(sym) for sym in SYMBOLS]
    results = await asyncio.gather(*tasks)
    
    for sym, dfs in zip(SYMBOLS, results):
        try:
            if dfs is None or not dfs: continue
            msgs = await process_signals(sym, dfs)
            for msg_data in msgs:
                await send_signal_message(msg_data)
        except Exception as e: 
            print(f"❌ {sym} tarama hatası: {e}") 

async def tracker_tick():
    if not is_market_open(): return
    await check_active_trades()

async def market_open_scan():
    if not is_market_open(): return
    for cid in CHAT_IDS:
        try: 
            await bot_app.bot.send_message(chat_id=cid, text="🟢 Seans açıldı, tarama ve takip başladı.", parse_mode="HTML")
            await bot_app.bot.send_message(chat_id=cid, text="Sistem her zaman senin için çalışıyor, kazanmak istiyorsan sadece sistemle kal... MZB 😉", parse_mode="HTML")
        except: pass
    await tick(force=True)

async def task_morning_summary(): await generate_market_summary("Sabah")
async def task_afternoon_summary(): await generate_market_summary("Öğleden Sonra")

async def clear_daily_state():
    global closed_trades, daily_alerts
    async with trade_lock:
        instant_states.clear()
        atr_states.clear()
        earnings_cache.clear()
        fibo_cache.clear()
        closed_trades.clear()
        daily_alerts.clear()
        pending_trades.clear()

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
    sched = AsyncIOScheduler(timezone=TIMEZONE)
    sched.add_job(market_open_scan, CronTrigger(day_of_week="mon-fri", hour=9, minute=55, timezone=TIMEZONE))
    # Tarama saatleri mum kapanışına (06, 21, 36, 51) çekildi
    sched.add_job(tick, CronTrigger(day_of_week="mon-fri", hour="10-17", minute="6,21,36,51", timezone=TIMEZONE))
    sched.add_job(tracker_tick, CronTrigger(day_of_week="mon-fri", hour="10-17", minute="*/5", timezone=TIMEZONE))
    sched.add_job(tracker_tick, CronTrigger(day_of_week="mon-fri", hour=17, minute=58, timezone=TIMEZONE))
    sched.add_job(daily_summary, CronTrigger(day_of_week="mon-fri", hour=18, minute=30, timezone=TIMEZONE))
    sched.add_job(clear_daily_state, CronTrigger(day_of_week="mon-fri", hour=18, minute=45, timezone=TIMEZONE))
    sched.add_job(task_morning_summary, CronTrigger(day_of_week="mon-fri", hour=14, minute=15, timezone=TIMEZONE))
    sched.add_job(task_afternoon_summary, CronTrigger(day_of_week="mon-fri", hour=17, minute=55, timezone=TIMEZONE))
    sched.start()
    print("✅ Bot çalışıyor. Çakışma koruması aktif. Loglar açık.")
    while True: await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())
