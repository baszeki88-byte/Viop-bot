import json, os
import asyncio
import threading
import logging
from http.server import BaseHTTPRequestHandler, HTTPServer
from datetime import datetime, timedelta, time as dt_time
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

DATA_DIR = os.getenv("DATA_DIR", "/tmp")
if DATA_DIR == "/tmp":
    logger.warning("⚠️ DATA_DIR=/tmp: Restart/deploy sonrası açık pozisyonlar kaybolur!")
KOSAN_AT_FILE = os.path.join(DATA_DIR, "kosan_at_trades.json")
RSI_EMA_FILE = os.path.join(DATA_DIR, "rsi_ema_trades.json")

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

SYMBOLS = [
    "AKBNK", "ALARK", "ASELS", "ASTOR", "BIMAS", "DOHOL", "EKGYO", "ENKAI", "EREGL", "FROTO",
    "GARAN", "GUBRF", "HALKB", "ISCTR", "KCHOL", "KRDMD", "MGROS", "ODAS", "PETKM", "PGSUS",
    "SAHOL", "SASA", "SISE", "TAVHL", "TCELL", "THYAO", "TKFEN", "TOASO", "TRALT", "TRMET",
    "TTKOM", "TUPRS", "VAKBN", "VESTL", "YKBNK", "AEFES", "ARCLK", "CIMSA", "DOAS", "OYAKC",
    "SOKM", "ULKER", "BRSAN", "AKSEN", "ENJSA",
    "XU030"
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
TF_MINUTES = {"5m": 5, "15m": 15, "30m": 30, "1h": 60, "4h": 240, "1d": 1440}

# Koşan At
KOSAN_RSI_PERIOD = 8
KOSAN_LONG_LEVEL = 75
KOSAN_SHORT_LEVEL = 35
SL_PCT = 1.5
TP1_PCT = 2.0
TP2_PCT = 3.0
MAX_TRADE_SESSION_DAYS = 4
MAX_SIGNAL_LAG_MIN = 30

# RSI EMA Kesişimi (1H)
RSI_X_PERIOD = 8
RSI_X_EMA_SPAN = 13
RSI_X_LOW = 20
RSI_X_HIGH = 85
RSI_X_SL_WINDOW = 3 
MAX_RSI_EMA_TRADE_DAYS = 3 # YENİ: RSI EMA takip süresi (seans günü)

KOSAN_AT_TRADES = {}
RSI_EMA_TRADES = {} # YENİ: RSI EMA Açık İşlemler
KOSAN_LAST_SIGNAL = {}
rsi_ema_cross_states = {}
rsi_ema_armed_states = {} 

is_scanning = False
background_tasks = set()

DATA_CACHE = {}
CACHE_TTL = 60

EXECUTOR = ThreadPoolExecutor(max_workers=20)
FETCH_SEM = asyncio.Semaphore(4)

# ==========================================
# YARDIMCI: ZAMAN, MUM, SEANS
# ==========================================
def get_real_lag_minutes(last_bar_time, tf):
    bar_close_time = last_bar_time + timedelta(minutes=TF_MINUTES.get(tf, 0))
    now = datetime.now(TIMEZONE)
    if now < bar_close_time: return 0
    return int((now - bar_close_time).total_seconds() / 60)

def get_lag_emoji(minutes):
    if minutes <= 5: return "🟢"
    elif minutes <= 15: return "🟡"
    return "🔴"

def closed_only(df, tf):
    if df is None or df.empty: return df
    now = datetime.now(TIMEZONE)
    close_t = df.index + pd.Timedelta(minutes=TF_MINUTES[tf])
    mask = close_t <= now
    after_close = (df.index.date == now.date()) & (now.time() >= dt_time(18, 5))
    return df[mask | after_close]

def is_arife(now):
    return now.month == 10 and now.day == 28

def is_trading_day(d):
    return d.weekday() <= 4 and d not in TR_HOLIDAYS

def is_market_open(now=None):
    now = now or datetime.now(TIMEZONE)
    if not is_trading_day(now.date()): return False
    if is_arife(now): return dt_time(9, 55) <= now.time() <= dt_time(13, 0)
    return dt_time(9, 55) <= now.time() <= dt_time(18, 10)

def session_days_elapsed(entry_date, today):
    n = 0
    d = entry_date
    while d <= today:
        if is_trading_day(d): n += 1
        d += timedelta(days=1)
    return n

# ==========================================
# KAYIT / YÜKLEME (ATOMİK)
# ==========================================
def save_trades():
    try:
        # Koşan At
        out_kosan = {}
        for k, v in KOSAN_AT_TRADES.items():
            t = v.copy()
            for f in ("entry_time", "last_bar"):
                if isinstance(t.get(f), (datetime, pd.Timestamp)): t[f] = t[f].isoformat()
            out_kosan[k] = t
        with open(KOSAN_AT_FILE + ".tmp", "w", encoding="utf-8") as f:
            json.dump(out_kosan, f, ensure_ascii=False, indent=2)
        os.replace(KOSAN_AT_FILE + ".tmp", KOSAN_AT_FILE)
        
        # RSI EMA
        out_rsi = {}
        for k, v in RSI_EMA_TRADES.items():
            t = v.copy()
            if isinstance(t.get("entry_time"), (datetime, pd.Timestamp)): t["entry_time"] = t["entry_time"].isoformat()
            out_rsi[k] = t
        with open(RSI_EMA_FILE + ".tmp", "w", encoding="utf-8") as f:
            json.dump(out_rsi, f, ensure_ascii=False, indent=2)
        os.replace(RSI_EMA_FILE + ".tmp", RSI_EMA_FILE)
        
    except Exception as e:
        logger.error(f"Trade kayıt hatası: {e}")

def _parse_dt(s):
    dt = datetime.fromisoformat(s)
    return TIMEZONE.localize(dt) if dt.tzinfo is None else dt

def load_trades():
    global KOSAN_AT_TRADES, RSI_EMA_TRADES
    if os.path.exists(KOSAN_AT_FILE):
        try:
            with open(KOSAN_AT_FILE, "r", encoding="utf-8") as f:
                loaded = json.load(f)
            for k, v in loaded.items():
                for fld in ("entry_time", "last_bar"):
                    if isinstance(v.get(fld), str): v[fld] = _parse_dt(v[fld])
                if "last_bar" not in v: v["last_bar"] = v["entry_time"]
                KOSAN_AT_TRADES[k] = v
            logger.info(f"✅ {len(KOSAN_AT_TRADES)} aktif Koşan At işlemi yüklendi.")
        except Exception as e:
            logger.error(f"Koşan At yükleme hatası: {e}")
            
    if os.path.exists(RSI_EMA_FILE):
        try:
            with open(RSI_EMA_FILE, "r", encoding="utf-8") as f:
                loaded = json.load(f)
            for k, v in loaded.items():
                if isinstance(v.get("entry_time"), str): v["entry_time"] = _parse_dt(v["entry_time"])
                RSI_EMA_TRADES[k] = v
            logger.info(f"✅ {len(RSI_EMA_TRADES)} aktif RSI EMA işlemi yüklendi.")
        except Exception as e:
            logger.error(f"RSI EMA yükleme hatası: {e}")

# ==========================================
# TELEGRAM GÖNDERİM (ALINTI DESTEKLİ)
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
                if not isinstance(df.index, pd.DatetimeIndex): df.index = pd.to_datetime(df.index)
                if df.index.tz is None: df.index = df.index.tz_localize(TIMEZONE)
                else: df.index = df.index.tz_convert(TIMEZONE)
                return df
        except Exception as e:
            logger.error(f"Veri çekme hatası ({symbol} {interval}): {e}")
            time_module.sleep(2)
    return None

async def fetch_ohlc_async(symbol, interval, limit=500):
    cache_key = f"{symbol}_{interval}_{limit}"
    if cache_key in DATA_CACHE:
        cached_df, cache_time = DATA_CACHE[cache_key]
        if time_module.time() - cache_time < CACHE_TTL: return cached_df.copy()
    try:
        async with FETCH_SEM:
            await asyncio.sleep(0.3)
            loop = asyncio.get_running_loop()
            df = await asyncio.wait_for(loop.run_in_executor(EXECUTOR, fetch_ohlc, symbol, interval, limit), timeout=20.0)
            if df is not None:
                DATA_CACHE[cache_key] = (df, time_module.time())
            return df
    except asyncio.TimeoutError:
        logger.warning(f"Timeout: {symbol} {interval}"); return None
    except Exception as e:
        logger.error(f"Async Fetch Hatası ({symbol}): {e}"); return None

# ==========================================
# RSI EMA KESİŞİMİ
# ==========================================
async def check_rsi_ema_crossover(symbol, df_1h_raw):
    df = closed_only(df_1h_raw, "1h")
    if df is None or len(df) < 30: return

    rsi = get_rsi(df['close'], RSI_X_PERIOD)
    rsi_ema = rsi.ewm(span=RSI_X_EMA_SPAN, adjust=False).mean()

    curr_rsi, prev_rsi = float(rsi.iloc[-1]), float(rsi.iloc[-2])
    curr_ema, prev_ema = float(rsi_ema.iloc[-1]), float(rsi_ema.iloc[-2])
    if pd.isna(curr_rsi) or pd.isna(prev_rsi) or pd.isna(curr_ema) or pd.isna(prev_ema): return

    bar_time = df.index[-1]
    now = datetime.now(TIMEZONE)

    armed_state = rsi_ema_armed_states.get(symbol)
    if armed_state:
        if session_days_elapsed(armed_state["time"].date(), now.date()) >= 1:
            del rsi_ema_armed_states[symbol]
            armed_state = None

    if not armed_state:
        if curr_rsi < RSI_X_LOW:
            rsi_ema_armed_states[symbol] = {"state": "LONG_ARMED", "time": bar_time}
        elif curr_rsi > RSI_X_HIGH:
            rsi_ema_armed_states[symbol] = {"state": "SHORT_ARMED", "time": bar_time}
        return

    cross_key = f"rsi_ema_{symbol}_1h"
    if rsi_ema_cross_states.get(cross_key) == bar_time: return

    direction = None
    if armed_state["state"] == "LONG_ARMED" and prev_rsi <= prev_ema and curr_rsi > curr_ema:
        direction = "LONG"
    elif armed_state["state"] == "SHORT_ARMED" and prev_rsi >= prev_ema and curr_rsi < curr_ema:
        direction = "SHORT"

    if not direction: return

    lag = get_real_lag_minutes(bar_time, "1h")
    if lag > 120: return

    rsi_ema_cross_states[cross_key] = bar_time
    del rsi_ema_armed_states[symbol]

    window_bars = df.iloc[-4:-1] 
    if len(window_bars) < RSI_X_SL_WINDOW: return

    price = float(df['close'].iloc[-1])
    
    if direction == "LONG":
        stop = float(window_bars['low'].min())
        emoji = "🟢"
        stop_txt = f"Sinyal öncesi 3 mumun en düşüğü"
    else:
        stop = float(window_bars['high'].max())
        emoji = "🔴"
        stop_txt = f"Sinyal öncesi 3 mumun en yükseği"

    risk_pct = abs(price - stop) / price * 100

    # TERS SİNYAL KONTROLÜ
    if symbol in RSI_EMA_TRADES:
        existing = RSI_EMA_TRADES[symbol]
        if existing["direction"] != direction:
            del RSI_EMA_TRADES[symbol]
            await send_all(f"🔄 <b>{symbol} - Ters Sinyal!</b>\nÖnceki {existing['direction']} işlemi kapatıldı, yeni {direction} yönlü işleme geçiliyor.")
        else:
            return # Aynı yönde zaten açık işlem varsa yeni sinyal atma

    # KAYDET
    RSI_EMA_TRADES[symbol] = {
        "symbol": symbol, "direction": direction, "entry": price, "sl": stop,
        "entry_time": now, "type": "RSI EMA"
    }
    save_trades()

    msg = (f"⚡ <b>RSI EMA KESİŞİMİ ({direction}) - {symbol} [1H]</b> ⚡\n\n"
           f"RSI({RSI_X_PERIOD}) ve EMA({RSI_X_EMA_SPAN}) <b>{direction}</b> yönlü kesişti!\n"
           f"<i>(Önceki aşırı bölge: {RSI_X_LOW if direction == 'LONG' else RSI_X_HIGH} altı/üstü)</i>\n\n"
           f"📊 <b>RSI({RSI_X_PERIOD}):</b> {curr_rsi:.1f}\n"
           f"📈 <b>EMA({RSI_X_EMA_SPAN}):</b> {curr_ema:.1f}\n"
           f"💰 <b>Fiyat:</b> {price:,.2f}\n"
           f"🛑 <b>Stop:</b> {stop:,.2f} (%{risk_pct:.2f} uzaklık)\n"
           f"<i>{stop_txt}</i>\n\n"
           f"{emoji} <b>Giriş Onayı:</b> {direction} yönlü işlemler düşünülebilir.\n"
           f"⏱ <b>Veri Gecikmesi:</b> {get_lag_emoji(lag)} {lag} dk")
    await send_all(msg)

# ==========================================
# KOŞAN AT
# ==========================================
def build_levels(direction, base):
    if direction == "LONG":
        return base * (1 - SL_PCT / 100), base * (1 + TP1_PCT / 100), base * (1 + TP2_PCT / 100)
    return base * (1 + SL_PCT / 100), base * (1 - TP1_PCT / 100), base * (1 - TP2_PCT / 100)

async def track_kosan(sym, df5):
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

        if trade["status"] == "WAITING_2ND":
            n = session_days_elapsed(trade["entry_time"].date(), datetime.now(TIMEZONE).date())
            if n >= 1:
                trade["status"] = "FULLY_ENTERED"
                save_trades()
                continue

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
                trade["sl"], trade["tp1"], trade["tp2"] = build_levels(trade["direction"], trade["avg_cost"])
                trade["status"] = "FULLY_ENTERED"
                save_trades()
                msg = (f"🔄 <b>{sym} - 2. GİRİŞ TETİKLENDİ (50K)</b> 🔄\n"
                       f"Fiyat 5M EMA 50'ye ({ema:,.2f}) değdi.\n\n"
                       f"💰 <b>Yeni Ortalama Maliyet:</b> {trade['avg_cost']:,.2f}\n"
                       f"🛑 <b>Yeni Stop (SL %{SL_PCT}):</b> {trade['sl']:,.2f}\n"
                       f"🎯 <b>Yeni TP1 (%{TP1_PCT}):</b> {trade['tp1']:,.2f}\n"
                       f"🎯 <b>Yeni TP2 (%{TP2_PCT}):</b> {trade['tp2']:,.2f}")
                await send_all(msg, ids)
                continue

        tp2_hit = hi >= trade["tp2"] if long else lo <= trade["tp2"]
        if tp2_hit:
            pnl = (trade["tp2"] - trade["avg_cost"]) / trade["avg_cost"] * 100 * (1 if long else -1)
            reason = f"🎯 <b>TP2 VURULDU (TAM KÂR)</b> ({trade['tp2']:,.2f})"
            closed = True
            break

        tp1_touch = hi >= trade["tp1"] if long else lo <= trade["tp1"]
        if tp1_touch and not trade.get("tp1_hit", False):
            trade["tp1_hit"] = True
            save_trades()
            await send_all(f"✅ <b>{sym} - TP1 VURULDU (Kısmi Kâr)</b>\nFiyat {trade['tp1']:,.2f} seviyesine ulaştı. TP2 bekleniyor.", ids)

    if closed:
        msg = (f"🚨 <b>{sym} İŞLEM KAPANDI</b> 🚨\n{reason}\n"
               f"📊 <b>K/Z (ort. maliyete göre):</b> %{pnl:+.2f}\nYeni sinyaller aranıyor.")
        await send_all(msg, ids)
        del KOSAN_AT_TRADES[sym]
        save_trades()
        return

    now = datetime.now(TIMEZONE)
    n = session_days_elapsed(trade["entry_time"].date(), now.date())
    if n > MAX_TRADE_SESSION_DAYS or (n >= MAX_TRADE_SESSION_DAYS and now.time() >= dt_time(18, 10)):
        msg = (f"⏰ <b>{sym}</b> — {MAX_TRADE_SESSION_DAYS}. seans günü geçti, TP/SL oluşmadı.\n"
               f"Bot bu işlemi takipten çıkardı. <b>Kendiniz takip ediniz.</b>\n\n"
               f"📍 Ort. maliyet: {trade['avg_cost']:,.2f} | SL: {trade['sl']:,.2f} | TP2: {trade['tp2']:,.2f}")
        await send_all(msg, ids)
        del KOSAN_AT_TRADES[sym]
        save_trades()
        return

    if changed: save_trades()

async def check_kosan_at(sym, xu_trend, df_1h_raw):
    try:
        has_trade = sym in KOSAN_AT_TRADES
        df_5m = await fetch_ohlc_async(sym, "5m", limit=500 if has_trade else 150)
        df_5m = closed_only(df_5m, "5m")
        if df_5m is None or len(df_5m) < 60: return
        df_5m = df_5m.copy()
        df_5m['rsi'] = get_rsi(df_5m['close'], KOSAN_RSI_PERIOD)
        df_5m['ema50'] = df_5m['close'].ewm(span=50, adjust=False).mean()

        if has_trade:
            await track_kosan(sym, df_5m)
            return

        if df_1h_raw is None or len(df_1h_raw) < 20: return
        df_1h = df_1h_raw.copy()
        df_1h['rsi_1h'] = get_rsi(df_1h['close'], 14)

        df = pd.merge_asof(
            df_5m[['open', 'high', 'low', 'close', 'rsi', 'ema50']].sort_index(),
            df_1h[['rsi_1h']].sort_index(),
            left_index=True, right_index=True, direction='backward'
        ).dropna()
        if len(df) < 2: return

        bar_time = df.index[-1]
        if KOSAN_LAST_SIGNAL.get(sym) == bar_time: return
        lag_dk = get_real_lag_minutes(bar_time, "5m")
        if lag_dk > MAX_SIGNAL_LAG_MIN: return

        curr, prev = df.iloc[-1], df.iloc[-2]
        long_cond = (prev['rsi'] <= KOSAN_LONG_LEVEL and curr['rsi'] > KOSAN_LONG_LEVEL
                     and curr['close'] > curr['ema50'] and curr['rsi_1h'] > 50 and xu_trend == 1)
        short_cond = (prev['rsi'] >= KOSAN_SHORT_LEVEL and curr['rsi'] < KOSAN_SHORT_LEVEL
                      and curr['close'] < curr['ema50'] and curr['rsi_1h'] < 50 and xu_trend == -1)
        if not (long_cond or short_cond): return

        direction = "LONG" if long_cond else "SHORT"
        entry = float(curr['close'])
        sl, tp1, tp2 = build_levels(direction, entry)
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
               f"🎯 <b>İlk TP1 (%{TP1_PCT}):</b> {tp1:,.2f} | 🎯 <b>TP2 (%{TP2_PCT}):</b> {tp2:,.2f}\n\n"
               f"⚠️ <i>İşlem aktif. SL/TP olana veya {MAX_TRADE_SESSION_DAYS} seans günü dolana kadar yeni sinyal aranmayacak.</i>\n"
               f"⏱ <b>Veri Gecikmesi:</b> {get_lag_emoji(lag_dk)} {lag_dk} dk")
        ids = await send_all(msg)

        KOSAN_LAST_SIGNAL[sym] = bar_time
        KOSAN_AT_TRADES[sym] = {
            "symbol": sym, "direction": direction, "entry1": entry, "entry2": None,
            "avg_cost": entry, "sl": sl, "tp1": tp1, "tp2": tp2,
            "status": "WAITING_2ND", "tp1_hit": False,
            "entry_time": datetime.now(TIMEZONE), "last_bar": bar_time,
            "msg_ids": ids
        }
        save_trades()
    except Exception as e:
        logger.error(f"Koşan At hatası ({sym}): {e}")

# ==========================================
# RSI EMA İŞLEM TAKİBİ (YENİ)
# ==========================================
async def track_rsi_ema_trades():
    if not RSI_EMA_TRADES: return
    for sym, trade in list(RSI_EMA_TRADES.items()):
        try:
            # 3 Seans Günü Kontrolü
            now = datetime.now(TIMEZONE)
            n = session_days_elapsed(trade["entry_time"].date(), now.date())
            if n >= MAX_RSI_EMA_TRADE_DAYS:
                del RSI_EMA_TRADES[sym]
                save_trades()
                await send_all(f"⏰ <b>{sym} - RSI EMA Takip Süresi Doldu</b>\n{MAX_RSI_EMA_TRADE_DAYS} seans günü geçti, işlem kapatıldı.")
                continue
            
            # SL Kontrolü (5M verisi ile anlık kontrol)
            df_5m = await fetch_ohlc_async(sym, "5m", limit=100)
            if df_5m is None or df_5m.empty: continue
            
            df_after = df_5m[df_5m.index >= trade["entry_time"]]
            if df_after.empty: continue
            
            low_after = float(df_after['low'].min())
            high_after = float(df_after['high'].max())
            
            sl_hit = False
            if trade["direction"] == "LONG" and low_after <= trade["sl"]:
                sl_hit = True
            elif trade["direction"] == "SHORT" and high_after >= trade["sl"]:
                sl_hit = True
                
            if sl_hit:
                del RSI_EMA_TRADES[sym]
                save_trades()
                await send_all(f"🛑 <b>{sym} - RSI EMA STOP OLDU</b>\nFiyat {trade['sl']:,.2f} seviyesine değdi.\nİşlem kapatıldı.")
                
        except Exception as e:
            logger.error(f"RSI EMA Track hatası ({sym}): {e}")

# ==========================================
# BIST 30 ÖZETİ
# ==========================================
async def generate_market_summary(session_name):
    try:
        df_30m, df_1h = await asyncio.gather(fetch_ohlc_async("XU030", "30m", limit=500), fetch_ohlc_async("XU030", "1h", limit=500))
        if df_30m is None or df_1h is None:
            logger.warning("XU030 özeti için veri çekilemedi.")
            await send_admin_alert("⚠️ <b>Özet Hatası:</b> XU030 30m/1h verisi çekilemedi.", "ozet_api_fail")
            return

        today = datetime.now(TIMEZONE).date()
        df_today = df_30m[df_30m.index.date == today]
        if df_today.empty:
            await send_admin_alert("⚠️ <b>Özet Hatası:</b> XU030 için bugünün verisi boş geldi.", "ozet_empty_fail")
            return

        if "Sabah" in session_name:
            session_df = df_today.between_time('09:55', '14:00'); time_str = "09:55 - 14:00"
        else:
            session_df = df_today.between_time('14:01', '18:05'); time_str = "14:01 - 18:05"
        if session_df.empty:
            logger.warning(f"{session_name} özeti için seans verisi boş!")
            return

        open_price = session_df['open'].iloc[0]; close_price = session_df['close'].iloc[-1]
        high_price = session_df['high'].max(); low_price = session_df['low'].min()
        pct_change = ((close_price - open_price) / open_price) * 100
        dist_to_high = ((high_price - close_price) / high_price) * 100
        dist_to_low = ((close_price - low_price) / low_price) * 100
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
                if len(h) >= 2: return s, ((h['close'].iloc[-1] - h['close'].iloc[-2]) / h['close'].iloc[-2]) * 100
            except Exception:
                pass
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

        rsi_1h = get_rsi(df_1h['close'], 8).iloc[-1]
        ema50_30m = df_30m['close'].ewm(span=50, adjust=False).mean().iloc[-1]
        trend_status = "🟢 EMA 50 Üzerinde" if close_price > ema50_30m else "🔴 EMA 50 Altında"
        best_sector = sector_lines[0][1].split(":")[0] if sector_lines else ""
        worst_sector = sector_lines[-1][1].split(":")[0] if sector_lines else ""

        if pct_change > 0 and vol_change > 0 and green_count > red_count: yorum = f"Alıcılar baskın. Özellikle {best_sector} sektörü öne çıkıyor. Long işlemler ön planda olabilir."
        elif pct_change < 0 and vol_change > 0 and red_count > green_count: yorum = f"Satıcılar baskın. {best_sector} sektörü direniyor ama genel görünüm zayıf. Short işlemler düşünülebilir."
        elif green_count > red_count and pct_change < 0: yorum = f"Endeks düşse de hisselerin çoğu yeşil. {best_sector} öne çıkıyor. Dönüş sinyali olabilir."
        elif red_count > green_count and pct_change > 0: yorum = f"Endeks yükselse de hisselerin çoğu kırmızı. {worst_sector} zayıf. Yükseliş inandırıcı değil."
        else: yorum = "Piyasa kararsız. Yön netleşene kadar beklemede kalmak mantıklı."

        msg = (f"🏛️ <b>BIST 30 (XU030) — {session_name} Özeti</b>\n⏱ ({time_str} Arası)\n\n"
               f"📈 <b>Fiyat:</b> {close_price:,.2f} (%{pct_change:+.2f})\n📍 <b>Kapanış:</b> {close_status}\n\n"
               f"📊 <b>Hacim:</b> {vol_status}\n\n"
               f"🌡️ <b>Piyasa Genişliği:</b> {green_count} Yeşil / {red_count} Kırmızı\n\n"
               f"🧭 <b>Trend:</b> 1H RSI: {rsi_1h:.1f} | 30m EMA 50: {ema50_30m:,.2f} ({trend_status})\n\n"
               f"🏭 <b>Sektör Performansı:</b>\n{sector_text}\n\n💡 <b>Yorum:</b> {yorum}")
        await send_all(msg)
    except Exception as e:
        logger.error(f"Özet fonksiyonu hatası: {e}")

# ==========================================
# KOMUTLAR
# ==========================================
async def analyze_symbol(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id not in CHAT_IDS: return
    text = update.message.text.upper().replace("/", "").strip()
    if not text.isalnum() or len(text) < 2 or len(text) > 10: return
    if text in ["START", "HELP", "YARDIM", "TARA", "OZET", "POZLAR"]: return
    if text not in SYMBOLS:
        await update.message.reply_text(f"❌ '{text}' tarama listesinde bulunamadı."); return
    await update.message.reply_text(f"🔍 {text} analiz ediliyor, lütfen bekleyin...")
    try:
        df_30m, df_1h, df_4h, df_1d = await asyncio.gather(
            fetch_ohlc_async(text, "30m", limit=500), fetch_ohlc_async(text, "1h", limit=500),
            fetch_ohlc_async(text, "4h", limit=500), fetch_ohlc_async(text, "1d", limit=500))
        if df_1h is None or df_4h is None:
            await update.message.reply_text("❌ Veri çekilemedi."); return
        msg = f"🤖 <b>{text} — Kapsamlı Analiz</b>\n"
        price = df_1h['close'].iloc[-1]; rsi_1h = get_rsi(df_1h['close'], 8).iloc[-1]
        trend_1h = "🟢 Yukarı" if price > df_1h['close'].ewm(span=200, adjust=False).mean().iloc[-1] else "🔴 Aşağı"
        msg += f"\n📊 <b>1 Saatlik (1H)</b>\n• Fiyat: {price:,.2f}\n• RSI(8): {rsi_1h:.1f}\n• Trend: {trend_1h} (EMA 200)\n"
        rsi_4h = get_rsi(df_4h['close'], 8).iloc[-1]
        trend_4h = "🟢 Yukarı" if price > df_4h['close'].ewm(span=200, adjust=False).mean().iloc[-1] else "🔴 Aşağı"
        msg += f"\n📊 <b>4 Saatlik (4H)</b>\n• RSI(8): {rsi_4h:.1f}\n• Trend: {trend_4h} (EMA 200)\n"
        if df_1d is not None: msg += f"\n📊 <b>Günlük (1D)</b>\n• RSI(14): {get_rsi(df_1d['close'], 14).iloc[-1]:.1f}\n"
        if df_30m is not None:
            ema50_30m = df_30m['close'].ewm(span=50, adjust=False).mean().iloc[-1]
            msg += f"\n⚓ <b>30 Dakikalık (30M)</b>\n• EMA 50: {ema50_30m:,.2f}\n"
        if text in KOSAN_AT_TRADES:
            t = KOSAN_AT_TRADES[text]
            msg += (f"\n🐎 <b>Açık Koşan At ({t['direction']})</b>\n• Ort. maliyet: {t['avg_cost']:,.2f}\n"
                    f"• SL: {t['sl']:,.2f} | TP1: {t['tp1']:,.2f} | TP2: {t['tp2']:,.2f}\n")
        if text in RSI_EMA_TRADES:
            t = RSI_EMA_TRADES[text]
            msg += (f"\n⚡ <b>Açık RSI EMA ({t['direction']})</b>\n• Giriş: {t['entry']:,.2f}\n"
                    f"• SL: {t['sl']:,.2f}\n")
        await update.message.reply_text(msg, parse_mode="HTML")
    except Exception as e:
        logger.error(f"Analiz hatası: {e}"); await update.message.reply_text(f"❌ Analiz hatası: {e}")

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id not in CHAT_IDS: return
    await update.message.reply_text("✅ Sistem çalışıyor. Analiz için /ASELS gibi komut yazabilirsiniz. Manuel tarama için /tara yazın.")

async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id not in CHAT_IDS: return
    msg = ("🤖 <b>Yardım Menüsü</b>\n\n• Hisse analizi: <code>/ASELS</code> veya <code>ASELS</code>\n"
           "• Anında tarama: <code>/tara</code>\n• Piyasa özeti: <code>/ozet</code>\n"
           "• Açık pozisyonlar: <code>/pozlar</code>\n\n"
           "Sistem <b>Koşan At (Kademeli Giriş)</b> ve <b>RSI EMA Kesişimi</b> sinyallerini takip eder.")
    await update.message.reply_text(msg, parse_mode="HTML")

async def cmd_pozlar(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id not in CHAT_IDS: return
    if not KOSAN_AT_TRADES and not RSI_EMA_TRADES:
        await update.message.reply_text("📭 Açık pozisyon bulunmuyor."); return
    
    longs = []
    shorts = []
    now = datetime.now(TIMEZONE)
    
    # Koşan At
    for sym, t in KOSAN_AT_TRADES.items():
        n = session_days_elapsed(t["entry_time"].date(), now.date())
        durum = "2. giriş bek." if t["status"] == "WAITING_2ND" else "tam giriş"
        line = (f"• <b>{sym}</b> (Koşan At) - {durum} | Ort: {t['avg_cost']:,.2f}\n"
                f"  SL {t['sl']:,.2f} | TP1 {t['tp1']:,.2f} | TP2 {t['tp2']:,.2f} | Gün {n}/{MAX_TRADE_SESSION_DAYS}")
        if t["direction"] == "LONG": longs.append(line)
        else: shorts.append(line)
        
    # RSI EMA
    for sym, t in RSI_EMA_TRADES.items():
        n = session_days_elapsed(t["entry_time"].date(), now.date())
        line = (f"• <b>{sym}</b> (RSI EMA) | Giriş: {t['entry']:,.2f}\n"
                f"  SL {t['sl']:,.2f} | Gün {n}/{MAX_RSI_EMA_TRADE_DAYS}")
        if t["direction"] == "LONG": longs.append(line)
        else: shorts.append(line)

    msg = "📊 <b>AÇIK POZİSYONLAR</b>\n\n"
    if longs:
        msg += "🟢 <b>LONG:</b>\n" + "\n".join(longs) + "\n\n"
    if shorts:
        msg += "🔴 <b>SHORT:</b>\n" + "\n".join(shorts)
        
    await update.message.reply_text(msg, parse_mode="HTML")

async def cmd_tara(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id not in CHAT_IDS: return
    if is_scanning:
        await update.message.reply_text("⏳ Şu an arka planda bir tarama devam ediyor. Lütfen bitmesini bekleyin...")
        return
    await update.message.reply_text("🔄 Manuel tarama başlatıldı...")
    async def run_manual_scan():
        await tick(force=True)
        await send_all("✅ Tarama tamamlandı. Sinyal koşulu oluştuysa mesajınız gelmiştir.")
    task = asyncio.create_task(run_manual_scan()); background_tasks.add(task); task.add_done_callback(background_tasks.discard)

async def cmd_ozet(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id not in CHAT_IDS: return
    await update.message.reply_text("⏳ Piyasa özeti hazırlanıyor, lütfen bekleyin...")
    now = datetime.now(TIMEZONE)
    session_name = "Sabah" if now.time() < dt_time(14, 0) else "Öğleden Sonra"
    await generate_market_summary(session_name)

# ==========================================
# BOT BAŞLATMA
# ==========================================
bot_app = Application.builder().token(BOT_TOKEN).build()

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error(f"🚨 Telegram Hatası: {context.error}")
    if update and update.effective_chat:
        try: await context.bot.send_message(chat_id=update.effective_chat.id, text="⚠️ Beklenmeyen bir hata oluştu, lütfen tekrar deneyin.")
        except Exception: pass

bot_app.add_error_handler(error_handler)

auth_filter = filters.User(user_id=CHAT_IDS)
bot_app.add_handler(CommandHandler("start", cmd_start, filters=auth_filter))
bot_app.add_handler(CommandHandler(["help", "yardim", "durum"], cmd_help, filters=auth_filter))
bot_app.add_handler(CommandHandler("tara", cmd_tara, filters=auth_filter))
bot_app.add_handler(CommandHandler("ozet", cmd_ozet, filters=auth_filter))
bot_app.add_handler(CommandHandler("pozlar", cmd_pozlar, filters=auth_filter))
bot_app.add_handler(MessageHandler(filters.TEXT & auth_filter, analyze_symbol))

# ==========================================
# TARAMA
# ==========================================
async def tick(force=False):
    global is_scanning
    if not force and not is_market_open(): return
    if is_scanning: return
    is_scanning = True
    logger.info(f"🔄 Tarama başladı. (Zaman: {datetime.now(TIMEZONE).strftime('%H:%M')})")
    try:
        # RSI EMA Açık İşlemleri Takip Et
        await track_rsi_ema_trades()

        xu_trend = 0
        try:
            df_xu = await fetch_ohlc_async("XU030", "1h", limit=100)
            if df_xu is not None and len(df_xu) > 50:
                xu_ema50 = df_xu['close'].ewm(span=50, adjust=False).mean().iloc[-1]
                xu_trend = 1 if df_xu['close'].iloc[-1] > xu_ema50 else -1
        except Exception as e:
            logger.error(f"XU030 endeks verisi çekilemedi: {e}")

        sem = asyncio.Semaphore(10)
        async def work(sym):
            async with sem:
                try:
                    df_1h = await fetch_ohlc_async(sym, "1h", limit=150)
                    await check_rsi_ema_crossover(sym, df_1h)
                    await check_kosan_at(sym, xu_trend, df_1h)
                    return df_1h is not None
                except Exception as e:
                    logger.error(f"Tarama hatası ({sym}): {e}")
                    return False

        results = await asyncio.gather(*[work(s) for s in SYMBOLS])
        failed = results.count(False)
        if failed >= 15:
            await send_admin_alert(f"⚠️ <b>Tarama Sorunu</b>\n{failed}/{len(SYMBOLS)} hisse için veri alınamadı.", "scan_fail")
    finally:
        is_scanning = False

async def market_open_scan():
    if not is_market_open(): return
    await send_all("🟢 Seans açıldı, tarama ve takip başladı.")
    await send_all("Sistem her zaman senin için çalışıyor, kazanmak istiyorsan sadece sistemle kal... MZB 😉")
    await tick(force=True)

async def clear_daily_state():
    DATA_CACHE.clear()
    rsi_ema_cross_states.clear()

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
    sched.add_job(tick, CronTrigger(day_of_week="mon-fri", hour="10-17", minute="1-59/5", timezone=TIMEZONE))
    sched.add_job(tick, CronTrigger(day_of_week="mon-fri", hour=18, minute=10, timezone=TIMEZONE), kwargs={'force': True})

    sched.add_job(clear_daily_state, CronTrigger(day_of_week="mon-fri", hour=18, minute=30, timezone=TIMEZONE))
    sched.add_job(task_morning_summary, CronTrigger(day_of_week="mon-fri", hour=14, minute=15, timezone=TIMEZONE))
    sched.add_job(task_afternoon_summary, CronTrigger(day_of_week="mon-fri", hour=18, minute=15, timezone=TIMEZONE))

    sched.start()
    logger.info("✅ Bot çalışıyor. RSI EMA İşlem Takibi eklendi.")
    while True: await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())
