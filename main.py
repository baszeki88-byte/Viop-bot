import json, os
import asyncio
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from datetime import datetime, time, timedelta
from enum import Enum
import numpy as np

import pandas as pd
import borsapy as bp
import pytz
import holidays

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

# --- AYARLAR ---
BOT_TOKEN = "9293651278:AAGdGb6Nr1gBX5-Er6YAb62ZNyUQEG4yxZg"
CHAT_ID = 5066109900
TIMEZONE = pytz.timezone("Europe/Istanbul")
SYMBOLS = ["F_XU0301224"]

RSI_PERIOD = 8
RSI_SHORT = 75
RSI_LONG = 25
ST_PERIOD = 10
ST_MULTIPLIER = 3.0
TIME_FRAMES = ["1h", "2h", "4h"]

def is_market_open(now=None):
    now = now or datetime.now(TIMEZONE)
    if now.weekday() > 4: return False
    tr_holidays = holidays.Turkey()
    if now.date() in tr_holidays: return False
    return time(9, 30) <= now.time() <= time(18, 0)

def get_earnings_note(symbol):
    try:
        ticker = bp.Ticker(symbol)
        cal = ticker.calendar
        if not cal: return ""
        earnings_date = cal.get('Earnings Date') if hasattr(cal, 'get') else None
        if not earnings_date: return ""
        if isinstance(earnings_date, str):
            earnings_date = datetime.strptime(earnings_date, "%Y-%m-%d")
        days_left = (earnings_date.date() - datetime.now().date()).days
        if 0 <= days_left <= 7:
            return f"\n📅 <b>Bilanço</b>: {earnings_date:%d.%m.%Y} ({days_left} gün kaldı)"
    except Exception: pass
    return ""

def fetch_ohlc(symbol, interval, limit=300):
    df = bp.Ticker(symbol).history(period=f"{limit + 2} bar", interval=interval)
    return df.iloc[:-1]

def fetch_all_tfs(symbol):
    return {tf: fetch_ohlc(symbol, tf) for tf in set(TIME_FRAMES) | {"15m"}}

# --- MANUEL İNDİKATÖRLER ---
def get_rsi(series, period=8):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def get_supertrend(df, period=10, multiplier=3.0):
    if len(df) < period + 1: return None
    hl2 = (df['high'] + df['low']) / 2
    tr1 = df['high'] - df['low']
    tr2 = (df['high'] - df['close'].shift()).abs()
    tr3 = (df['low'] - df['close'].shift()).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.rolling(window=period).mean()
    
    upperband = hl2 + (multiplier * atr)
    lowerband = hl2 - (multiplier * atr)
    
    supertrend = pd.Series(np.nan, index=df.index, dtype=float)
    direction = pd.Series(1, index=df.index, dtype=int)
    
    for i in range(period, len(df)):
        if df['close'].iloc[i] > upperband.iloc[i-1]:
            direction.iloc[i] = 1
        elif df['close'].iloc[i] < lowerband.iloc[i-1]:
            direction.iloc[i] = -1
        else:
            direction.iloc[i] = direction.iloc[i-1]
            if direction.iloc[i] == 1 and lowerband.iloc[i] < lowerband.iloc[i-1]:
                lowerband.iloc[i] = lowerband.iloc[i-1]
            if direction.iloc[i] == -1 and upperband.iloc[i] > upperband.iloc[i-1]:
                upperband.iloc[i] = upperband.iloc[i-1]
        
        if direction.iloc[i] == 1:
            supertrend.iloc[i] = lowerband.iloc[i]
        else:
            supertrend.iloc[i] = upperband.iloc[i]
            
    return {"dir": int(direction.iloc[-1]), "line": float(supertrend.iloc[-1])}

class State(Enum): IDLE="IDLE"; SHORT="SHORT"; LONG="LONG"
states = {}

def process_signals(symbol, dfs):
    msgs = []
    ctx_df = dfs.get("15m")
    ctx = {"ema50": None}
    if ctx_df is not None and len(ctx_df) > 50:
        ctx["ema50"] = float(ctx_df["close"].ewm(span=50, adjust=False).mean().iloc[-1])

    for tf in TIME_FRAMES:
        df = dfs.get(tf)
        if df is None or len(df) < 20: continue
        rsi = get_rsi(df["close"], period=RSI_PERIOD)
        if rsi is None or len(rsi) < 2: continue
        
        r_now, r_prev = float(rsi.iloc[-1]), float(rsi.iloc[-2])
        sti = get_supertrend(df, period=ST_PERIOD, multiplier=ST_MULTIPLIER)
        st_dir = sti["dir"] if sti else 0
        st = states.setdefault((symbol, tf), {"state": State.IDLE, "last_bar": None})
        bar_time = df.index[-1]
        
        if st["last_bar"] == bar_time: continue
        st["last_bar"] = bar_time
        
        if r_prev <= RSI_SHORT < r_now:
            onay = "✅ ONAYLI" if st_dir == -1 else "⚠️ ZAYIF"
            msg = f"🔴 <b>{symbol}</b> — SHORT [{tf.upper()}] {onay}\nRSI: {r_prev:.1f} → {r_now:.1f}\n"
            if sti: msg += f"🧭 ST Çizgi: {sti['line']:,.2f}\n"
            if ctx["ema50"]: msg += f"📈 EMA50: {ctx['ema50']:,.2f}\n"
            msg += get_earnings_note(symbol)
            msgs.append(msg); st["state"] = State.SHORT
        elif r_prev >= RSI_LONG > r_now:
            onay = "✅ ONAYLI" if st_dir == 1 else "⚠️ ZAYIF"
            msg = f"🟢 <b>{symbol}</b> — LONG [{tf.upper()}] {onay}\nRSI: {r_prev:.1f} → {r_now:.1f}\n"
            if sti: msg += f"🧭 ST Çizgi: {sti['line']:,.2f}\n"
            if ctx["ema50"]: msg += f"📈 EMA50: {ctx['ema50']:,.2f}\n"
            msg += get_earnings_note(symbol)
            msgs.append(msg); st["state"] = State.LONG
        elif st["state"] == State.SHORT and r_now < RSI_SHORT:
            msgs.append(f"⚪ {symbol} SHORT_EXIT [{tf.upper()}]"); st["state"] = State.IDLE
        elif st["state"] == State.LONG and r_now > RSI_LONG:
            msgs.append(f"⚪ {symbol} LONG_EXIT [{tf.upper()}]"); st["state"] = State.IDLE
    return msgs

bot_app = Application.builder().token(BOT_TOKEN).build()

async def send_message(text): await bot_app.bot.send_message(chat_id=CHAT_ID, text=text, parse_mode="HTML")
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE): await update.message.reply_text("✅ Sistem çalışıyor.")
bot_app.add_handler(CommandHandler("start", cmd_start))

async def tick(force=False):
    if not force and not is_market_open(): return
    for sym in SYMBOLS:
        try:
            for msg in process_signals(sym, fetch_all_tfs(sym)): await send_message(msg)
        except Exception as e: print(f"Hata: {e}")

async def market_open_scan():
    if not is_market_open(): return
    await send_message("🟢 Seans açıldı, tarama başladı.")
    await tick(force=True)

class DummyHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200); self.end_headers(); self.wfile.write(b"Alive")
def run_dummy_server():
    HTTPServer(("0.0.0.0", int(os.environ.get("PORT", 8080))), DummyHandler).serve_forever()

async def main():
    threading.Thread(target=run_dummy_server, daemon=True).start()
    await bot_app.initialize(); await bot_app.start(); await bot_app.updater.start_polling()
    sched = AsyncIOScheduler(timezone=TIMEZONE)
    sched.add_job(market_open_scan, CronTrigger(day_of_week="mon-fri", hour=9, minute=30, timezone=TIMEZONE))
    sched.add_job(tick, CronTrigger(day_of_week="mon-fri", hour="9-17", minute="0,30", timezone=TIMEZONE))
    sched.start()
    print("✅ Bot çalışıyor.")
    while True: await asyncio.sleep(3600)

if __name__ == "__main__": asyncio.run(main())
