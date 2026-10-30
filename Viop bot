import json, os
import asyncio
from datetime import datetime, time, timedelta
from enum import Enum

import pandas as pd
import pandas_ta as ta
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
SYMBOLS = ["F_XU0301224"]  # Vade sonu geldiğinde burayı güncelleyeceksin

RSI_PERIOD = 8
RSI_SHORT = 75
RSI_LONG = 25
ST_PERIOD = 10
ST_MULTIPLIER = 3.0
TIME_FRAMES = ["1h", "2h", "4h"]

# --- YARDIMCI FONKSİYONLAR ---
def is_market_open(now=None):
    now = now or datetime.now(TIMEZONE)
    if now.weekday() > 4: return False
    tr_holidays = holidays.Turkey()
    if now.date() in tr_holidays: return False
    t = now.time()
    return time(9, 30) <= t <= time(18, 0)

def get_earnings_note(symbol):
    try:
        ticker = bp.Ticker(symbol)
        cal = ticker.calendar
        if not cal: return ""
        earnings_date = None
        if hasattr(cal, 'get'):
            earnings_date = cal.get('Earnings Date') or cal.get('earnings_date')
        elif hasattr(cal, 'columns'):
            for col in ['Earnings Date', 'earnings_date', 'Tarih']:
                if col in cal.columns:
                    earnings_date = cal[col].iloc[0]
                    break
        if not earnings_date: return ""
        
        if isinstance(earnings_date, str):
            earnings_date = datetime.strptime(earnings_date, "%Y-%m-%d")
        
        days_left = (earnings_date.date() - datetime.now().date()).days
        if 0 <= days_left <= 7:
            return f"\n📅 <b>Bilanço</b>: {earnings_date:%d.%m.%Y} ({days_left} gün kaldı)"
    except Exception:
        pass
    return ""

def fetch_ohlc(symbol, interval, limit=300):
    ticker = bp.Ticker(symbol)
    df = ticker.history(period=f"{limit + 2} bar", interval=interval)
    return df.iloc[:-1]

def fetch_all_tfs(symbol):
    tfs = set(TIME_FRAMES) | {"15m"}
    return {tf: fetch_ohlc(symbol, tf) for tf in tfs}

def supertrend_info(df):
    st = ta.supertrend(df["high"], df["low"], df["close"], length=ST_PERIOD, multiplier=ST_MULTIPLIER)
    if st is None or len(st) < 2: return None
    line_col = [c for c in st.columns if c.startswith('SUPERT_')][0]
    dir_col = [c for c in st.columns if c.startswith('SUPERTd_')][0]
    line = st[line_col]
    direction = st[dir_col]
    return {"dir": int(direction.iloc[-1]), "line": float(line.iloc[-1])}

class State(Enum):
    IDLE = "IDLE"
    SHORT = "SHORT"
    LONG = "LONG"

states = {}

def process_signals(symbol, dfs):
    msgs = []
    ctx_df = dfs.get("15m")
    ctx = {"ema50": None, "ema200": None, "last": None}
    if ctx_df is not None and len(ctx_df) > 200:
        ctx["ema50"] = float(ta.ema(ctx_df["close"], length=50).iloc[-1])
        ctx["ema200"] = float(ta.ema(ctx_df["close"], length=200).iloc[-1])
        ctx["last"] = float(ctx_df["close"].iloc[-1])

    for tf in TIME_FRAMES:
        df = dfs.get(tf)
        if df is None or len(df) < 20: continue
        
        rsi = ta.rsi(df["close"], length=RSI_PERIOD)
        if rsi is None or len(rsi) < 2: continue
        
        r_now, r_prev = float(rsi.iloc[-1]), float(rsi.iloc[-2])
        sti = supertrend_info(df)
        st_dir = sti["dir"] if sti else 0
        
        st = states.setdefault((symbol, tf), {"state": State.IDLE, "last_bar": None})
        bar_time = df.index[-1]
        
        if st["last_bar"] == bar_time: continue
        st["last_bar"] = bar_time
        
        rsi_short = r_prev <= RSI_SHORT < r_now
        rsi_long = r_prev >= RSI_LONG > r_now
        
        if rsi_short:
            onay = "✅ ONAYLI" if st_dir == -1 else "⚠️ ZAYIF (ST çelişkili)"
            msg = f"🔴 <b>{symbol}</b> — SHORT [{tf.upper()}] {onay}\n"
            msg += f"RSI: {r_prev:.1f} → {r_now:.1f}\n"
            msg += f"🧭 SuperTrend: {'🔴 SHORT' if st_dir==-1 else '🟢 LONG'}\n"
            if sti: msg += f"Çizgi: {sti['line']:,.2f}\n"
            if ctx["ema50"]: msg += f"📈 15m EMA50: {ctx['ema50']:,.2f} | EMA200: {ctx['ema200']:,.2f}\n"
            msg += get_earnings_note(symbol)
            msgs.append(msg)
            st["state"] = State.SHORT
            
        elif rsi_long:
            onay = "✅ ONAYLI" if st_dir == 1 else "⚠️ ZAYIF (ST çelişkili)"
            msg = f"🟢 <b>{symbol}</b> — LONG [{tf.upper()}] {onay}\n"
            msg += f"RSI: {r_prev:.1f} → {r_now:.1f}\n"
            msg += f"🧭 SuperTrend: {'🟢 LONG' if st_dir==1 else '🔴 SHORT'}\n"
            if sti: msg += f"Çizgi: {sti['line']:,.2f}\n"
            if ctx["ema50"]: msg += f"📈 15m EMA50: {ctx['ema50']:,.2f} | EMA200: {ctx['ema200']:,.2f}\n"
            msg += get_earnings_note(symbol)
            msgs.append(msg)
            st["state"] = State.LONG
            
        elif st["state"] == State.SHORT and r_now < RSI_SHORT:
            msgs.append(f"⚪ <b>{symbol}</b> — SHORT_EXIT [{tf.upper()}]\nRSI {RSI_SHORT} altına döndü.")
            st["state"] = State.IDLE
            
        elif st["state"] == State.LONG and r_now > RSI_LONG:
            msgs.append(f"⚪ <b>{symbol}</b> — LONG_EXIT [{tf.upper()}]\nRSI {RSI_LONG} üstüne döndü.")
            st["state"] = State.IDLE
            
    return msgs

# --- TELEGRAM BOT KURULUMU ---
bot_app = Application.builder().token(BOT_TOKEN).build()

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("✅ Sistem çalışıyor. Sinyaller buraya düşecek.")

bot_app.add_handler(CommandHandler("start", cmd_start))

async def send_message(text):
    await bot_app.bot.send_message(chat_id=CHAT_ID, text=text, parse_mode="HTML")

async def opening_summary():
    now = datetime.now(TIMEZONE)
    lines = [f"🟢 <b>Seans açıldı</b> — {now:%d.%m.%Y %H:%M}", "━━━━━━━━━━━━━━"]
    for sym in SYMBOLS:
        try:
            dfs = fetch_all_tfs(sym)
            ctx = dfs.get("15m")
            lines.append(f"\n📊 <b>{sym}</b>")
            for tf in TIME_FRAMES:
                df = dfs.get(tf)
                if df is None or len(df) < RSI_PERIOD: continue
                rsi = ta.rsi(df["close"], length=RSI_PERIOD)
                sti = supertrend_info(df)
                r = float(rsi.iloc[-1]) if rsi is not None else 0
                st_dir = ("🟢" if sti and sti["dir"] == 1 else "🔴" if sti else "—")
                lines.append(f" {tf.upper():>3} RSI: {r:5.1f} | ST: {st_dir}")
            if ctx is not None and len(ctx) > 200:
                e50 = float(ta.ema(ctx["close"], length=50).iloc[-1])
                lines.append(f" 15m EMA50: {e50:,.2f}")
        except Exception as e:
            lines.append(f" ⚠️ {sym} hata: {e}")
    await send_message("\n".join(lines))

async def tick(force=False):
    if not force and not is_market_open(): return
    for sym in SYMBOLS:
        try:
            dfs = fetch_all_tfs(sym)
            for msg in process_signals(sym, dfs):
                await send_message(msg)
                print(f"Sinyal gönderildi: {sym}")
        except Exception as e:
            print(f"Hata ({sym}): {e}")

async def market_open_scan():
    if not is_market_open(): return
    await opening_summary()
    await tick(force=True)

# --- ÇALIŞTIRICI ---
async def main():
    print("Bot başlatılıyor, lütfen bekleyin...")
    await bot_app.initialize()
    await bot_app.start()
    await bot_app.updater.start_polling()
    
    sched = AsyncIOScheduler(timezone=TIMEZONE)
    sched.add_job(market_open_scan, CronTrigger(day_of_week="mon-fri", hour=9, minute=30, timezone=TIMEZONE))
    sched.add_job(tick, CronTrigger(day_of_week="mon-fri", hour="9-17", minute="0,30", timezone=TIMEZONE))
    sched.start()
    
    print("✅ Bot çalışıyor. Telegram'dan /start yazabilirsiniz.")
    
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())
