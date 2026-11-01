import json, os
import asyncio
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from datetime import datetime, time, timedelta
from enum import Enum
import numpy as np
import io

import pandas as pd
import borsapy as bp
import pytz
import holidays

# Grafik Kütüphaneleri
import mplfinance as mpf
import matplotlib.pyplot as plt

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

# ==========================================
# --- AYARLAR ---
# ==========================================
BOT_TOKEN = "9293651278:AAGdGb6Nr1gBX5-Er6YAb62ZNyUQEG4yxZg"
CHAT_ID = 5066109900
TIMEZONE = pytz.timezone("Europe/Istanbul")

SYMBOLS = [
    "TOASO", "TRALT", "ASTOR", "GUBRF", "TRMET",
    "TCELL", "GARAN", "YKBNK", "TAVHL", "ENKAI",
    "TTKOM", "SAHOL", "SASA", "AEFES", "ASELS",
    "MGROS", "TUPRS", "AKBNK", "KCHOL", "KRDMD",
    "ISCTR", "THYAO", "BIMAS", "EREGL", "EKGYO",
    "PGSUS", "FROTO", "SISE", "VAKBN", "ALARK", 
    "AKSEN", "ARCLK", "BRSAN", "CIMSA", "DOAS", 
    "DOHOL", "ENJSA", "OYAKC", "PETKM", "SOKM", 
    "TKFEN", "ULKER", "VESTL", "XU030"
]

RSI_PERIOD = 8
RSI_SHORT = 75
RSI_LONG = 25
EMA_PERIOD = 200
SL_MULTIPLIER = 1.5  
TP1_MULTIPLIER = 1.5 
TP2_MULTIPLIER = 2.5
TP3_MULTIPLIER = 4.0  
TIME_FRAMES = ["1h", "4h"] 
# ==========================================

def is_market_open(now=None):
    now = now or datetime.now(TIMEZONE)
    if now.weekday() > 4: return False
    tr_holidays = holidays.Turkey()
    if now.date() in tr_holidays: return False
    return time(9, 55) <= now.time() <= time(18, 0)

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
    try:
        df = bp.Ticker(symbol).history(period=f"{limit + 2} bar", interval=interval)
        if df is not None and not df.empty:
            df.columns = [str(c).lower() for c in df.columns]
            return df.iloc[:-1]
        return None
    except:
        return None

def fetch_all_tfs(symbol):
    tfs = set(TIME_FRAMES) | {"15m"}
    return {tf: fetch_ohlc(symbol, tf) for tf in tfs}

def get_rsi(series, period=8):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def get_macd(series, fast=12, slow=26, signal=9):
    exp1 = series.ewm(span=fast, adjust=False).mean()
    exp2 = series.ewm(span=slow, adjust=False).mean()
    macd_line = exp1 - exp2
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    return macd_line, signal_line

def calculate_atr(df, period=10):
    if df is None or len(df) < period + 1: return None
    tr1 = df['high'] - df['low']
    tr2 = (df['high'] - df['close'].shift()).abs()
    tr3 = (df['low'] - df['close'].shift()).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.rolling(window=period).mean()
    return float(atr.iloc[-1])

def get_ema_trend(df, period=200):
    if df is None or len(df) < period: return 0
    ema = df['close'].ewm(span=period, adjust=False).mean()
    close_price = float(df['close'].iloc[-1])
    ema_val = float(ema.iloc[-1])
    return 1 if close_price > ema_val else -1 

def check_divergence(df, rsi_series):
    if df is None or len(df) < 20: return None
    recent_df = df.tail(20).copy()
    recent_rsi = rsi_series.tail(20)
    high1_idx = recent_df['high'].iloc[:10].idxmax()
    high2_idx = recent_df['high'].iloc[10:].idxmax()
    if recent_df.loc[high2_idx, 'high'] > recent_df.loc[high1_idx, 'high']:
        if recent_rsi.loc[high2_idx] < recent_rsi.loc[high1_idx]: return "BEARISH" 
    low1_idx = recent_df['low'].iloc[:10].idxmin()
    low2_idx = recent_df['low'].iloc[10:].idxmin()
    if recent_df.loc[low2_idx, 'low'] < recent_df.loc[low1_idx, 'low']:
        if recent_rsi.loc[low2_idx] > recent_rsi.loc[low1_idx]: return "BULLISH" 
    return None

# --- YENİ: GRAFİK OLUŞTURMA FONKSİYONU ---
def create_chart_image(df, symbol, tf):
    try:
        # Son 30 mumu alacağız ama EMA 200'ü doğru hesaplamak için önce tüm veride hesaplıyoruz
        df_chart = df.copy()
        df_chart['ema50'] = df_chart['close'].ewm(span=50, adjust=False).mean()
        df_chart['ema200'] = df_chart['close'].ewm(span=EMA_PERIOD, adjust=False).mean()
        
        # Son 30 mumu kesip alıyoruz
        plot_df = df_chart.tail(30)

        # Karanlık Tema ve Mum Renkleri
        mc = mpf.make_marketcolors(up='g', down='r', edge='inherit', wick='inherit', volume='in')
        s  = mpf.make_mpf_style(marketcolors=mc, gridstyle='', facecolor='#1e1e1e', figcolor='#1e1e1e', 
                                rc={'axes.labelcolor': 'white', 'xtick.color': 'white', 'ytick.color': 'white'})

        # EMA Çizgileri
        addplots = [
            mpf.make_addplot(plot_df['ema50'], color='orange', width=1),
            mpf.make_addplot(plot_df['ema200'], color='blue', width=1)
        ]

        # Resmi hafızada oluştur (Diske yazmadan)
        buf = io.BytesIO()
        mpf.plot(plot_df, type='candle', style=s, title=f'\n{symbol} - {tf.upper()}', 
                 volume=True, addplot=addplots, savefig=buf)
        buf.seek(0)
        
        # RAM'i temizle
        plt.close('all') 
        return buf
    except Exception as e:
        print(f"Grafik Hatası ({symbol}): {e}")
        return None

class State(Enum): IDLE="IDLE"; SHORT="SHORT"; LONG="LONG"
states = {}

def process_signals(symbol, dfs):
    msgs = []
    ctx_df = dfs.get("15m")
    ctx = {"ema50": None, "ema200_15m": 0}
    if ctx_df is not None and len(ctx_df) > 200:
        ctx["ema50"] = float(ctx_df["close"].ewm(span=50, adjust=False).mean().iloc[-1])
        ctx["ema200_15m"] = get_ema_trend(ctx_df, EMA_PERIOD)

    trend_4h = get_ema_trend(dfs.get("4h"), EMA_PERIOD)
    trend_15m = ctx["ema200_15m"]
    
    sniper_signals = {"LONG": [], "SHORT": []}
    standard_signals = {"LONG": [], "SHORT": []}
    rsi_alerts = {"LONG": [], "SHORT": []}

    for tf in TIME_FRAMES:
        df = dfs.get(tf)
        if df is None or len(df) < 30: continue
        rsi = get_rsi(df["close"], period=RSI_PERIOD)
        if rsi is None or len(rsi) < 2: continue
        
        macd_line, macd_sig_line = get_macd(df["close"])
        macd_now, macd_sig_now = float(macd_line.iloc[-1]), float(macd_sig_line.iloc[-1])
        r_now, r_prev = float(rsi.iloc[-1]), float(rsi.iloc[-2])
        atr = calculate_atr(df, period=10)
        div_type = check_divergence(df, rsi)
        if not atr: continue
        
        close_price = float(df["close"].iloc[-1])
        st = states.setdefault((symbol, tf), {"state": State.IDLE, "last_bar": None})
        bar_time = df.index[-1]
        if st["last_bar"] == bar_time: continue
        st["last_bar"] = bar_time
        
        if r_prev <= RSI_SHORT < r_now:
            ema_4h_ok = (trend_4h == -1)
            macd_ok = (macd_now < macd_sig_now)
            ema_15m_ok = (trend_15m == -1)
            if ema_4h_ok and macd_ok:
                sig_data = {"tf": tf, "prev": r_prev, "now": r_now, "atr": atr, "price": close_price, "div": div_type, "df": df}
                if ema_15m_ok: sniper_signals["SHORT"].append(sig_data)
                else: standard_signals["SHORT"].append(sig_data)
            else:
                rsi_alerts["SHORT"].append({"tf": tf, "prev": r_prev, "now": r_now, "price": close_price})
            st["state"] = State.SHORT
            
        elif r_prev >= RSI_LONG > r_now:
            ema_4h_ok = (trend_4h == 1)
            macd_ok = (macd_now > macd_sig_now)
            ema_15m_ok = (trend_15m == 1)
            if ema_4h_ok and macd_ok:
                sig_data = {"tf": tf, "prev": r_prev, "now": r_now, "atr": atr, "price": close_price, "div": div_type, "df": df}
                if ema_15m_ok: sniper_signals["LONG"].append(sig_data)
                else: standard_signals["LONG"].append(sig_data)
            else:
                rsi_alerts["LONG"].append({"tf": tf, "prev": r_prev, "now": r_now, "price": close_price})
            st["state"] = State.LONG
            
        elif st["state"] == State.SHORT and r_now < RSI_SHORT:
            msgs.append(f"⚪ {symbol} SHORT_EXIT [{tf.upper()}]"); st["state"] = State.IDLE
        elif st["state"] == State.LONG and r_now > RSI_LONG:
            msgs.append(f"⚪ {symbol} LONG_EXIT [{tf.upper()}]"); st["state"] = State.IDLE

    # --- 1. KESKİN NİŞANCI SİNYALLERİ (GRAFİKLİ) ---
    for direction in ["SHORT", "LONG"]:
        sigs = sniper_signals[direction]
        if not sigs: continue
        
        tfs = [s["tf"] for s in sigs]
        tfs_sorted = sorted(tfs, key=lambda x: ["1h", "4h"].index(x))
        tf_str = " + ".join([t.upper() for t in tfs_sorted])
        last_sig = sigs[-1]
        
        header = f"🔥🎯 <b>KESKİN NİŞANCI SİNYALİ (%66 Başarı) 🎯🔥</b>\n"
        if len(tfs) >= 2: header += f"🌟👑 <b>EFSANE UYUM: {tf_str} TAM UYUM</b> 👑🌟\n"
            
        msg = header
        msg += f"{'🔴' if direction == 'SHORT' else '🟢'} <b>{symbol}</b> — {direction} [{tf_str}] ✅ MACD & 4H EMA 200 ONAYLI\n"
        msg += f"RSI: {last_sig['prev']:.1f} → {last_sig['now']:.1f}\n"
        
        if direction == "SHORT":
            sl_price = last_sig["price"] + (last_sig["atr"] * SL_MULTIPLIER)
            tp1_price = last_sig["price"] - (last_sig["atr"] * TP1_MULTIPLIER)
            tp2_price = last_sig["price"] - (last_sig["atr"] * TP2_MULTIPLIER)
            tp3_price = last_sig["price"] - (last_sig["atr"] * TP3_MULTIPLIER)
        else:
            sl_price = last_sig["price"] - (last_sig["atr"] * SL_MULTIPLIER)
            tp1_price = last_sig["price"] + (last_sig["atr"] * TP1_MULTIPLIER)
            tp2_price = last_sig["price"] + (last_sig["atr"] * TP2_MULTIPLIER)
            tp3_price = last_sig["price"] + (last_sig["atr"] * TP3_MULTIPLIER)
            
        msg += f"🛑 <b>Stop-Loss:</b> {sl_price:,.2f}\n🎯 <b>Hedef 1:</b> {tp1_price:,.2f}\n🏆 <b>Hedef 2:</b> {tp2_price:,.2f}\n🚀 <b>Hedef 3:</b> {tp3_price:,.2f}\n"
        if ctx["ema50"]: msg += f"📈 15m EMA50: {ctx['ema50']:,.2f}\n"
        msg += get_earnings_note(symbol)
        if any(s["div"] == ("BEARISH" if direction == "SHORT" else "BULLISH") for s in sigs): msg += f"\n⚠️ <b>(RSI UYUMSUZLUĞU VAR!)</b>"
            
        msgs.append({"text": msg, "df": last_sig["df"], "symbol": symbol, "tf": last_sig["tf"]})

    # --- 2. STANDART SİNYALLER (GRAFİKLİ) ---
    for direction in ["SHORT", "LONG"]:
        sigs = standard_signals[direction]
        if not sigs: continue
        
        tfs = [s["tf"] for s in sigs]
        tfs_sorted = sorted(tfs, key=lambda x: ["1h", "4h"].index(x))
        tf_str = " + ".join([t.upper() for t in tfs_sorted])
        last_sig = sigs[-1]
        
        header = f"⚡ <b>STANDART SİNYAL (%55 Başarı) ⚡</b>\n"
        if len(tfs) >= 2: header += f"🌟 <b>UYUM: {tf_str} TAM UYUM</b> 🌟\n"
            
        msg = header
        msg += f"{'🔴' if direction == 'SHORT' else '🟢'} <b>{symbol}</b> — {direction} [{tf_str}] ✅ MACD & 4H EMA 200 ONAYLI\n"
        msg += f"RSI: {last_sig['prev']:.1f} → {last_sig['now']:.1f}\n"
        
        if direction == "SHORT":
            sl_price = last_sig["price"] + (last_sig["atr"] * SL_MULTIPLIER)
            tp1_price = last_sig["price"] - (last_sig["atr"] * TP1_MULTIPLIER)
            tp2_price = last_sig["price"] - (last_sig["atr"] * TP2_MULTIPLIER)
            tp3_price = last_sig["price"] - (last_sig["atr"] * TP3_MULTIPLIER)
        else:
            sl_price = last_sig["price"] - (last_sig["atr"] * SL_MULTIPLIER)
            tp1_price = last_sig["price"] + (last_sig["atr"] * TP1_MULTIPLIER)
            tp2_price = last_sig["price"] + (last_sig["atr"] * TP2_MULTIPLIER)
            tp3_price = last_sig["price"] + (last_sig["atr"] * TP3_MULTIPLIER)
            
        msg += f"🛑 <b>Stop-Loss:</b> {sl_price:,.2f}\n🎯 <b>Hedef 1:</b> {tp1_price:,.2f}\n🏆 <b>Hedef 2:</b> {tp2_price:,.2f}\n🚀 <b>Hedef 3:</b> {tp3_price:,.2f}\n"
        if ctx["ema50"]: msg += f"📈 15m EMA50: {ctx['ema50']:,.2f}\n"
        msg += get_earnings_note(symbol)
        if any(s["div"] == ("BEARISH" if direction == "SHORT" else "BULLISH") for s in sigs): msg += f"\n⚠️ <b>(RSI UYUMSUZLUĞU VAR!)</b>"
            
        msgs.append({"text": msg, "df": last_sig["df"], "symbol": symbol, "tf": last_sig["tf"]})

    # --- 3. SADECE RSI UYARILARI (GRAFİKSİZ) ---
    for direction in ["SHORT", "LONG"]:
        sigs = rsi_alerts[direction]
        if not sigs: continue
        
        tfs = [s["tf"] for s in sigs]
        tfs_sorted = sorted(tfs, key=lambda x: ["1h", "4h"].index(x))
        tf_str = " + ".join([t.upper() for t in tfs_sorted])
        last_sig = sigs[-1]
        
        msg = f"⚠️ <b>{symbol}</b> — SADECE RSI UYARISI [{tf_str}]\n"
        msg += f"RSI 8: {last_sig['prev']:.1f} → {last_sig['now']:.1f} ({'75 üstü' if direction=='SHORT' else '25 altı'})\n"
        msg += f"👀 <b>Grafiği kontrol edin. İşlem alacaksanız kendiniz değerlendirin.</b>\n(Not: Bu sinyal MACD veya EMA 200 onayı içermez.)"
        
        msgs.append({"text": msg, "df": None, "symbol": symbol, "tf": None})

    return msgs

bot_app = Application.builder().token(BOT_TOKEN).build()

# --- YENİ: GRAFİKLİ MESAJ GÖNDERİCİ ---
async def send_signal_message(msg_data):
    text = msg_data["text"]
    df = msg_data["df"]
    symbol = msg_data["symbol"]
    tf = msg_data["tf"]
    
    if df is not None:
        buf = create_chart_image(df, symbol, tf)
        if buf:
            try:
                await bot_app.bot.send_photo(chat_id=CHAT_ID, photo=buf, caption=text, parse_mode="HTML")
                return
            except Exception as e:
                print(f"Fotoğraf gönderme hatası: {e}")
    
    # Eğer grafik yoksa veya yüklenemediyse normal mesaj at
    await bot_app.bot.send_message(chat_id=CHAT_ID, text=text, parse_mode="HTML")

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE): 
    await update.message.reply_text("✅ Sistem 3 Katmanlı ve Grafik Özellikli olarak çalışıyor.")

bot_app.add_handler(CommandHandler("start", cmd_start))

async def tick(force=False):
    if not force and not is_market_open(): return
    for sym in SYMBOLS:
        try:
            dfs = fetch_all_tfs(sym)
            for msg_data in process_signals(sym, dfs): 
                await send_signal_message(msg_data)
        except Exception as e: 
            print(f"Hata ({sym}): {e}")

async def market_open_scan():
    if not is_market_open(): return
    await bot_app.bot.send_message(chat_id=CHAT_ID, text="🟢 Seans açıldı, 3 Katmanlı tarama başladı.", parse_mode="HTML")
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
    sched.add_job(market_open_scan, CronTrigger(day_of_week="mon-fri", hour=9, minute=55, timezone=TIMEZONE))
    sched.add_job(tick, CronTrigger(day_of_week="mon-fri", hour="10-17", minute="0,30", timezone=TIMEZONE))
    sched.start()
    print("✅ Bot çalışıyor.")
    while True: await asyncio.sleep(3600)

if __name__ == "__main__": asyncio.run(main())
