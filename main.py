import json, os
import asyncio
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from datetime import datetime, time, timedelta
from enum import Enum
import numpy as np
import io
import mplfinance as mpf
import matplotlib.pyplot as plt

import pandas as pd
import borsapy as bp
import pytz
import holidays

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
    "TOASO", "TRALT", "ASTOR", "GUBRF", "TRMET", "TCELL", "GARAN", "YKBNK", "TAVHL", "ENKAI",
    "TTKOM", "SAHOL", "SASA", "AEFES", "ASELS", "MGROS", "TUPRS", "AKBNK", "KCHOL", "KRDMD",
    "ISCTR", "THYAO", "BIMAS", "EREGL", "EKGYO", "PGSUS", "FROTO", "SISE", "VAKBN", "ALARK", 
    "AKSEN", "ARCLK", "BRSAN", "CIMSA", "DOAS", "DOHOL", "ENJSA", "OYAKC", "PETKM", "SOKM", 
    "TKFEN", "ULKER", "VESTL", "XU030"
]

RSI_PERIOD = 8
RSI_SHORT = 75
RSI_LONG = 25
EMA_PERIOD = 200
SL_MULTIPLIER = 1.5  
TIME_FRAMES = ["1h", "4h"] 

# --- KISA VADE AYARLARI (SAF SİSTEM) ---
KV_RSI_PERIOD = 14
KV_RSI_LONG = 60
KV_RSI_SHORT = 40
KV_TP1_PCT = 5
KV_SL_MULT = 3.0

# Aktif işlemleri tutacağımız sözlük
active_trades = {}
# ==========================================

def is_market_open(now=None):
    now = now or datetime.now(TIMEZONE)
    if now.weekday() > 4: return False
    tr_holidays = holidays.Turkey()
    if now.date() in tr_holidays: return False
    return time(9, 55) <= now.time() <= time(18, 0)

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
    tfs = set(TIME_FRAMES) | {"15m", "30m"}
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

def calculate_psar(df, af=0.02, max_af=0.2):
    high = df['high'].values
    low = df['low'].values
    n = len(df)
    psar = np.zeros(n)
    trend = np.zeros(n) 
    if n == 0: return df
    psar[0] = low[0]
    trend[0] = 1
    ep = high[0]
    acc = af
    for i in range(1, n):
        prev_psar = psar[i-1]
        if trend[i-1] == 1:
            psar[i] = prev_psar + acc * (ep - prev_psar)
            if i >= 2: psar[i] = min(psar[i], low[i-1], low[i-2])
            elif i >= 1: psar[i] = min(psar[i], low[i-1])
            if low[i] < psar[i]:
                trend[i] = -1; psar[i] = ep; ep = low[i]; acc = af
            else:
                trend[i] = 1
                if high[i] > ep: ep = high[i]; acc = min(acc + af, max_af)
        else:
            psar[i] = prev_psar + acc * (ep - prev_psar)
            if i >= 2: psar[i] = max(psar[i], high[i-1], high[i-2])
            elif i >= 1: psar[i] = max(psar[i], high[i-1])
            if high[i] > psar[i]:
                trend[i] = 1; psar[i] = ep; ep = high[i]; acc = af
            else:
                trend[i] = -1
                if low[i] < ep: ep = low[i]; acc = min(acc + af, max_af)
    df['psar_trend'] = trend
    return df

def create_chart_image(df, symbol, tf):
    try:
        df_chart = df.copy()
        df_chart['ema50'] = df_chart['close'].ewm(span=50, adjust=False).mean()
        df_chart['ema200'] = df_chart['close'].ewm(span=EMA_PERIOD, adjust=False).mean()
        plot_df = df_chart.tail(30)
        mc = mpf.make_marketcolors(up='g', down='r', edge='inherit', wick='inherit', volume='in')
        s  = mpf.make_mpf_style(marketcolors=mc, gridstyle='', facecolor='#1e1e1e', figcolor='#1e1e1e', rc={'axes.labelcolor': 'white', 'xtick.color': 'white', 'ytick.color': 'white'})
        addplots = [mpf.make_addplot(plot_df['ema50'], color='orange', width=1), mpf.make_addplot(plot_df['ema200'], color='blue', width=1)]
        buf = io.BytesIO()
        mpf.plot(plot_df, type='candle', style=s, title=f'\n{symbol} - {tf.upper()}', volume=True, addplot=addplots, savefig=buf)
        buf.seek(0)
        plt.close('all') 
        return buf
    except Exception as e:
        return None

class State(Enum): IDLE="IDLE"; SHORT="SHORT"; LONG="LONG"
states = {}

def process_signals(symbol, dfs):
    msgs = []
    
    # --- 1. KATMAN: SNIPER & STANDART (1H/4H) ---
    ctx_df = dfs.get("15m")
    trend_4h = get_ema_trend(dfs.get("4h"), EMA_PERIOD)
    trend_15m = get_ema_trend(ctx_df, EMA_PERIOD) if ctx_df is not None else 0
    
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
        if not atr: continue
        
        close_price = float(df["close"].iloc[-1])
        st = states.setdefault((symbol, tf), {"state": State.IDLE, "last_bar": None})
        bar_time = df.index[-1]
        if st["last_bar"] == bar_time: continue
        st["last_bar"] = bar_time
        
        if r_prev <= RSI_SHORT < r_now:
            if trend_4h == -1 and macd_now < macd_sig_now:
                sig_data = {"tf": tf, "price": close_price, "atr": atr, "df": df}
                if trend_15m == -1: sniper_signals["SHORT"].append(sig_data)
                else: standard_signals["SHORT"].append(sig_data)
            else:
                rsi_alerts["SHORT"].append({"tf": tf, "prev": r_prev, "now": r_now})
            st["state"] = State.SHORT
            
        elif r_prev >= RSI_LONG > r_now:
            if trend_4h == 1 and macd_now > macd_sig_now:
                sig_data = {"tf": tf, "price": close_price, "atr": atr, "df": df}
                if trend_15m == 1: sniper_signals["LONG"].append(sig_data)
                else: standard_signals["LONG"].append(sig_data)
            else:
                rsi_alerts["LONG"].append({"tf": tf, "prev": r_prev, "now": r_now})
            st["state"] = State.LONG

    # Sniper ve Standart Mesajları
    for direction, sig_list, title, emoji in [
        ("SHORT", sniper_signals["SHORT"], "🔥🎯 KESKİN NİŞANCI SİNYALİ", "🔴"),
        ("LONG", sniper_signals["LONG"], "🔥🎯 KESKİN NİŞANCI SİNYALİ", "🟢"),
        ("SHORT", standard_signals["SHORT"], "⚡ STANDART SİNYAL", "🔴"),
        ("LONG", standard_signals["LONG"], "⚡ STANDART SİNYAL", "🟢"),
    ]:
        if not sig_list: continue
        last_sig = sig_list[-1]
        msg = f"{title}\n{emoji} <b>{symbol}</b> — {direction} [{last_sig['tf'].upper()}]\n"
        msg += f"💰 <b>Giriş Fiyatı:</b> {last_sig['price']:,.2f}\n"
        sl_price = last_sig["price"] - (last_sig["atr"] * SL_MULTIPLIER) if direction == "LONG" else last_sig["price"] + (last_sig["atr"] * SL_MULTIPLIER)
        msg += f"🛑 <b>İlk Stop (1.5 ATR):</b> {sl_price:,.2f}\n"
        msg += f"💡 <b>Strateji:</b> %3 kârda yarısını kapat, kalanı 30m EMA50 ile takip et."
        msgs.append({"text": msg, "df": last_sig["df"], "symbol": symbol, "tf": last_sig["tf"], "direction": direction, "price": last_sig["price"], "sl": sl_price, "type": "standard"})

    # --- 2. KATMAN: 30M KISA VADE (SAF SİSTEM) ---
    df_30m = dfs.get("30m")
    if df_30m is not None and len(df_30m) > 25:
        df_30m = df_30m.copy()
        df_30m['ema13'] = df_30m['close'].ewm(span=13, adjust=False).mean()
        df_30m['ema21'] = df_30m['close'].ewm(span=21, adjust=False).mean()
        df_30m['rsi14'] = get_rsi(df_30m['close'], KV_RSI_PERIOD)
        df_30m = calculate_psar(df_30m)
        
        trend_4h_50 = get_ema_trend(dfs.get("4h"), 50)
        
        i = len(df_30m) - 1
        cross_up = (df_30m['ema13'].iloc[i-1] <= df_30m['ema21'].iloc[i-1]) and (df_30m['ema13'].iloc[i] > df_30m['ema21'].iloc[i])
        cross_down = (df_30m['ema13'].iloc[i-1] >= df_30m['ema21'].iloc[i-1]) and (df_30m['ema13'].iloc[i] < df_30m['ema21'].iloc[i])
        
        atr = calculate_atr(df_30m, 10)
        close_price = float(df_30m['close'].iloc[i])
        
        is_kv_long = cross_up and (df_30m['rsi14'].iloc[i] > KV_RSI_LONG) and (trend_4h_50 == 1) and (df_30m['psar_trend'].iloc[i] == 1)
        is_kv_short = cross_down and (df_30m['rsi14'].iloc[i] < KV_RSI_SHORT) and (trend_4h_50 == -1) and (df_30m['psar_trend'].iloc[i] == -1)
        
        if is_kv_long and atr:
            sl_price = close_price - (atr * KV_SL_MULT)
            tp_price = close_price * (1 + KV_TP1_PCT/100)
            
            msg = f"⚡ <b>30M KISA VADE SİNYALİ (LONG)</b>\n"
            msg += f"🟢 <b>{symbol}</b> — LONG [30M] ✅ PSAR & 4H EMA50 ONAYLI\n\n"
            msg += f"💰 <b>Giriş Fiyatı:</b> {close_price:,.2f}\n"
            msg += f"🛑 <b>İlk Stop:</b> {sl_price:,.2f} (3 ATR)\n"
            msg += f"🎯 <b>1. Hedef (TP1):</b> {tp_price:,.2f} (%{KV_TP1_PCT} Kâr)\n"
            msg += f"💡 <b>Çıkış:</b> TP1'de yarısını kapat, stopu maliyete çek, kalanı 30m EMA50 ile takip et."
            
            msgs.append({"text": msg, "df": df_30m, "symbol": symbol, "tf": "30m", "direction": "LONG", "price": close_price, "sl": sl_price, "tp": tp_price, "atr": atr, "type": "kisa_vade"})
            
        elif is_kv_short and atr:
            sl_price = close_price + (atr * KV_SL_MULT)
            tp_price = close_price * (1 - KV_TP1_PCT/100)
            
            msg = f"⚡ <b>30M KISA VADE SİNYALİ (SHORT)</b>\n"
            msg += f"🔴 <b>{symbol}</b> — SHORT [30M] ✅ PSAR & 4H EMA50 ONAYLI\n\n"
            msg += f"💰 <b>Giriş Fiyatı:</b> {close_price:,.2f}\n"
            msg += f"🛑 <b>İlk Stop:</b> {sl_price:,.2f} (3 ATR)\n"
            msg += f"🎯 <b>1. Hedef (TP1):</b> {tp_price:,.2f} (%{KV_TP1_PCT} Kâr)\n"
            msg += f"💡 <b>Çıkış:</b> TP1'de yarısını kapat, stopu maliyete çek, kalanı 30m EMA50 ile takip et."
            
            msgs.append({"text": msg, "df": df_30m, "symbol": symbol, "tf": "30m", "direction": "SHORT", "price": close_price, "sl": sl_price, "tp": tp_price, "atr": atr, "type": "kisa_vade"})

    # RSI Alarmları (Sadece bilgi)
    for direction, sig_list in [("SHORT", rsi_alerts["SHORT"]), ("LONG", rsi_alerts["LONG"])]:
        if not sig_list: continue
        last_sig = sig_list[-1]
        msg = f"⚠️ <b>{symbol}</b> — SADECE RSI UYARISI [{last_sig['tf'].upper()}]\n"
        msg += f"RSI 8: {last_sig['prev']:.1f} → {last_sig['now']:.1f}\n"
        msg += f"👀 <b>Grafiği kontrol edin. İşlem alacaksanız kendiniz değerlendirin.</b>"
        msgs.append({"text": msg, "df": None, "symbol": symbol, "tf": None, "type": "alert"})

    return msgs

# --- İŞLEM TAKİP SİSTEMİ (SAF SİSTEM) ---
async def check_active_trades():
    global active_trades
    if not active_trades: return
    
    for trade_key, trade in list(active_trades.items()):
        sym = trade["symbol"]
        df_30m = fetch_ohlc(sym, "30m", limit=100) 
        if df_30m is None or len(df_30m) < 50: continue
        
        df_30m['ema50'] = df_30m['close'].ewm(span=50, adjust=False).mean()
        current_price = float(df_30m['close'].iloc[-1])
        current_ema50 = float(df_30m['ema50'].iloc[-1])
        current_high = float(df_30m['high'].iloc[-1])
        current_low = float(df_30m['low'].iloc[-1])
        
        t_type = trade["type"]
        
        # --- KISA VADE (SAF SİSTEM) TAKİBİ ---
        if trade.get("signal_type") == "kisa_vade":
            tp_hit = trade.get("tp_hit", False)
            init_sl = trade["sl"]
            entry_price = trade["entry"]
            
            # 1. Stop Kontrolü (Her durumda geçerli, ilk girişe göre 3 ATR)
            if (t_type == "LONG" and current_low <= init_sl) or (t_type == "SHORT" and current_high >= init_sl):
                msg = f"🛑 <b>STOP OLDU - {sym}</b>\n"
                msg += f"İlk Stop Seviyesine ({init_sl:,.2f}) değdi. Pozisyonu kapatın."
                await bot_app.bot.send_message(chat_id=CHAT_ID, text=msg, parse_mode="HTML", reply_to_message_id=trade["msg_id"])
                del active_trades[trade_key]
                continue

            # 2. TP Kontrolü
            target_tp = trade["tp"]
            if not tp_hit:
                if (t_type == "LONG" and current_high >= target_tp) or (t_type == "SHORT" and current_low <= target_tp):
                    trade["tp_hit"] = True
                    
                    msg = f"🎯 <b>TP1 VURULDU (%5 Kâr) - {sym}</b>\n"
                    msg += f"👉 <b>Pozisyonun yarısını (%50) kapatın.</b>\n"
                    msg += f"🛑 <b>Stop-Loss'u maliyete çekin: {entry_price:,.2f}</b>\n"
                    msg += f"⏳ <i>Kalan yarıyı 30m EMA50 kırılımına kadar tutun.</i>"
                    await bot_app.bot.send_message(chat_id=CHAT_ID, text=msg, parse_mode="HTML", reply_to_message_id=trade["msg_id"])
                    continue

            # 3. EMA 50 Çıkış Kontrolü (TP vurulduysa)
            if tp_hit:
                if (t_type == "LONG" and current_price <= current_ema50) or (t_type == "SHORT" and current_price >= current_ema50):
                    msg = f"⚪ <b>EMA 50 ÇIKIŞI - {sym}</b>\n"
                    msg += f"👉 <b>Kalan pozisyonu kapatın.</b>"
                    await bot_app.bot.send_message(chat_id=CHAT_ID, text=msg, parse_mode="HTML", reply_to_message_id=trade["msg_id"])
                    del active_trades[trade_key]
        
        # --- STANDART/SNIPER TAKİBİ (ESKİ SİSTEM) ---
        else:
            # TP1 (%3) ve BE Mantığı
            if not trade.get("hit_3pct", False):
                if (t_type == "LONG" and current_high >= trade["entry"] * 1.03) or (t_type == "SHORT" and current_low <= trade["entry"] * 0.97):
                    trade["hit_3pct"] = True
                    msg = f"🎯 <b>TP1 VURULDU (3% Kâr) - {sym}</b>\n👉 <b>Yarısını kapat, stopu maliyete çek ({trade['entry']:,.2f}).</b>"
                    await bot_app.bot.send_message(chat_id=CHAT_ID, text=msg, parse_mode="HTML", reply_to_message_id=trade["msg_id"])
            else:
                if (t_type == "LONG" and current_price <= current_ema50) or (t_type == "SHORT" and current_price >= current_ema50):
                    msg = f"⚪ <b>EMA 50 ÇIKIŞI - {sym}</b>\n👉 <b>Kalan pozisyonu kapatın.</b>"
                    await bot_app.bot.send_message(chat_id=CHAT_ID, text=msg, parse_mode="HTML", reply_to_message_id=trade["msg_id"])
                    del active_trades[trade_key]


bot_app = Application.builder().token(BOT_TOKEN).build()

async def send_signal_message(msg_data):
    text = msg_data["text"]
    df = msg_data["df"]
    symbol = msg_data["symbol"]
    tf = msg_data["tf"]
    
    sent_msg = None
    if df is not None:
        buf = create_chart_image(df, symbol, tf)
        if buf:
            try:
                sent_msg = await bot_app.bot.send_photo(chat_id=CHAT_ID, photo=buf, caption=text, parse_mode="HTML")
            except Exception:
                pass
    
    if not sent_msg:
        sent_msg = await bot_app.bot.send_message(chat_id=CHAT_ID, text=text, parse_mode="HTML")
    
    if "direction" in msg_data and msg_data["type"] in ["standard", "kisa_vade"]:
        trade_key = f"{symbol}_{msg_data['direction']}_{msg_data['type']}"
        trade_info = {
            "symbol": symbol, "type": msg_data["direction"], "entry": msg_data["price"], 
            "msg_id": sent_msg.message_id, "signal_type": msg_data["type"],
            "hit_3pct": False, "tp_hit": False, "sl": msg_data.get("sl")
        }
        if msg_data["type"] == "kisa_vade":
            trade_info["tp"] = msg_data.get("tp")
            
        active_trades[trade_key] = trade_info

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE): 
    await update.message.reply_text("✅ Sistem 4 Katmanlı (Sniper, Standart, RSI, Kısa Vade Saf) çalışıyor.")

bot_app.add_handler(CommandHandler("start", cmd_start))

async def tick(force=False):
    if not force and not is_market_open(): return
    for sym in SYMBOLS:
        try:
            dfs = fetch_all_tfs(sym)
            for msg_data in process_signals(sym, dfs): 
                await send_signal_message(msg_data)
        except Exception as e: 
            pass

async def tracker_tick():
    if not is_market_open(): return
    await check_active_trades()

async def market_open_scan():
    if not is_market_open(): return
    await bot_app.bot.send_message(chat_id=CHAT_ID, text="🟢 Seans açıldı, 4 Katmanlı tarama ve takip başladı.", parse_mode="HTML")
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
    sched.add_job(tracker_tick, CronTrigger(day_of_week="mon-fri", hour="10-17", minute="15,45", timezone=TIMEZONE)) 
    sched.start()
    print("✅ Bot çalışıyor.")
    while True: await asyncio.sleep(3600)

if __name__ == "__main__": asyncio.run(main())
