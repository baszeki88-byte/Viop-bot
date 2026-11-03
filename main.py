import json, os
import asyncio
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from datetime import datetime, time, timedelta
from enum import Enum

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

import numpy as np
import io
import mplfinance as mpf

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
BOT_TOKEN = os.environ.get("BOT_TOKEN", "9293651278:AAGdGb6Nr1gBX5-Er6YAb62ZNyUQEG4yxZg")
if "BOT_TOKEN" not in os.environ:
    print("⚠️ BOT_TOKEN env'de yok — fallback kullanılıyor. Üretimde env kullanın!")

CHAT_IDS = [5066109900, 8776376951]
TIMEZONE = pytz.timezone("Europe/Istanbul")

SYMBOLS = [
    "TOASO", "TRALT", "ASTOR", "GUBRF", "TRMET", "TCELL", "GARAN", "YKBNK", "TAVHL", "ENKAI",
    "TTKOM", "SAHOL", "SASA", "AEFES", "ASELS", "MGROS", "TUPRS", "AKBNK", "KCHOL", "KRDMD",
    "ISCTR", "THYAO", "BIMAS", "EREGL", "EKGYO", "PGSUS", "FROTO", "SISE", "VAKBN", "ALARK",
    "AKSEN", "ARCLK", "BRSAN", "CIMSA", "DOAS", "DOHOL", "ENJSA", "OYAKC", "PETKM", "SOKM",
    "TKFEN", "ULKER", "VESTL", "XU030", "XU100", "XAUUSD", "XAGUSD"
]

BIST_STOCKS = [s for s in SYMBOLS if not s.startswith("XU") and not s.endswith("USD")]

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
RSI_SHORT_STD = 75
RSI_LONG_STD = 25

INSTANT_SHORT = 81
INSTANT_LONG = 25

ALARM_SHORT_UPPER = 81
ALARM_SHORT_LOWER = 65
ALARM_LONG_LOWER = 25
ALARM_LONG_UPPER = 45

EMA_PERIOD = 200
SL_MULTIPLIER = 1.5
# --- DEĞİŞTİ: 4h taramadan çıkarıldı (backtest sonucu) ---
TIME_FRAMES = ["1h"]

# --- KISA VADE ---
KV_RSI_PERIOD = 14
KV_RSI_LONG = 60
KV_RSI_SHORT = 40
KV_TP1_PCT = 5
KV_SL_MULT = 3.0
KV_EMA_FAST = 13
KV_EMA_SLOW = 34

# --- RSI BÜYÜSÜ H1 STRATEJİSİ (backtest kazananı) ---
RB_SL_ATR_MULT = 2.5
RB_TP1_PCT = 1.5
RB_TP2_PCT = 3.0
RB_TIMEOUT_BARS = 24

# --- DEĞİŞTİ: Hızlı MACD (backtest kazananı) ---
MACD_FAST = 8
MACD_SLOW = 17
MACD_SIGNAL = 9

active_trades = {}
rsi_alarm_states = {}
last_rsi_alert_msgs = {}
earnings_cache = {}
# ==========================================


def is_market_open(now=None):
    now = now or datetime.now(TIMEZONE)
    if now.weekday() > 4: return False
    tr_holidays = holidays.Turkey()
    if now.date() in tr_holidays: return False
    return time(9, 55) <= now.time() <= time(18, 0)


def get_earnings_note(symbol):
    today = datetime.now(TIMEZONE).date()
    cache_key = f"{symbol}_{today}"
    if cache_key in earnings_cache:
        return earnings_cache[cache_key]
    try:
        ticker = bp.Ticker(symbol)
        cal = ticker.calendar
        if not cal:
            earnings_cache[cache_key] = ""
            return ""
        earnings_date = cal.get('Earnings Date') if hasattr(cal, 'get') else None
        if not earnings_date:
            earnings_cache[cache_key] = ""
            return ""
        if isinstance(earnings_date, str):
            earnings_date = datetime.strptime(earnings_date, "%Y-%m-%d").date()
        elif hasattr(earnings_date, "date"):
            earnings_date = earnings_date.date()
        days_left = (earnings_date - today).days
        if 0 <= days_left <= 5:
            note = f"\n📅 <b>Bilanço Uyarısı:</b> Bilançoya {days_left} gün kaldı! (Dikkatli olun)"
            earnings_cache[cache_key] = note
            return note
    except Exception as e:
        print(f"get_earnings_note {symbol}: {e}")
    earnings_cache[cache_key] = ""
    return ""


def fetch_ohlc(symbol, interval, limit=300):
    try:
        df = bp.Ticker(symbol).history(period=f"{limit + 2} bar", interval=interval)
        if df is not None and not df.empty:
            df.columns = [str(c).lower() for c in df.columns]
            if hasattr(df.index, "tz") and df.index.tz is not None:
                try:
                    if str(df.index.tz) != "Europe/Istanbul":
                        df.index = df.index.tz_convert(TIMEZONE)
                except Exception:
                    pass
            return df.iloc[:-1]
        return None
    except Exception as e:
        print(f"fetch_ohlc {symbol} {interval}: {e}")
        return None


async def fetch_ohlc_async(symbol, interval, limit=300):
    return await asyncio.to_thread(fetch_ohlc, symbol, interval, limit)


async def fetch_all_tfs_async(symbol):
    # Trend ve nişancı onayı için 4h ve 15m hala çekiliyor
    tfs = ["15m", "30m", "1h", "4h"]
    results = await asyncio.gather(*[fetch_ohlc_async(symbol, tf) for tf in tfs])
    return dict(zip(tfs, results))


def get_rsi(series, period=8):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))


def get_macd(series):
    exp1 = series.ewm(span=MACD_FAST, adjust=False).mean()
    exp2 = series.ewm(span=MACD_SLOW, adjust=False).mean()
    macd_line = exp1 - exp2
    signal_line = macd_line.ewm(span=MACD_SIGNAL, adjust=False).mean()
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
        s = mpf.make_mpf_style(marketcolors=mc, gridstyle='', facecolor='#1e1e1e', figcolor='#1e1e1e',
                                rc={'axes.labelcolor': 'white', 'xtick.color': 'white', 'ytick.color': 'white'})
        addplots = [mpf.make_addplot(plot_df['ema50'], color='orange', width=1),
                    mpf.make_addplot(plot_df['ema200'], color='blue', width=1)]
        buf = io.BytesIO()
        mpf.plot(plot_df, type='candle', style=s, title=f'\n{symbol} - {tf.upper()}',
                 volume=True, addplot=addplots, savefig=buf)
        buf.seek(0)
        plt.close('all')
        return buf
    except Exception as e:
        print(f"create_chart_image {symbol} {tf}: {e}")
        return None


class State(Enum):
    IDLE = "IDLE"
    SHORT = "SHORT"
    LONG = "LONG"


states = {}


def process_signals(symbol, dfs):
    msgs = []
    ctx_df = dfs.get("15m")
    trend_4h = get_ema_trend(dfs.get("4h"), EMA_PERIOD)
    trend_15m = get_ema_trend(ctx_df, EMA_PERIOD) if ctx_df is not None else 0

    sniper_signals = {"LONG": [], "SHORT": []}
    standard_signals = {"LONG": [], "SHORT": []}
    rsi_alerts_instant = {"LONG": [], "SHORT": []}
    rsi_alerts_confirmed = {"LONG": [], "SHORT": []}

    for tf in TIME_FRAMES:
        df = dfs.get(tf)
        if df is None or len(df) < 30: continue
        rsi = get_rsi(df["close"], period=RSI_PERIOD)
        if rsi is None or len(rsi) < 2: continue

        macd_line, macd_sig_line = get_macd(df["close"])
        macd_now = float(macd_line.iloc[-1])
        macd_sig_now = float(macd_sig_line.iloc[-1])
        r_now, r_prev = float(rsi.iloc[-1]), float(rsi.iloc[-2])
        atr = calculate_atr(df, period=10)
        if not atr: continue

        close_price = float(df["close"].iloc[-1])

        st = states.setdefault((symbol, tf), {"state": State.IDLE, "last_bar": None})
        bar_time = df.index[-1]
        if st["last_bar"] != bar_time:
            if r_prev <= RSI_SHORT_STD < r_now:
                if trend_4h == -1 and macd_now < macd_sig_now:
                    sig_data = {"tf": tf, "price": close_price, "atr": atr, "df": df}
                    if trend_15m == -1: sniper_signals["SHORT"].append(sig_data)
                    else: standard_signals["SHORT"].append(sig_data)
                st["state"] = State.SHORT
            elif r_prev >= RSI_LONG_STD > r_now:
                if trend_4h == 1 and macd_now > macd_sig_now:
                    sig_data = {"tf": tf, "price": close_price, "atr": atr, "df": df}
                    if trend_15m == 1: sniper_signals["LONG"].append(sig_data)
                    else: standard_signals["LONG"].append(sig_data)
                st["state"] = State.LONG
            st["last_bar"] = bar_time

        if r_prev <= INSTANT_SHORT < r_now:
            rsi_alerts_instant["SHORT"].append({"tf": tf, "prev": r_prev, "now": r_now, "price": close_price})
        elif r_prev >= INSTANT_LONG > r_now:
            rsi_alerts_instant["LONG"].append({"tf": tf, "prev": r_prev, "now": r_now, "price": close_price})

        alarm_key = f"{symbol}_{tf}"
        if alarm_key not in rsi_alarm_states:
            rsi_alarm_states[alarm_key] = {"state": "IDLE", "peak_rsi": 0, "peak_price": 0,
                                           "low_rsi": 100, "low_price": 0}

        alarm_data = rsi_alarm_states[alarm_key]
        state = alarm_data["state"]

        if state == "IDLE":
            if r_now > ALARM_SHORT_UPPER:
                rsi_alarm_states[alarm_key] = {"state": "SHORT_ARMED", "peak_rsi": r_now,
                                                "peak_price": close_price, "low_rsi": 100, "low_price": 0}
            elif r_now < ALARM_LONG_LOWER:
                rsi_alarm_states[alarm_key] = {"state": "LONG_ARMED", "peak_rsi": 0,
                                                "peak_price": 0, "low_rsi": r_now, "low_price": close_price}

        elif state == "SHORT_ARMED":
            if r_now > alarm_data["peak_rsi"]:
                alarm_data["peak_rsi"] = r_now
                alarm_data["peak_price"] = close_price
            if r_now <= ALARM_SHORT_LOWER:
                price_ok = (alarm_data["peak_price"] - close_price) >= atr * 0.5
                candle_ok = close_price < float(df['open'].iloc[-1])
                macd_ok = macd_now < macd_sig_now
                if price_ok and candle_ok and macd_ok:
                    rsi_alerts_confirmed["SHORT"].append({
                        "tf": tf, "prev": r_prev, "now": r_now, "price": close_price,
                        "peak_rsi": alarm_data["peak_rsi"], "peak_price": alarm_data["peak_price"],
                        "atr": atr
                    })
                    rsi_alarm_states[alarm_key]["state"] = "IDLE"
                else:
                    alarm_data["reject_count"] = alarm_data.get("reject_count", 0) + 1
                    if alarm_data["reject_count"] >= 5:
                        rsi_alarm_states[alarm_key]["state"] = "IDLE"
            elif r_now < ALARM_LONG_LOWER:
                rsi_alarm_states[alarm_key] = {"state": "LONG_ARMED", "peak_rsi": 0,
                                                "peak_price": 0, "low_rsi": r_now, "low_price": close_price}

        elif state == "LONG_ARMED":
            if r_now < alarm_data["low_rsi"]:
                alarm_data["low_rsi"] = r_now
                alarm_data["low_price"] = close_price
            if r_now >= ALARM_LONG_UPPER:
                price_ok = (close_price - alarm_data["low_price"]) >= atr * 0.5
                candle_ok = close_price > float(df['open'].iloc[-1])
                macd_ok = macd_now > macd_sig_now
                if price_ok and candle_ok and macd_ok:
                    rsi_alerts_confirmed["LONG"].append({
                        "tf": tf, "prev": r_prev, "now": r_now, "price": close_price,
                        "low_rsi": alarm_data["low_rsi"], "low_price": alarm_data["low_price"],
                        "atr": atr
                    })
                    rsi_alarm_states[alarm_key]["state"] = "IDLE"
                else:
                    alarm_data["reject_count"] = alarm_data.get("reject_count", 0) + 1
                    if alarm_data["reject_count"] >= 5:
                        rsi_alarm_states[alarm_key]["state"] = "IDLE"
            elif r_now > ALARM_SHORT_UPPER:
                rsi_alarm_states[alarm_key] = {"state": "SHORT_ARMED", "peak_rsi": r_now,
                                                "peak_price": close_price, "low_rsi": 100, "low_price": 0}

    # --- STANDART & NİŞANCI ---
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
        msgs.append({"text": msg, "df": last_sig["df"], "symbol": symbol, "tf": last_sig["tf"],
                     "direction": direction, "price": last_sig["price"], "sl": sl_price, "type": "standard"})

    # --- KISA VADE (30m) ---
    df_30m = dfs.get("30m")
    if df_30m is not None and len(df_30m) > KV_EMA_SLOW + 10:
        df_30m = df_30m.copy()
        df_30m['ema_fast'] = df_30m['close'].ewm(span=KV_EMA_FAST, adjust=False).mean()
        df_30m['ema_slow'] = df_30m['close'].ewm(span=KV_EMA_SLOW, adjust=False).mean()
        df_30m['rsi14'] = get_rsi(df_30m['close'], KV_RSI_PERIOD)
        df_30m = calculate_psar(df_30m)

        trend_4h_50 = get_ema_trend(dfs.get("4h"), 50)
        i = len(df_30m) - 1
        cross_up = (df_30m['ema_fast'].iloc[i-1] <= df_30m['ema_slow'].iloc[i-1]) and (df_30m['ema_fast'].iloc[i] > df_30m['ema_slow'].iloc[i])
        cross_down = (df_30m['ema_fast'].iloc[i-1] >= df_30m['ema_slow'].iloc[i-1]) and (df_30m['ema_fast'].iloc[i] < df_30m['ema_slow'].iloc[i])
        atr_kv = calculate_atr(df_30m, 10)
        close_kv = float(df_30m['close'].iloc[i])

        is_kv_long = cross_up and (df_30m['rsi14'].iloc[i] > KV_RSI_LONG) and (trend_4h_50 == 1) and (df_30m['psar_trend'].iloc[i] == 1)
        is_kv_short = cross_down and (df_30m['rsi14'].iloc[i] < KV_RSI_SHORT) and (trend_4h_50 == -1) and (df_30m['psar_trend'].iloc[i] == -1)

        if is_kv_long and atr_kv:
            sl_price = close_kv - (atr_kv * KV_SL_MULT)
            tp_price = close_kv * (1 + KV_TP1_PCT/100)
            msg = f"⚡ <b>30M KISA VADE SİNYALİ (LONG)</b>\n🟢 <b>{symbol}</b> — LONG [30M] ✅ PSAR & 4H EMA50 ONAYLI\n\n💰 <b>Giriş Fiyatı:</b> {close_kv:,.2f}\n🛑 <b>İlk Stop:</b> {sl_price:,.2f} (3 ATR)\n🎯 <b>1. Hedef (TP1):</b> {tp_price:,.2f} (%{KV_TP1_PCT} Kâr)\n💡 <b>Çıkış:</b> TP1'de yarısını kapat, stopu maliyete çek, kalanı 30m EMA50 ile takip et."
            msgs.append({"text": msg, "df": df_30m, "symbol": symbol, "tf": "30m",
                         "direction": "LONG", "price": close_kv, "sl": sl_price,
                         "tp": tp_price, "atr": atr_kv, "type": "kisa_vade"})
        elif is_kv_short and atr_kv:
            sl_price = close_kv + (atr_kv * KV_SL_MULT)
            tp_price = close_kv * (1 - KV_TP1_PCT/100)
            msg = f"⚡ <b>30M KISA VADE SİNYALİ (SHORT)</b>\n🔴 <b>{symbol}</b> — SHORT [30M] ✅ PSAR & 4H EMA50 ONAYLI\n\n💰 <b>Giriş Fiyatı:</b> {close_kv:,.2f}\n🛑 <b>İlk Stop:</b> {sl_price:,.2f} (3 ATR)\n🎯 <b>1. Hedef (TP1):</b> {tp_price:,.2f} (%{KV_TP1_PCT} Kâr)\n💡 <b>Çıkış:</b> TP1'de yarısını kapat, stopu maliyete çek, kalanı 30m EMA50 ile takip et."
            msgs.append({"text": msg, "df": df_30m, "symbol": symbol, "tf": "30m",
                         "direction": "SHORT", "price": close_kv, "sl": sl_price,
                         "tp": tp_price, "atr": atr_kv, "type": "kisa_vade"})

    # --- RSI BÜYÜSÜ: İLK UYARI ---
    for direction, sig_list in [("SHORT", rsi_alerts_instant["SHORT"]), ("LONG", rsi_alerts_instant["LONG"])]:
        if not sig_list: continue
        last_sig = sig_list[-1]
        earnings_note = get_earnings_note(symbol)

        if direction == "SHORT":
            msg = f"✨ <b>RSI BÜYÜSÜ</b> ✨\n⚠️ <b>{symbol}</b> — İLK RSI UYARISI [{last_sig['tf'].upper()}]\nRSI 8: {last_sig['prev']:.1f} → {last_sig['now']:.1f} (Aşırı Alım)\n👀 <b>Grafiği kontrol edin. Teyit sinyali bekleniyor...</b>"
        else:
            msg = f"✨ <b>RSI BÜYÜSÜ</b> ✨\n⚠️ <b>{symbol}</b> — İLK RSI UYARISI [{last_sig['tf'].upper()}]\nRSI 8: {last_sig['prev']:.1f} → {last_sig['now']:.1f} (Aşırı Satım)\n👀 <b>Grafiği kontrol edin. Teyit sinyali bekleniyor...</b>"
        msg += earnings_note
        msgs.append({"text": msg, "df": None, "symbol": symbol, "tf": last_sig['tf'],
                     "type": "alert_instant", "direction": direction})

    # --- RSI BÜYÜSÜ: TEYİTLİ (H1 STRATEJİSİ) ---
    for direction, sig_list in [("SHORT", rsi_alerts_confirmed["SHORT"]), ("LONG", rsi_alerts_confirmed["LONG"])]:
        if not sig_list: continue
        last_sig = sig_list[-1]

        entry = last_sig['price']
        atr = last_sig.get('atr', 0)

        if direction == "SHORT":
            sl = entry + (atr * RB_SL_ATR_MULT)
            tp1 = entry * (1 - RB_TP1_PCT / 100)
            tp2 = entry * (1 - RB_TP2_PCT / 100)
            sl_pct = ((sl - entry) / entry) * 100

            msg = f"✨ <b>RSI BÜYÜSÜ (Teyit)</b> ✨\n🎯 <b>TEYİTLİ SİNYAL (Short) - {symbol}</b>\nRSI 8 zirveyi ({last_sig['peak_rsi']:.1f}) gördü ve {last_sig['now']:.1f} seviyesine geri çekildi.\n📈 <b>Zirve Fiyatı:</b> {last_sig['peak_price']:,.2f}\n\n"
            msg += f"💰 <b>Giriş:</b> {entry:,.2f}\n"
            msg += f"🛑 <b>SL (2.5 ATR):</b> {sl:,.2f} (%{sl_pct:.2f})\n"
            msg += f"🎯 <b>TP1 (%{RB_TP1_PCT}):</b> {tp1:,.2f} → yarısını kapat, SL'yi {entry:,.2f}'ye çek\n"
            msg += f"🎯 <b>TP2 (%{RB_TP2_PCT}):</b> {tp2:,.2f} → kalanı burada kapat\n\n"
            msg += f"⚠️ <b>ATR:</b> {atr:.2f} | <b>Risk:</b> %{sl_pct:.2f} | <b>Ödül:</b> %{RB_TP1_PCT}/%{RB_TP2_PCT}"
        else:
            sl = entry - (atr * RB_SL_ATR_MULT)
            tp1 = entry * (1 + RB_TP1_PCT / 100)
            tp2 = entry * (1 + RB_TP2_PCT / 100)
            sl_pct = ((entry - sl) / entry) * 100

            msg = f"✨ <b>RSI BÜYÜSÜ (Teyit)</b> ✨\n🎯 <b>TEYİTLİ SİNYAL (Long) - {symbol}</b>\nRSI 8 dibi ({last_sig['low_rsi']:.1f}) gördü ve {last_sig['now']:.1f} seviyesine yükseldi.\n📉 <b>Dip Fiyatı:</b> {last_sig['low_price']:,.2f}\n\n"
            msg += f"💰 <b>Giriş:</b> {entry:,.2f}\n"
            msg += f"🛑 <b>SL (2.5 ATR):</b> {sl:,.2f} (%{sl_pct:.2f})\n"
            msg += f"🎯 <b>TP1 (%{RB_TP1_PCT}):</b> {tp1:,.2f} → yarısını kapat, SL'yi {entry:,.2f}'ye çek\n"
            msg += f"🎯 <b>TP2 (%{RB_TP2_PCT}):</b> {tp2:,.2f} → kalanı burada kapat\n\n"
            msg += f"⚠️ <b>ATR:</b> {atr:.2f} | <b>Risk:</b> %{sl_pct:.2f} | <b>Ödül:</b> %{RB_TP1_PCT}/%{RB_TP2_PCT}"

        msgs.append({
            "text": msg, "df": None, "symbol": symbol, "tf": last_sig['tf'],
            "type": "rsi_buyusu", "direction": direction,
            "price": entry, "sl": sl, "tp1": tp1, "tp2": tp2, "atr": atr,
            "ref_tf": last_sig['tf']
        })

    return msgs


async def _fetch_stock_change(sym):
    def _inner():
        try:
            h = bp.Ticker(sym).history(period="5d", interval="1d")
            if len(h) >= 2:
                prev = h['Close'].iloc[-2]
                last = h['Close'].iloc[-1]
                return sym, ((last - prev) / prev) * 100
        except Exception as e:
            print(f"_fetch_stock_change {sym}: {e}")
        return sym, None
    return await asyncio.to_thread(_inner)


async def generate_market_summary(session_name):
    print(f"▶️ Piyasa özeti başlıyor: {session_name} @ {datetime.now(TIMEZONE).strftime('%H:%M:%S')}")
    try:
        df_30m, df_1h = await asyncio.gather(
            fetch_ohlc_async("XU030", "30m", limit=250),
            fetch_ohlc_async("XU030", "1h",  limit=250),
        )

        if df_30m is None or df_1h is None:
            print(f"⚠️ Özet({session_name}): XU030 verisi çekilemedi")
            return

        today = datetime.now(TIMEZONE).date()
        df_today = df_30m[df_30m.index.date == today]

        if df_today.empty:
            print(f"⚠️ Özet({session_name}): Bugün için XU030 30m verisi yok")
            return

        if "Sabah" in session_name:
            session_df = df_today.between_time('09:55', '14:00')
            time_str = "09:55 - 14:00"
        else:
            session_df = df_today.between_time('14:01', '17:54')
            time_str = "14:01 - 17:54"

        if session_df.empty:
            print(f"⚠️ Özet({session_name}): '{time_str}' aralığında veri yok")
            return

        open_price = session_df['open'].iloc[0]
        close_price = session_df['close'].iloc[-1]
        high_price = session_df['high'].max()
        low_price = session_df['low'].min()
        pct_change = ((close_price - open_price) / open_price) * 100

        dist_to_high = ((high_price - close_price) / high_price) * 100
        dist_to_low = ((close_price - low_price) / low_price) * 100

        if dist_to_high < 0.2:
            close_status = "🟢 Zirvede Kapattı (Güçlü)"
        elif dist_to_low < 0.2:
            close_status = "🔴 Dipte Kapattı (Zayıf)"
        else:
            close_status = "⚪ Orta Bölgede Kapattı"

        session_vol = session_df['volume'].sum()
        past_vols = []
        for i in range(1, 4):
            past_date = today - timedelta(days=i)
            past_df = df_30m[df_30m.index.date == past_date]
            if not past_df.empty:
                if "Sabah" in session_name:
                    p_vol = past_df.between_time('09:55', '14:00')['volume'].sum()
                else:
                    p_vol = past_df.between_time('14:01', '17:54')['volume'].sum()
                past_vols.append(p_vol)

        avg_3d_vol = sum(past_vols) / len(past_vols) if past_vols else session_vol
        vol_change = ((session_vol - avg_3d_vol) / avg_3d_vol) * 100 if avg_3d_vol > 0 else 0

        if vol_change > 10:
            vol_status = f"🟢 Son 3 günün %{vol_change:.1f} ÜZERİNDE (Hacimli)"
        elif vol_change < -10:
            vol_status = f"🔴 Son 3 günün %{abs(vol_change):.1f} ALTINDA (Sessiz)"
        else:
            vol_status = "⚪ Normal Seviyede"

        stock_changes = {}
        green_count = 0
        red_count = 0

        results = await asyncio.gather(*[_fetch_stock_change(s) for s in BIST_STOCKS])
        for sym, change in results:
            if change is None:
                continue
            stock_changes[sym] = change
            if change > 0:
                green_count += 1
            else:
                red_count += 1

        sector_lines = []
        for sector_name, stocks in SECTORS.items():
            sector_stocks = [s for s in stocks if s in stock_changes]
            if not sector_stocks:
                continue
            greens = sum(1 for s in sector_stocks if stock_changes[s] > 0)
            reds = len(sector_stocks) - greens
            avg_chg = sum(stock_changes[s] for s in sector_stocks) / len(sector_stocks)

            if greens > reds: emoji = "🟢"
            elif reds > greens: emoji = "🔴"
            else: emoji = "⚪"

            sector_lines.append((avg_chg, f"{emoji} {sector_name}: {greens}Y/{reds}K (Ort: %{avg_chg:+.2f})"))

        sector_lines.sort(key=lambda x: x[0], reverse=True)
        sector_text = "\n".join([line for _, line in sector_lines])

        rsi_1h = get_rsi(df_1h['close'], RSI_PERIOD).iloc[-1]
        ema50_30m = df_30m['close'].ewm(span=50).mean().iloc[-1]
        trend_status = "🟢 EMA 50 Üzerinde" if close_price > ema50_30m else "🔴 EMA 50 Altında"

        best_sector = sector_lines[0][1].split(":")[0] if sector_lines else ""
        worst_sector = sector_lines[-1][1].split(":")[0] if sector_lines else ""

        if pct_change > 0 and vol_change > 0 and green_count > red_count:
            yorum = f"Alıcılar baskın. Özellikle {best_sector} sektörü öne çıkıyor. Long işlemler ön planda olabilir."
        elif pct_change < 0 and vol_change > 0 and red_count > green_count:
            yorum = f"Satıcılar baskın. {best_sector} sektörü direniyor ama genel görünüm zayıf. Short işlemler düşünülebilir."
        elif green_count > red_count and pct_change < 0:
            yorum = f"Endeks düşse de hisselerin çoğu yeşil. {best_sector} öne çıkıyor. Dönüş sinyali olabilir."
        elif red_count > green_count and pct_change > 0:
            yorum = f"Endeks yükselse de hisselerin çoğu kırmızı. {worst_sector} zayıf. Yükseliş inandırıcı değil."
        else:
            yorum = "Piyasa kararsız. Yön netleşene kadar beklemede kalmak mantıklı."

        msg = f"🏛️ <b>BIST 30 (XU030) — {session_name} Özeti</b>\n"
        msg += f"⏱ ({time_str} Arası)\n\n"
        msg += f"📈 <b>Fiyat:</b> {close_price:,.2f} (%{pct_change:+.2f})\n"
        msg += f"📍 <b>Kapanış:</b> {close_status}\n\n"
        msg += f"📊 <b>Hacim:</b> {vol_status}\n\n"
        msg += f"🌡️ <b>Piyasa Genişliği:</b> {green_count} Yeşil / {red_count} Kırmızı\n\n"
        msg += f"🧭 <b>Trend:</b> 1H RSI: {rsi_1h:.1f} | 30m EMA 50: {ema50_30m:,.2f} ({trend_status})\n\n"
        msg += f"🏭 <b>Sektör Performansı:</b>\n{sector_text}\n\n"
        msg += f"💡 <b>Yorum:</b> {yorum}"

        sent_count = 0
        for cid in CHAT_IDS:
            try:
                await bot_app.bot.send_message(chat_id=cid, text=msg, parse_mode="HTML")
                sent_count += 1
            except Exception as send_err:
                print(f"Özet gönderim hatası chat {cid}: {send_err}")
        print(f"✅ Özet tamamlandı: {session_name} → {sent_count}/{len(CHAT_IDS)} gönderildi")

    except Exception as e:
        print(f"❌ Piyasa Özeti Hatası ({session_name}): {e}")
        import traceback
        traceback.print_exc()


async def analyze_symbol(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.upper().replace("/", "").strip()

    if text == "START":
        return

    if text not in SYMBOLS:
        await update.message.reply_text(f"❌ '{text}' tarama listesinde bulunamadı. Lütfen geçerli bir sembol girin (Örn: /ASELS).")
        return

    await update.message.reply_text(f"🔍 {text} analiz ediliyor, lütfen bekleyin...")

    try:
        df_30m, df_1h, df_4h, df_1d = await asyncio.gather(
            fetch_ohlc_async(text, "30m", limit=250),
            fetch_ohlc_async(text, "1h",  limit=250),
            fetch_ohlc_async(text, "4h",  limit=250),
            fetch_ohlc_async(text, "1d",  limit=250),
        )

        if df_1h is None or df_4h is None:
            await update.message.reply_text("❌ Veri çekilemedi.")
            return

        msg = f"🤖 <b>{text} — Kapsamlı Analiz</b>\n"
        price = df_1h['close'].iloc[-1]

        rsi_1h = get_rsi(df_1h['close'], RSI_PERIOD).iloc[-1]
        ema200_1h = df_1h['close'].ewm(span=200).mean().iloc[-1]
        trend_1h = "🟢 Yukarı" if price > ema200_1h else "🔴 Aşağı"

        msg += f"\n📊 <b>1 Saatlik (1H)</b>\n"
        msg += f"• Fiyat: {price:,.2f}\n"
        msg += f"• RSI(8): {rsi_1h:.1f}\n"
        msg += f"• Trend: {trend_1h} (EMA 200)\n"

        rsi_4h = get_rsi(df_4h['close'], RSI_PERIOD).iloc[-1]
        ema200_4h = df_4h['close'].ewm(span=200).mean().iloc[-1]
        trend_4h = "🟢 Yukarı" if price > ema200_4h else "🔴 Aşağı"

        msg += f"\n📊 <b>4 Saatlik (4H)</b>\n"
        msg += f"• RSI(8): {rsi_4h:.1f}\n"
        msg += f"• Trend: {trend_4h} (EMA 200)\n"

        if df_1d is not None:
            rsi_1d = get_rsi(df_1d['close'], 14).iloc[-1]
            msg += f"\n📊 <b>Günlük (1D)</b>\n"
            msg += f"• RSI(14): {rsi_1d:.1f}\n"

        ema50_30m = 0
        if df_30m is not None:
            ema50_30m = df_30m['close'].ewm(span=50).mean().iloc[-1]
            msg += f"\n⚓ <b>30 Dakikalık (30M)</b>\n"
            msg += f"• EMA 50: {ema50_30m:,.2f}\n"

        msg += f"\n💡 <b>Yorum ve Strateji:</b>\n"
        if trend_4h == "🟢 Yukarı":
            msg += f"Ana trend YUKARI. Alım yönlü işlemler düşünülebilir.\n"
            if df_30m is not None:
                msg += f"🛒 <b>Olası Alım Yeri (Destek):</b> 30m EMA 50 seviyesi olan <b>{ema50_30m:,.2f}</b> civarı takip edilebilir."
        else:
            msg += f"Ana trend AŞAĞI. Satış (Short) yönlü işlemler düşünülebilir.\n"
            if df_30m is not None:
                msg += f"🛒 <b>Olası Satım Yeri (Direnç):</b> 30m EMA 50 seviyesi olan <b>{ema50_30m:,.2f}</b> civarı takip edilebilir."

        await update.message.reply_text(msg, parse_mode="HTML")

    except Exception as e:
        await update.message.reply_text(f"❌ Analiz hatası: {e}")


async def check_active_trades():
    global active_trades
    if not active_trades: return

    for trade_key, trade in list(active_trades.items()):
        sym = trade["symbol"]
        df_30m = await fetch_ohlc_async(sym, "30m", limit=100)
        if df_30m is None or len(df_30m) < 50: continue

        df_30m['ema50'] = df_30m['close'].ewm(span=50, adjust=False).mean()
        current_price = float(df_30m['close'].iloc[-1])
        current_ema50 = float(df_30m['ema50'].iloc[-1])
        current_high = float(df_30m['high'].iloc[-1])
        current_low = float(df_30m['low'].iloc[-1])

        t_type = trade["type"]

        # === RSI BÜYÜSÜ H1 ===
        if trade.get("signal_type") == "rsi_buyusu":
            entry = trade["entry"]
            tp1 = trade["tp1"]
            tp2 = trade["tp2"]
            sl_orig = trade["sl"]
            hit_tp1 = trade.get("hit_tp1", False)

            # TP1 sonrası SL = entry (breakeven)
            current_sl = entry if hit_tp1 else sl_orig

            # SL kontrolü
            if (t_type == "LONG" and current_low <= current_sl) or (t_type == "SHORT" and current_high >= current_sl):
                if hit_tp1:
                    msg = f"🛑 <b>STOP (Breakeven) - {sym}</b>\nTP1 sonrası SL maliyete çekilmişti. Kalan pozisyon kapatıldı.\n<b>Net kâr:</b> +%{RB_TP1_PCT/2:.2f} (TP1'den)"
                else:
                    msg = f"🛑 <b>STOP OLDU - {sym}</b>\nİlk Stop ({sl_orig:,.2f}) seviyesine değdi. Pozisyon kapatıldı."
                for cid in CHAT_IDS:
                    reply_id = trade.get("msg_ids", {}).get(str(cid))
                    try: await bot_app.bot.send_message(chat_id=cid, text=msg, parse_mode="HTML", reply_to_message_id=reply_id)
                    except: await bot_app.bot.send_message(chat_id=cid, text=msg, parse_mode="HTML")
                del active_trades[trade_key]
                continue

            # TP1 kontrolü
            if not hit_tp1:
                if (t_type == "LONG" and current_high >= tp1) or (t_type == "SHORT" and current_low <= tp1):
                    trade["hit_tp1"] = True
                    msg = f"🎯 <b>TP1 VURULDU (%{RB_TP1_PCT}) - {sym}</b>\n👉 <b>Pozisyonun yarısını kapat.</b>\n🛑 <b>SL'yi maliyete çek:</b> {entry:,.2f}\n⏳ <i>Kalan yarıyı TP2 ({tp2:,.2f}) veya breakeven'a kadar tut.</i>"
                    for cid in CHAT_IDS:
                        reply_id = trade.get("msg_ids", {}).get(str(cid))
                        try: await bot_app.bot.send_message(chat_id=cid, text=msg, parse_mode="HTML", reply_to_message_id=reply_id)
                        except: await bot_app.bot.send_message(chat_id=cid, text=msg, parse_mode="HTML")
                    continue

            # TP2 kontrolü (TP1 sonrası)
            if hit_tp1:
                if (t_type == "LONG" and current_high >= tp2) or (t_type == "SHORT" and current_low <= tp2):
                    msg = f"🎯🎯 <b>TP2 VURULDU (%{RB_TP2_PCT}) - {sym}</b>\n👉 <b>Kalan pozisyonu kapat.</b>\n<b>Net kâr:</b> ~%{(RB_TP1_PCT+RB_TP2_PCT)/2:.2f} (yarı yarıya)"
                    for cid in CHAT_IDS:
                        reply_id = trade.get("msg_ids", {}).get(str(cid))
                        try: await bot_app.bot.send_message(chat_id=cid, text=msg, parse_mode="HTML", reply_to_message_id=reply_id)
                        except: await bot_app.bot.send_message(chat_id=cid, text=msg, parse_mode="HTML")
                    del active_trades[trade_key]
                    continue

        # === KISA VADE (mevcut) ===
        elif trade.get("signal_type") == "kisa_vade":
            tp_hit = trade.get("tp_hit", False)
            init_sl = trade["sl"]
            entry_price = trade["entry"]

            if (t_type == "LONG" and current_low <= init_sl) or (t_type == "SHORT" and current_high >= init_sl):
                msg = f"🛑 <b>STOP OLDU - {sym}</b>\nİlk Stop Seviyesine ({init_sl:,.2f}) değdi. Pozisyonu kapatın."
                for cid in CHAT_IDS:
                    reply_id = trade.get("msg_ids", {}).get(str(cid))
                    try: await bot_app.bot.send_message(chat_id=cid, text=msg, parse_mode="HTML", reply_to_message_id=reply_id)
                    except: await bot_app.bot.send_message(chat_id=cid, text=msg, parse_mode="HTML")
                del active_trades[trade_key]
                continue

            target_tp = trade["tp"]
            if not tp_hit:
                if (t_type == "LONG" and current_high >= target_tp) or (t_type == "SHORT" and current_low <= target_tp):
                    trade["tp_hit"] = True
                    msg = f"🎯 <b>TP1 VURULDU (%5 Kâr) - {sym}</b>\n👉 <b>Pozisyonun yarısını (%50) kapatın.</b>\n🛑 <b>Stop-Loss'u maliyete çekin: {entry_price:,.2f}</b>"
                    for cid in CHAT_IDS:
                        reply_id = trade.get("msg_ids", {}).get(str(cid))
                        try: await bot_app.bot.send_message(chat_id=cid, text=msg, parse_mode="HTML", reply_to_message_id=reply_id)
                        except: await bot_app.bot.send_message(chat_id=cid, text=msg, parse_mode="HTML")
                    continue

            if tp_hit:
                if (t_type == "LONG" and current_price <= current_ema50) or (t_type == "SHORT" and current_price >= current_ema50):
                    msg = f"⚪ <b>EMA 50 ÇIKIŞI - {sym}</b>\n👉 <b>Kalan pozisyonu kapatın.</b>"
                    for cid in CHAT_IDS:
                        reply_id = trade.get("msg_ids", {}).get(str(cid))
                        try: await bot_app.bot.send_message(chat_id=cid, text=msg, parse_mode="HTML", reply_to_message_id=reply_id)
                        except: await bot_app.bot.send_message(chat_id=cid, text=msg, parse_mode="HTML")
                    del active_trades[trade_key]

        # === STANDART ===
        else:
            if not trade.get("hit_3pct", False):
                if (t_type == "LONG" and current_high >= trade["entry"] * 1.03) or (t_type == "SHORT" and current_low <= trade["entry"] * 0.97):
                    trade["hit_3pct"] = True
                    msg = f"🎯 <b>TP1 VURULDU (3% Kâr) - {sym}</b>\n👉 <b>Yarısını kapat, stopu maliyete çek ({trade['entry']:,.2f}).</b>"
                    for cid in CHAT_IDS:
                        reply_id = trade.get("msg_ids", {}).get(str(cid))
                        try: await bot_app.bot.send_message(chat_id=cid, text=msg, parse_mode="HTML", reply_to_message_id=reply_id)
                        except: await bot_app.bot.send_message(chat_id=cid, text=msg, parse_mode="HTML")
            else:
                if (t_type == "LONG" and current_price <= current_ema50) or (t_type == "SHORT" and current_price >= current_ema50):
                    msg = f"⚪ <b>EMA 50 ÇIKIŞI - {sym}</b>\n👉 <b>Kalan pozisyonu kapatın.</b>"
                    for cid in CHAT_IDS:
                        reply_id = trade.get("msg_ids", {}).get(str(cid))
                        try: await bot_app.bot.send_message(chat_id=cid, text=msg, parse_mode="HTML", reply_to_message_id=reply_id)
                        except: await bot_app.bot.send_message(chat_id=cid, text=msg, parse_mode="HTML")
                    del active_trades[trade_key]


bot_app = Application.builder().token(BOT_TOKEN).build()


async def send_signal_message(msg_data):
    text = msg_data["text"]
    df = msg_data["df"]
    symbol = msg_data["symbol"]
    tf = msg_data.get("tf")
    msg_type = msg_data["type"]

    buf = None
    if df is not None:
        buf = create_chart_image(df, symbol, tf)

    msg_ids = {}
    for cid in CHAT_IDS:
        try:
            reply_to = None
            if msg_type in ("alert_confirmed", "rsi_buyusu"):
                key = f"{symbol}_{msg_data.get('ref_tf')}"
                if key in last_rsi_alert_msgs and str(cid) in last_rsi_alert_msgs[key]:
                    reply_to = last_rsi_alert_msgs[key][str(cid)]

            if buf:
                buf.seek(0)
                sent = await bot_app.bot.send_photo(chat_id=cid, photo=buf, caption=text, parse_mode="HTML", reply_to_message_id=reply_to)
            else:
                sent = await bot_app.bot.send_message(chat_id=cid, text=text, parse_mode="HTML", reply_to_message_id=reply_to)

            if msg_type == "alert_instant":
                key = f"{symbol}_{tf}"
                if key not in last_rsi_alert_msgs:
                    last_rsi_alert_msgs[key] = {}
                last_rsi_alert_msgs[key][str(cid)] = sent.message_id

            if "direction" in msg_data and msg_type in ["standard", "kisa_vade", "rsi_buyusu"]:
                msg_ids[str(cid)] = sent.message_id

        except Exception as e:
            print(f"send_signal_message {symbol} chat {cid}: {e}")
            try:
                if buf:
                    buf.seek(0)
                    sent = await bot_app.bot.send_photo(chat_id=cid, photo=buf, caption=text, parse_mode="HTML")
                else:
                    sent = await bot_app.bot.send_message(chat_id=cid, text=text, parse_mode="HTML")
                if "direction" in msg_data and msg_type in ["standard", "kisa_vade", "rsi_buyusu"]:
                    msg_ids[str(cid)] = sent.message_id
            except Exception as e2:
                print(f"send_signal_message fallback {symbol} chat {cid}: {e2}")

    if "direction" in msg_data and msg_type in ["standard", "kisa_vade", "rsi_buyusu"]:
        trade_key = f"{symbol}_{msg_data['direction']}_{msg_type}"
        trade_info = {
            "symbol": symbol, "type": msg_data["direction"], "entry": msg_data["price"],
            "msg_ids": msg_ids, "signal_type": msg_type,
            "hit_3pct": False, "tp_hit": False, "sl": msg_data.get("sl")
        }
        if msg_type == "kisa_vade":
            trade_info["tp"] = msg_data.get("tp")
        elif msg_type == "rsi_buyusu":
            trade_info["tp1"] = msg_data.get("tp1")
            trade_info["tp2"] = msg_data.get("tp2")
            trade_info["hit_tp1"] = False
        active_trades[trade_key] = trade_info


async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("✅ Sistem çalışıyor. Analiz için /ASELS gibi komut yazabilirsiniz.")


bot_app.add_handler(CommandHandler("start", cmd_start))
bot_app.add_handler(MessageHandler(filters.TEXT, analyze_symbol))


async def tick(force=False):
    if not force and not is_market_open(): return
    for sym in SYMBOLS:
        try:
            dfs = await fetch_all_tfs_async(sym)
            for msg_data in process_signals(sym, dfs):
                await send_signal_message(msg_data)
        except Exception as e:
            print(f"tick {sym}: {e}")


async def tracker_tick():
    if not is_market_open(): return
    await check_active_trades()


async def market_open_scan():
    if not is_market_open(): return
    for cid in CHAT_IDS:
        try: await bot_app.bot.send_message(chat_id=cid, text="🟢 Seans açıldı, tarama ve takip başladı.", parse_mode="HTML")
        except: pass
    await tick(force=True)


async def task_morning_summary():
    await generate_market_summary("Sabah")


async def task_afternoon_summary():
    await generate_market_summary("Öğleden Sonra")


async def clear_daily_state():
    last_rsi_alert_msgs.clear()
    earnings_cache.clear()
    print("🧹 Günlük state temizlendi.")


class DummyHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200); self.end_headers(); self.wfile.write(b"Alive")
    def do_HEAD(self):
        self.send_response(200); self.end_headers()
    def log_message(self, format, *args):
        return


def run_dummy_server():
    HTTPServer(("0.0.0.0", int(os.environ.get("PORT", 8080))), DummyHandler).serve_forever()


async def main():
    threading.Thread(target=run_dummy_server, daemon=True).start()
    await bot_app.initialize(); await bot_app.start(); await bot_app.updater.start_polling()
    sched = AsyncIOScheduler(timezone=TIMEZONE)

    sched.add_job(market_open_scan, CronTrigger(day_of_week="mon-fri", hour=9, minute=55, timezone=TIMEZONE))
    sched.add_job(tick, CronTrigger(day_of_week="mon-fri", hour="10-17", minute="0,30", timezone=TIMEZONE))
    sched.add_job(tracker_tick, CronTrigger(day_of_week="mon-fri", hour="10-17", minute="15,45", timezone=TIMEZONE))

    sched.add_job(task_morning_summary, CronTrigger(day_of_week="mon-fri", hour=14, minute=55, timezone=TIMEZONE))
    sched.add_job(task_afternoon_summary, CronTrigger(day_of_week="mon-fri", hour=17, minute=55, timezone=TIMEZONE))

    sched.add_job(clear_daily_state, CronTrigger(day_of_week="mon-fri", hour=18, minute=5, timezone=TIMEZONE))

    sched.start()
    print("✅ Bot çalışıyor.")
    while True: await asyncio.sleep(3600)


if __name__ == "__main__":
    asyncio.run(main())
