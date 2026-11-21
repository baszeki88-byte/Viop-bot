# ==============================================================================
# KOŞAN AT — NİHAİ CANLI SİNYAL & POZİSYON TAKİP ASİSTANI (BIST + GLD + XU030)
# Render Web Service Uyumlu Tam Kod (main.py)
# ==============================================================================

import os
import json
import time
import threading
import logging
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from datetime import datetime, time as dt_time
import numpy as np
import pandas as pd
import pytz
import requests
import borsapy as bp

try:
    import yfinance as yf
    HAS_YF = True
except ImportError:
    HAS_YF = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("KosanAtNihaiAsistan")

TZ = pytz.timezone("Europe/Istanbul")
STATE_FILE = "kosan_at_nihai_state.json"

# --- ORTAM DEĞİŞKENLERİ ---
BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
CHAT_IDS_STR = os.environ.get("CHAT_IDS", "5066109900,8776376951,8634151585")

try:
    CHAT_IDS = [int(cid.strip()) for cid in CHAT_IDS_STR.split(",") if cid.strip()]
except ValueError:
    CHAT_IDS = [5066109900, 8776376951, 8634151585]

WEB_PORT = int(os.environ.get("PORT", "10000"))

TV_SESSION = os.environ.get("TV_SESSION", "").strip()
TV_SESSION_SIGN = os.environ.get("TV_SESSION_SIGN", "").strip()
if TV_SESSION and TV_SESSION_SIGN:
    try:
        bp.set_tradingview_auth(session=TV_SESSION, session_sign=TV_SESSION_SIGN)
        logger.info("TradingView auth aktif.")
    except Exception as e:
        logger.warning(f"TradingView auth uyarısı: {e}")

BIST_SYMBOLS = [
    "AKBNK", "ALARK", "ASELS", "ASTOR", "BIMAS", "DOHOL", "EKGYO", "ENKAI", "EREGL", "FROTO",
    "GARAN", "GUBRF", "HALKB", "ISCTR", "KCHOL", "KRDMD", "MGROS", "ODAS", "PETKM", "PGSUS",
    "SAHOL", "SASA", "SISE", "TAVHL", "TCELL", "THYAO", "TKFEN", "TOASO", "TRALT", "TRMET",
    "TTKOM", "TUPRS", "VAKBN", "VESTL", "YKBNK", "AEFES", "ARCLK", "CIMSA", "DOAS", "OYAKC",
    "SOKM", "ULKER", "BRSAN", "AKSEN", "ENJSA"
]
INDEX_SYMBOL = "XU030"
GOLD_SYMBOL = "GLD"

WHITE_LIST = {
    "EKGYO", "MGROS", "TRALT", "THYAO", "TRMET", "PGSUS", "KRDMD", "ODAS",
    "ALARK", "VESTL", "AKSEN", "TOASO", "ARCLK", "TTKOM", "SISE", "FROTO",
    "TKFEN", "HALKB", "GUBRF", "ASELS", "ASTOR", "PETKM", "KCHOL", "ISCTR"
}

# Zaman Pencereleri
MARKET_OPEN_MSG_TIME = dt_time(9, 50)
EARLY_WINDOW_START = dt_time(10, 0)
EARLY_WINDOW_END = dt_time(11, 0)
PRIME_START = dt_time(11, 0)
PRIME_END = dt_time(17, 0)
NO_NEW_ENTRY_AFTER = dt_time(17, 0)

DAILY_SUMMARY_TIME = dt_time(18, 15)
WEEKLY_SUMMARY_TIME = dt_time(18, 25)
WEEKLY_CLEANUP_TIME = dt_time(18, 30)

# Strateji & Portföy Parametreleri
KOSAN_RSI_PERIOD = 8
KOSAN_LONG_LEVEL = 75
KOSAN_SHORT_LEVEL = 35

SL_PCT = 1.5
WHITE_BASE_TP_PCT = 2.5
WHITE_ZB_TP_PCT = 4.0
RED_BASE_TP_PCT = 1.5
RED_ZB_TP_PCT = 3.0

WHITE_PRIME_ALLOC = 100_000.0
WHITE_EARLY_ALLOC = 50_000.0
WHITE_OFF_ALLOC = 75_000.0
WHITE_ADD_ALLOC = 50_000.0

RED_PRIME_ALLOC = 50_000.0
RED_EARLY_ALLOC = 25_000.0
RED_OFF_ALLOC = 30_000.0

ZB_LOOKBACK = 18
ZB_ORDER = 2

LATEST_PRICES: dict[str, float] = {}
STATE_LOCK = threading.Lock()

# Çoklu iş parçacığı (Thread) kontrolleri
scan_event = threading.Event()
manual_notify_queue = []

def default_state() -> dict:
    return {
        "positions": {},
        "last_seen_h1_pos": {},
        "daily_stats": {
            "date": "", "kosan_signals": 0, "tp_4": 0, "tp_3": 0,
            "tp_2_5": 0, "tp_1_5": 0, "sl": 0, "zb_exit": 0,
        },
        "weekly_stats": {
            "week_key": "", "kosan_signals": 0, "tp_4": 0, "tp_3": 0,
            "tp_2_5": 0, "tp_1_5": 0, "sl": 0, "zb_exit": 0,
        },
        "sent_flags": {
            "date": "", "open_msg": False, "prime_active": False,
            "daily_summary": False, "weekly_summary": False, "weekly_cleanup": False,
        },
        "tg_offset": 0,
    }

def load_state() -> dict:
    if not os.path.exists(STATE_FILE):
        return default_state()
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        base = default_state()
        for k, v in base.items():
            if k not in data:
                data[k] = v
            elif isinstance(v, dict):
                merged = v.copy()
                merged.update(data[k])
                data[k] = merged
        return data
    except Exception as e:
        logger.error(f"State okuma hatası: {e}")
        return default_state()

def save_state(state: dict) -> None:
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"State yazma hatası: {e}")

def send_tg(text: str, reply_ids: dict = None, target_chat_id: int = None) -> dict:
    if not BOT_TOKEN:
        logger.info(f"[TELEGRAM LOG - TOKEN YOK]\n{text}")
        return {}

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    sent_ids = {}
    
    # Hedef belirtilmişse sadece o kişiye, yoksa herkese gönder
    targets = [target_chat_id] if target_chat_id else CHAT_IDS

    for cid in targets:
        kw = {}
        if reply_ids and str(cid) in reply_ids:
            kw = {"reply_to_message_id": reply_ids[str(cid)], "allow_sending_without_reply": True}
            
        payload = {"chat_id": cid, "text": text, "parse_mode": "HTML", **kw}
        try:
            resp = requests.post(url, json=payload, timeout=10)
            if resp.ok:
                sent_ids[str(cid)] = resp.json().get("result", {}).get("message_id")
            else:
                logger.warning(f"Telegram gönderim hatası ({cid}): {resp.text}")
        except Exception as e:
            logger.error(f"Telegram bağlantı hatası ({cid}): {e}")
            
    return sent_ids

def get_rsi(series: pd.Series, period: int = 8) -> pd.Series:
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))

def check_zeki_bey_series(rsi_series: pd.Series, idx: int, direction: str, lookback: int = 18, order: int = 2) -> bool:
    min_needed = max(10, order * 2 + 3)
    if idx < min_needed:
        return False
    start_i = max(0, idx - (lookback - 1))
    vals = rsi_series.iloc[start_i:idx + 1].dropna().values
    if len(vals) < min_needed:
        return False

    curr_val = float(vals[-1])
    prev_val = float(vals[-2])

    def find_peaks(data, ord_n):
        peaks = []
        for i in range(ord_n, len(data) - ord_n):
            if data[i] == max(data[i - ord_n:i + ord_n + 1]):
                peaks.append(i)
        return peaks

    def find_troughs(data, ord_n):
        troughs = []
        for i in range(ord_n, len(data) - ord_n):
            if data[i] == min(data[i - ord_n:i + ord_n + 1]):
                troughs.append(i)
        return troughs

    if direction == "LONG":
        peaks = find_peaks(vals, order)
        if len(peaks) >= 2:
            p1, p2 = peaks[-2], peaks[-1]
            if p2 - p1 < order:
                return False
            slope = (vals[p2] - vals[p1]) / (p2 - p1)
            proj_curr = vals[p2] + slope * (len(vals) - 1 - p2)
            proj_prev = vals[p2] + slope * (len(vals) - 2 - p2)
            if prev_val <= proj_prev and curr_val > proj_curr:
                return True

    elif direction == "SHORT":
        troughs = find_troughs(vals, order)
        if len(troughs) >= 2:
            t1, t2 = troughs[-2], troughs[-1]
            if t2 - t1 < order:
                return False
            slope = (vals[t2] - vals[t1]) / (t2 - t1)
            proj_curr = vals[t2] + slope * (len(vals) - 1 - t2)
            proj_prev = vals[t2] + slope * (len(vals) - 2 - t2)
            if prev_val >= proj_prev and curr_val < proj_curr:
                return True
    return False

def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    out.columns = [str(c).lower().strip() for c in out.columns]
    if not isinstance(out.index, pd.DatetimeIndex):
        out.index = pd.to_datetime(out.index)
    if out.index.tz is None:
        out.index = out.index.tz_localize(TZ)
    else:
        out.index = out.index.tz_convert(TZ)
    out = out[~out.index.duplicated(keep="last")].sort_index()
    if not {"open", "high", "low", "close"}.issubset(set(out.columns)):
        return pd.DataFrame()
    return out.dropna(subset=["open", "high", "low", "close"])

def fetch_bars(symbol: str, interval: str, period: str) -> pd.DataFrame:
    if symbol != GOLD_SYMBOL:
        for _ in range(2):
            try:
                df = bp.Ticker(symbol).history(period=period, interval=interval)
                norm = _normalize_df(df)
                if not norm.empty:
                    return norm
            except Exception:
                time.sleep(0.3)
        return pd.DataFrame()

    try:
        df = bp.Ticker(GOLD_SYMBOL).history(period=period, interval=interval)
        norm = _normalize_df(df)
        if not norm.empty:
            return norm
    except Exception:
        pass

    if HAS_YF:
        yf_period = "60d" if interval == "5m" else "730d"
        try:
            yf_df = yf.download(GOLD_SYMBOL, period=yf_period, interval=interval, progress=False, auto_adjust=True)
            if yf_df is not None and not yf_df.empty:
                if isinstance(yf_df.columns, pd.MultiIndex):
                    yf_df.columns = [c[0] for c in yf_df.columns]
                return _normalize_df(yf_df)
        except Exception as e:
            logger.warning(f"GLD yfinance hatası: {e}")
    return pd.DataFrame()

def is_white(symbol: str) -> bool:
    return symbol in WHITE_LIST

def badge(symbol: str) -> str:
    return "⬜🏳️ WHITE LIST" if is_white(symbol) else "🟥🚩 RED LIST"

def is_prime(now_t: dt_time) -> bool:
    return PRIME_START <= now_t < PRIME_END

def is_early_10_11(now_t: dt_time) -> bool:
    return EARLY_WINDOW_START <= now_t < EARLY_WINDOW_END

def can_enter_now(symbol: str, now_t: dt_time) -> bool:
    if symbol == GOLD_SYMBOL:
        return True
    if now_t >= NO_NEW_ENTRY_AFTER:
        return False
    return True

def get_alloc_and_label(symbol: str, now_t: dt_time) -> tuple[float, str]:
    white_flag = is_white(symbol)
    if symbol == GOLD_SYMBOL:
        return (WHITE_PRIME_ALLOC if white_flag else RED_PRIME_ALLOC), "Verimli Saat"
    if is_prime(now_t):
        return (WHITE_PRIME_ALLOC if white_flag else RED_PRIME_ALLOC), "Verimli Saat (11:00-17:00)"
    if is_early_10_11(now_t):
        return (WHITE_EARLY_ALLOC if white_flag else RED_EARLY_ALLOC), "10:00-11:00 Dilimi"
    return (WHITE_OFF_ALLOC if white_flag else RED_OFF_ALLOC), "Saat Dışı"

def calc_sl_tp(avg_cost: float, direction: str, tp_pct: float) -> tuple[float, float]:
    if direction == "LONG":
        return round(avg_cost * (1.0 - SL_PCT / 100.0), 4), round(avg_cost * (1.0 + tp_pct / 100.0), 4)
    return round(avg_cost * (1.0 + SL_PCT / 100.0), 4), round(avg_cost * (1.0 - tp_pct / 100.0), 4)

def record_exit(state: dict, reason: str, tp_pct: float = 0.0) -> None:
    for key in ("daily_stats", "weekly_stats"):
        st = state[key]
        if reason == "SL":
            st["sl"] += 1
        elif reason == "ZB_EXIT":
            st["zb_exit"] += 1
        elif reason == "TP":
            if abs(tp_pct - 4.0) < 1e-6:
                st["tp_4"] += 1
            elif abs(tp_pct - 3.0) < 1e-6:
                st["tp_3"] += 1
            elif abs(tp_pct - 2.5) < 1e-6:
                st["tp_2_5"] += 1
            elif abs(tp_pct - 1.5) < 1e-6:
                st["tp_1_5"] += 1

def format_pozlar(positions: dict, latest_prices: dict[str, float]) -> str:
    if not positions:
        return "📭 <b>Açık Pozisyon Bulunmuyor</b>"
    lines = ["📋 <b>AÇIK POZİSYONLAR</b>\n"]
    for sym, pos in list(positions.items()):
        cur_p = latest_prices.get(sym, pos["avg_cost"])
        is_long = pos["direction"] == "LONG"
        pnl_pct = ((cur_p - pos["avg_cost"]) / pos["avg_cost"]) * 100.0 * (1.0 if is_long else -1.0)
        icon = "🟢" if pnl_pct >= 0 else "🔴"
        lines.append(
            f"{icon} <b>{sym}</b> ({badge(sym)}) | <b>{pos['direction']}</b>\n"
            f"   • Tarama: <code>{pos.get('source_scan', 'Koşan At 5m RSI(8)')}</code>\n"
            f"   • Ort. Maliyet: <code>{pos['avg_cost']:.2f}</code> | Aktif: <code>{cur_p:.2f}</code> | K/Z: <b>%{pnl_pct:+.2f}</b>\n"
            f"   • Tutar: <code>{pos['allocated_tl']:,.0f} TL</code> | SL: <code>{pos['sl']:.2f}</code> | TP: <code>%{pos['tp_pct']:.1f}</code> ({pos['tp']:.2f})"
        )
    return "\n\n".join(lines)

def format_daily(state: dict) -> str:
    st = state["daily_stats"]
    return (
        f"📊 <b>GÜNLÜK İŞLEM ÖZETİ ({st.get('date', '')})</b>\n"
        f"• Koşan At Sinyal Sayısı: <b>{st['kosan_signals']}</b>\n"
        f"• %4 TP Sayısı: <b>{st['tp_4']}</b>\n"
        f"• %3 TP Sayısı: <b>{st['tp_3']}</b>\n"
        f"• %2.5 TP Sayısı: <b>{st['tp_2_5']}</b>\n"
        f"• %1.5 TP Sayısı: <b>{st['tp_1_5']}</b>\n"
        f"• SL Sayısı: <b>{st['sl']}</b>\n"
        f"• Açık Pozisyon: <b>{len(state['positions'])}</b>"
    )

def format_weekly(state: dict) -> str:
    st = state["weekly_stats"]
    return (
        f"🗓️ <b>HAFTALIK İŞLEM ÖZETİ ({st.get('week_key', '')})</b>\n"
        f"• Koşan At Tetiklenen Sinyal: <b>{st['kosan_signals']}</b>\n"
        f"• %4 TP Sayısı: <b>{st['tp_4']}</b>\n"
        f"• %3 TP Sayısı: <b>{st['tp_3']}</b>\n"
        f"• %2.5 TP Sayısı: <b>{st['tp_2_5']}</b>\n"
        f"• %1.5 TP Sayısı: <b>{st['tp_1_5']}</b>\n"
        f"• SL Sayısı: <b>{st['sl']}</b>"
    )

def analyze_symbol_text(symbol: str) -> str:
    df1 = fetch_bars(symbol, "1h", "15d")
    df5 = fetch_bars(symbol, "5m", "5d")
    
    if df1.empty:
        return f"❌ <b>{symbol}</b> için veri çekilemedi veya geçersiz sembol."
        
    df1["rsi8"] = get_rsi(df1["close"], 8)
    df1["rsi14"] = get_rsi(df1["close"], 14)
    df1["ema50"] = df1["close"].ewm(span=50, adjust=False).mean()
    df1["ema200"] = df1["close"].ewm(span=200, adjust=False).mean()
    
    cur_p = float(df1['close'].iloc[-1])
    rsi8 = float(df1['rsi8'].iloc[-1])
    rsi14 = float(df1['rsi14'].iloc[-1])
    ema50 = float(df1['ema50'].iloc[-1])
    ema200 = float(df1['ema200'].iloc[-1]) if not pd.isna(df1['ema200'].iloc[-1]) else cur_p
    
    trend_1h = "🟢 Yukarı" if cur_p > ema200 else "🔴 Aşağı"
    
    msg = (
        f"🤖 <b>{symbol} — Kapsamlı Analiz</b>\n\n"
        f"📊 <b>1 Saatlik (1H)</b>\n"
        f"• Fiyat: <code>{cur_p:.2f}</code>\n"
        f"• RSI(8): <code>{rsi8:.1f}</code>\n"
        f"• RSI(14): <code>{rsi14:.1f}</code>\n"
        f"• Trend: {trend_1h} (EMA 200)\n"
        f"• EMA 50: <code>{ema50:.2f}</code>\n"
    )
    
    if not df5.empty:
        df5["rsi5"] = get_rsi(df5["close"], 8)
        df5["ema50"] = df5["close"].ewm(span=50, adjust=False).mean()
        cur_5m = float(df5['close'].iloc[-1])
        rsi5 = float(df5['rsi5'].iloc[-1])
        ema50_5m = float(df5['ema50'].iloc[-1])
        trend_5m = "🟢 EMA50 Üstü" if cur_5m > ema50_5m else "🔴 EMA50 Altı"
        msg += (
            f"\n⚓ <b>5 Dakikalık (5M)</b>\n"
            f"• RSI(8): <code>{rsi5:.1f}</code>\n"
            f"• Trend: {trend_5m} (EMA 50: <code>{ema50_5m:.2f}</code>)\n"
        )
    return msg

def poll_telegram(state: dict, latest_prices: dict[str, float]) -> None:
    if not BOT_TOKEN or not CHAT_IDS:
        return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates"
    params = {"offset": state.get("tg_offset", 0) + 1, "timeout": 1}
    try:
        resp = requests.get(url, params=params, timeout=5)
        if not resp.ok:
            return
        for upd in resp.json().get("result", []):
            state["tg_offset"] = upd["update_id"]
            msg = upd.get("message") or {}
            txt = (msg.get("text") or "").strip()
            chat_id = int((msg.get("chat") or {}).get("id", 0))
            
            if chat_id not in CHAT_IDS:
                continue
                
            # Komut yanıtlarında artık alıntı (reply) yapılmıyor, sadece o kişiye gidiyor
            cmd_text = txt.upper().replace("/", "").split("@")[0].strip()

            if cmd_text == "START":
                start_msg = (
                    "✅ <b>Sistem çalışıyor.</b>\n\n"
                    "📌 <b>Komutlar:</b>\n"
                    "• <code>/asels</code> — Hisse analizi\n"
                    "• <code>/tara</code> — Anlık tarama\n"
                    "• <code>/poz</code> — Açık pozisyonlar\n\n"
                    "💡 <i>Analiz için /ASELS gibi komut yazabilirsiniz.</i>"
                )
                send_tg(start_msg, target_chat_id=chat_id)
                
            elif cmd_text in ["YARDIM", "HELP"]:
                help_msg = (
                    "🤖 <b>Yardım Menüsü</b>\n\n"
                    "• Hisse analizi: <code>/ASELS</code>\n"
                    "• Anında tarama: <code>/tara</code>\n"
                    "• Açık pozisyonlar: <code>/poz</code>\n\n"
                    "Sistem arka planda Koşan At sinyallerini tarar."
                )
                send_tg(help_msg, target_chat_id=chat_id)
                
            elif cmd_text == "POZ":
                send_tg(format_pozlar(state["positions"], latest_prices), target_chat_id=chat_id)
                
            elif cmd_text == "TARA":
                send_tg("🔄 Manuel tarama başlatıldı, lütfen bekleyin...", target_chat_id=chat_id)
                with STATE_LOCK:
                    manual_notify_queue.append(chat_id) # Alıntısız bildirim için sadece chat_id tutuyoruz
                scan_event.set() # Tarayıcı döngüsünü uyandır
                
            elif cmd_text in BIST_SYMBOLS or cmd_text == GOLD_SYMBOL or cmd_text == INDEX_SYMBOL:
                send_tg(f"🔍 {cmd_text} analiz ediliyor, lütfen bekleyin...", target_chat_id=chat_id)
                analysis = analyze_symbol_text(cmd_text)
                send_tg(analysis, target_chat_id=chat_id)
                
    except Exception as e:
        logger.debug(f"Telegram polling hatası: {e}")

def sync_schedules(state: dict, now_dt: datetime) -> None:
    today_str = now_dt.strftime("%Y-%m-%d")
    week_str = now_dt.strftime("%Y-W%U")
    now_t = now_dt.time()

    if state["daily_stats"]["date"] != today_str:
        state["daily_stats"] = {
            "date": today_str, "kosan_signals": 0, "tp_4": 0, "tp_3": 0,
            "tp_2_5": 0, "tp_1_5": 0, "sl": 0, "zb_exit": 0,
        }
        state["sent_flags"] = {
            "date": today_str, "open_msg": False, "prime_active": is_prime(now_t),
            "daily_summary": False, "weekly_summary": False, "weekly_cleanup": False,
        }

    if state["weekly_stats"]["week_key"] != week_str:
        state["weekly_stats"] = {
            "week_key": week_str, "kosan_signals": 0, "tp_4": 0, "tp_3": 0,
            "tp_2_5": 0, "tp_1_5": 0, "sl": 0, "zb_exit": 0,
        }

    if now_dt.weekday() <= 4:
        if now_t >= MARKET_OPEN_MSG_TIME and not state["sent_flags"]["open_msg"]:
            send_tg("🔔 <b>Borsa Açıldı</b> (09:50)\nKoşan At canlı asistanı aktif.")
            state["sent_flags"]["open_msg"] = True

        prime_now = is_prime(now_t)
        prev_prime = state["sent_flags"]["prime_active"]
        if prime_now and not prev_prime:
            send_tg("⚡ <b>En Verimli Saatler Başladı (11:00)</b>\nWhite List 100K / Red List 50K kademeleri aktif.")
            state["sent_flags"]["prime_active"] = True
        elif (not prime_now) and prev_prime:
            send_tg("💤 <b>En Verimli Saatler Bitti (17:00)</b>\n17:00 sonrası yeni işlem kapalı; yalnızca mevcut pozisyonlar takip ediliyor.")
            state["sent_flags"]["prime_active"] = False

        if now_t >= DAILY_SUMMARY_TIME and not state["sent_flags"]["daily_summary"]:
            send_tg(format_daily(state))
            state["sent_flags"]["daily_summary"] = True

        if now_dt.weekday() == 4:
            if now_t >= WEEKLY_SUMMARY_TIME and not state["sent_flags"]["weekly_summary"]:
                send_tg(format_weekly(state))
                state["sent_flags"]["weekly_summary"] = True

            if now_t >= WEEKLY_CLEANUP_TIME and not state["sent_flags"]["weekly_cleanup"]:
                cleaned = len(state["positions"])
                state["positions"] = {}
                state["sent_flags"]["weekly_cleanup"] = True
                send_tg(f"🧹 <b>Haftalık Temizlik Tamamlandı (Cuma 18:30)</b>\nTemizlenen açık pozisyon kaydı: <b>{cleaned}</b>")

def run_cycle(state: dict) -> None:
    now_dt = datetime.now(TZ)
    now_t = now_dt.time()
    sync_schedules(state, now_dt)

    xu_trend = 0
    xu_h1 = fetch_bars(INDEX_SYMBOL, "1h", "5d")
    if not xu_h1.empty and len(xu_h1) >= 50:
        xu_ema50 = xu_h1["close"].ewm(span=50, adjust=False).mean()
        xu_trend = 1 if float(xu_h1["close"].iloc[-1]) > float(xu_ema50.iloc[-1]) else -1

    for sym in BIST_SYMBOLS + [GOLD_SYMBOL]:
        df5 = fetch_bars(sym, "5m", "5d")
        df1 = fetch_bars(sym, "1h", "15d")
        if df5.empty or df1.empty or len(df5) < 55 or len(df1) < 25:
            continue

        df5 = df5.copy()
        df5["rsi5"] = get_rsi(df5["close"], KOSAN_RSI_PERIOD)
        df5["ema50"] = df5["close"].ewm(span=50, adjust=False).mean()
        df5 = df5.dropna(subset=["rsi5", "ema50"])
        if len(df5) < 2:
            continue

        df1 = df1.copy()
        df1["rsi14"] = get_rsi(df1["close"], 14)
        df1["rsi8"] = get_rsi(df1["close"], 8)
        last_h1_idx = len(df1) - 1
        zb_long_now = check_zeki_bey_series(df1["rsi8"], last_h1_idx, "LONG", lookback=ZB_LOOKBACK, order=ZB_ORDER)
        zb_short_now = check_zeki_bey_series(df1["rsi8"], last_h1_idx, "SHORT", lookback=ZB_LOOKBACK, order=ZB_ORDER)
        rsi14_now = float(df1["rsi14"].iloc[-1]) if pd.notna(df1["rsi14"].iloc[-1]) else 50.0

        curr = df5.iloc[-1]
        prev = df5.iloc[-2]
        cur_p = float(curr["close"])
        hi_p = float(curr["high"])
        lo_p = float(curr["low"])
        ema50_p = float(curr["ema50"])
        LATEST_PRICES[sym] = cur_p

        new_h1_bar = state["last_seen_h1_pos"].get(sym, -1) != last_h1_idx
        if new_h1_bar:
            state["last_seen_h1_pos"][sym] = last_h1_idx

        pos = state["positions"].get(sym)
        white_flag = is_white(sym)

        if pos is not None:
            is_long = pos["direction"] == "LONG"
            closed = False

            if new_h1_bar:
                same_zb = (is_long and zb_long_now) or ((not is_long) and zb_short_now)
                opp_zb = (is_long and zb_short_now) or ((not is_long) and zb_long_now)

                if opp_zb:
                    pnl_pct = ((cur_p - pos["avg_cost"]) / pos["avg_cost"]) * 100.0 * (1.0 if is_long else -1.0)
                    msg = (
                        f"🛑 <b>ZB ÇIKIŞ TETİKLENDİ — POZİSYON KAPANDI</b>\n"
                        f"{badge(sym)} | <b>{sym}</b> ({pos['direction']})\n"
                        f"Kapanış: <code>{cur_p:.2f}</code> | K/Z: <b>%{pnl_pct:+.2f}</b>"
                    )
                    send_tg(msg, reply_ids=pos.get("msg_ids"))
                    record_exit(state, "ZB_EXIT", pos["tp_pct"])
                    del state["positions"][sym]
                    closed = True

                elif same_zb:
                    target_zb_tp = WHITE_ZB_TP_PCT if white_flag else RED_ZB_TP_PCT
                    if pos["tp_pct"] < target_zb_tp:
                        _, new_tp = calc_sl_tp(pos["avg_cost"], pos["direction"], target_zb_tp)
                        pos["tp_pct"] = target_zb_tp
                        pos["tp"] = new_tp
                        msg = (
                            f"🎯 <b>ZEKİ BEY ONAYI — TP %{target_zb_tp:.1f} GÜNCELLENDİ</b>\n"
                            f"{badge(sym)} | <b>{sym}</b> ({pos['direction']})\n"
                            f"Ort. Maliyet: <code>{pos['avg_cost']:.2f}</code> | Yeni TP: <code>{new_tp:.2f}</code>"
                        )
                        send_tg(msg, reply_ids=pos.get("msg_ids"))

            if not closed:
                sl_hit = (lo_p <= pos["sl"]) if is_long else (hi_p >= pos["sl"])
                if sl_hit:
                    pnl_pct = ((pos["sl"] - pos["avg_cost"]) / pos["avg_cost"]) * 100.0 * (1.0 if is_long else -1.0)
                    msg = (
                        f"❌ <b>STOP-LOSS (SL)</b> — <b>{sym}</b> ({pos['direction']})\n"
                        f"{badge(sym)} | SL: <code>{pos['sl']:.2f}</code> | K/Z: <b>%{pnl_pct:+.2f}</b>"
                    )
                    send_tg(msg, reply_ids=pos.get("msg_ids"))
                    record_exit(state, "SL", pos["tp_pct"])
                    del state["positions"][sym]
                    closed = True

            if not closed and white_flag and (not pos.get("added_2nd", False)):
                hit_2nd = (lo_p <= ema50_p) if is_long else (hi_p >= ema50_p)
                if hit_2nd:
                    prev_alloc = pos["allocated_tl"]
                    new_alloc = prev_alloc + WHITE_ADD_ALLOC
                    new_avg = ((pos["avg_cost"] * prev_alloc) + (ema50_p * WHITE_ADD_ALLOC)) / new_alloc
                    new_sl, new_tp = calc_sl_tp(new_avg, pos["direction"], pos["tp_pct"])
                    pos["avg_cost"] = round(new_avg, 4)
                    pos["allocated_tl"] = new_alloc
                    pos["sl"] = new_sl
                    pos["tp"] = new_tp
                    pos["added_2nd"] = True
                    msg = (
                        f"➕ <b>EKLEME YAPILDI (+{WHITE_ADD_ALLOC:,.0f} TL)</b> — <b>{sym}</b>\n"
                        f"{badge(sym)} | Toplam Tutar: <code>{new_alloc:,.0f} TL</code>\n"
                        f"Güncel Ort. Maliyet: <code>{pos['avg_cost']:.2f}</code>\n"
                        f"Güncel SL: <code>{new_sl:.2f}</code> | Güncel TP (%{pos['tp_pct']:.1f}): <code>{new_tp:.2f}</code>"
                    )
                    send_tg(msg, reply_ids=pos.get("msg_ids"))

            if not closed:
                tp_hit = (hi_p >= pos["tp"]) if is_long else (lo_p <= pos["tp"])
                if tp_hit:
                    msg = (
                        f"✅ <b>TAKE-PROFIT (TP %{pos['tp_pct']:.1f})</b> — <b>{sym}</b> ({pos['direction']})\n"
                        f"{badge(sym)} | TP: <code>{pos['tp']:.2f}</code>"
                    )
                    send_tg(msg, reply_ids=pos.get("msg_ids"))
                    record_exit(state, "TP", pos["tp_pct"])
                    del state["positions"][sym]
                    closed = True

            continue

        if not can_enter_now(sym, now_t):
            continue

        xu_ok_long = True if sym == GOLD_SYMBOL else (xu_trend == 1)
        xu_ok_short = True if sym == GOLD_SYMBOL else (xu_trend == -1)

        long_sig = (
            prev["rsi5"] <= KOSAN_LONG_LEVEL
            and curr["rsi5"] > KOSAN_LONG_LEVEL
            and cur_p > ema50_p
            and rsi14_now > 50
            and xu_ok_long
        )
        short_sig = (
            prev["rsi5"] >= KOSAN_SHORT_LEVEL
            and curr["rsi5"] < KOSAN_SHORT_LEVEL
            and cur_p < ema50_p
            and rsi14_now < 50
            and xu_ok_short
        )

        if not (long_sig or short_sig):
            continue

        direction = "LONG" if long_sig else "SHORT"
        tp_pct = WHITE_BASE_TP_PCT if white_flag else RED_BASE_TP_PCT
        zb_target_tp = WHITE_ZB_TP_PCT if white_flag else RED_ZB_TP_PCT
        alloc_tl, saat_etiket = get_alloc_and_label(sym, now_t)
        sl_val, tp_val = calc_sl_tp(cur_p, direction, tp_pct)

        sig_text = (
            f"🐎 <b>KOŞAN AT SİNYALİ — {sym} ({direction})</b>\n"
            f"{badge(sym)} | <i>{saat_etiket}</i>\n"
            f"• Giriş: <code>{cur_p:.2f}</code>\n"
            f"• Portföy Tutarı: <b>{alloc_tl:,.0f} TL</b>"
            + (" <i>(5m EMA50'de +50K ekleme aktif)</i>" if white_flag else " <i>(Ekleme yok)</i>")
            + f"\n• Stop-Loss (%{SL_PCT:.1f}): <code>{sl_val:.2f}</code>\n"
            f"• Baz TP (%{tp_pct:.1f}): <code>{tp_val:.2f}</code> <i>(ZB onayında %{zb_target_tp:.1f})</i>"
        )
        
        msg_ids = send_tg(sig_text)

        state["positions"][sym] = {
            "symbol": sym,
            "direction": direction,
            "source_scan": "Koşan At (5m RSI8 + 1H Filtre)",
            "entry_price": cur_p,
            "avg_cost": cur_p,
            "allocated_tl": alloc_tl,
            "sl": sl_val,
            "tp": tp_val,
            "tp_pct": tp_pct,
            "added_2nd": False,
            "msg_ids": msg_ids,
            "opened_at": now_dt.isoformat(),
        }
        state["daily_stats"]["kosan_signals"] += 1
        state["weekly_stats"]["kosan_signals"] += 1

    save_state(state)

# --- ANA DÖNGÜLER (THREADS) ---

def scanner_worker() -> None:
    """Arka planda 60 saniyede bir tarama yapar."""
    state = load_state()
    logger.info("Veri tarayıcı döngüsü başladı.")
    while True:
        # 60 saniye bekle VEYA /tara komutu gelirse hemen uyan
        scan_event.wait(timeout=60)
        scan_event.clear()
        
        targets = []
        with STATE_LOCK:
            while manual_notify_queue:
                targets.append(manual_notify_queue.pop(0))

        try:
            run_cycle(state)
        except Exception as e:
            logger.error(f"Tarama hatası: {e}")

        for cid in targets:
            send_tg("✅ Tarama tamamlandı. Sinyal varsa mesajınız gelmiştir.", target_chat_id=cid)

def telegram_worker() -> None:
    """Saniyede bir Telegram komutlarını kontrol eder (Anında cevap için)."""
    state = load_state()
    logger.info("Telegram komut dinleyici döngüsü başladı.")
    while True:
        try:
            with STATE_LOCK:
                latest_p = {s: p["avg_cost"] for s, p in state["positions"].items()}
                latest_p.update(LATEST_PRICES)
                poll_telegram(state, latest_p)
                save_state(state)
        except Exception as e:
            logger.error(f"Telegram döngü hatası: {e}")
        time.sleep(1) # Her 1 saniyede bir kontrol et (Anında yanıt)

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(b"OK - Kosan At Canli Asistan Aktif")

    def log_message(self, format, *args):
        return

def start_web_server() -> None:
    server = ThreadingHTTPServer(("0.0.0.0", WEB_PORT), HealthHandler)
    logger.info(f"Render HTTP sağlık sunucusu 0.0.0.0:{WEB_PORT} üzerinde dinliyor.")
    server.serve_forever()

def main() -> None:
    # 1. Veri Tarayıcı Thread (Yavaş işler)
    threading.Thread(target=scanner_worker, daemon=True).start()
    # 2. Telegram Komut Dinleyici Thread (Hızlı işler)
    threading.Thread(target=telegram_worker, daemon=True).start()
    # 3. Web Sunucu (Render için)
    start_web_server()

if __name__ == "__main__":
    main()