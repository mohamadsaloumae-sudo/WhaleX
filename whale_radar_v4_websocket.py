# ============================================================
# 🐋 رادار الحيتان — النسخة v4.0 WebSocket
# ============================================================
# المتطلبات:
# pip install python-binance pandas ta requests python-dotenv psycopg2-binary websocket-client
# ============================================================

import os, time, logging, requests, json, threading
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from collections import defaultdict, deque
from dotenv import load_dotenv
from binance import ThreadedWebsocketManager
from binance.client import Client
from binance.exceptions import BinanceAPIException
import ta
from psycopg2.extras import RealDictCursor
import psycopg2

load_dotenv()

# ============================================================
# ⚙️ الإعدادات
# ============================================================
BINANCE_API_KEY    = os.getenv("BINANCE_API_KEY", "")
BINANCE_SECRET_KEY = os.getenv("BINANCE_SECRET_KEY", "")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")
DATABASE_URL       = os.getenv("DATABASE_URL", "")

# ── حدود الإشارات ──
MIN_SCORE         = 2
MIN_CONFIDENCE    = 45
MIN_SCORE_RELAXED = 1
MIN_CONF_RELAXED  = 35

# ── إعدادات الاستراتيجيات ──
WHALE_VOL_MULT    = 4.0
RSI_OVERSOLD      = 40
RSI_OVERBOUGHT    = 60
BREAKOUT_LOOKBACK = 20
SR_LOOKBACK       = 50
SR_ZONE_PCT       = 0.008
VP_BINS           = 20

# ── إعدادات WebSocket ──
KLINE_INTERVAL    = "15m"       # إطار زمني الشموع
CANDLES_REQUIRED  = 100         # شموع مطلوبة قبل التحليل
TOP_SYMBOLS_COUNT = 40          # عدد العملات المراقبة
SIGNAL_COOLDOWN   = 60 * 60 * 4 # 4 ساعات بين كل إشارة لنفس العملة (ثانية)
MAX_SIGNALS_HOUR  = 10          # حد أقصى للإشارات في الساعة
HEALTH_INTERVAL   = 60 * 30     # رسالة صحة كل 30 دقيقة

# ============================================================
# 🪵 السجلات
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("whale_radar_v4.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ============================================================
# 🗂️ مخزن البيانات في الذاكرة (بدل قاعدة بيانات للكاندل)
# ============================================================
class CandleStore:
    """يخزن الشموع لكل عملة في الذاكرة بشكل مستمر"""

    def __init__(self, max_candles=150):
        self.max_candles = max_candles
        # symbol → deque of candle dicts
        self._store: dict[str, deque] = defaultdict(lambda: deque(maxlen=max_candles))
        self._lock  = threading.Lock()

    def update(self, symbol: str, candle: dict):
        """تحديث أو إضافة شمعة"""
        with self._lock:
            q = self._store[symbol]
            if q and q[-1]['time'] == candle['time']:
                q[-1] = candle        # تحديث الشمعة الحالية
            else:
                q.append(candle)      # إضافة شمعة جديدة

    def get_df(self, symbol: str) -> pd.DataFrame | None:
        with self._lock:
            q = self._store[symbol]
            if len(q) < CANDLES_REQUIRED:
                return None
            df = pd.DataFrame(list(q))
            for col in ['open','high','low','close','volume']:
                df[col] = pd.to_numeric(df[col])
            return df

    def symbol_count(self) -> int:
        with self._lock:
            return sum(1 for q in self._store.values() if len(q) >= CANDLES_REQUIRED)

    def candle_count(self, symbol: str) -> int:
        with self._lock:
            return len(self._store[symbol])


# ============================================================
# ⏱️ مدير الإشارات (cooldown + rate limit)
# ============================================================
class SignalManager:
    def __init__(self):
        self._last_signal: dict[str, float] = {}   # symbol → timestamp
        self._hourly:      deque             = deque()  # timestamps في آخر ساعة
        self._lock         = threading.Lock()

    def can_signal(self, symbol: str) -> bool:
        now = time.time()
        with self._lock:
            # Cooldown لنفس العملة
            last = self._last_signal.get(symbol, 0)
            if now - last < SIGNAL_COOLDOWN:
                remaining = int((SIGNAL_COOLDOWN - (now - last)) / 60)
                log.debug(f"⏳ {symbol} cooldown {remaining}m متبقي")
                return False
            # Rate limit الساعة
            cutoff = now - 3600
            while self._hourly and self._hourly[0] < cutoff:
                self._hourly.popleft()
            if len(self._hourly) >= MAX_SIGNALS_HOUR:
                log.warning(f"⚠️ تجاوز حد الساعة ({MAX_SIGNALS_HOUR} إشارة)")
                return False
            return True

    def record(self, symbol: str):
        now = time.time()
        with self._lock:
            self._last_signal[symbol] = now
            self._hourly.append(now)

    def signals_this_hour(self) -> int:
        now = time.time(); cutoff = now - 3600
        with self._lock:
            while self._hourly and self._hourly[0] < cutoff:
                self._hourly.popleft()
            return len(self._hourly)

    def reset_symbol(self, symbol: str):
        with self._lock:
            self._last_signal.pop(symbol, None)


# ============================================================
# 🔌 قاعدة البيانات
# ============================================================
def get_connection():
    try:
        return psycopg2.connect(DATABASE_URL)
    except Exception as e:
        log.error(f"❌ DB: {e}")
        return None

def setup_database():
    conn = get_connection()
    if not conn: return
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS active_trades (
                id           SERIAL PRIMARY KEY,
                symbol       VARCHAR(20) UNIQUE NOT NULL,
                entry_price  DECIMAL(20,8),
                direction    VARCHAR(10),
                sl           DECIMAL(20,8),
                tp1          DECIMAL(20,8),
                tp2          DECIMAL(20,8),
                tp3          DECIMAL(20,8),
                tp1_hit      BOOLEAN DEFAULT FALSE,
                tp2_hit      BOOLEAN DEFAULT FALSE,
                strategy     TEXT,
                score        INTEGER DEFAULT 0,
                grade        VARCHAR(5) DEFAULT 'C',
                confidence   INTEGER DEFAULT 0,
                status       VARCHAR(10) DEFAULT 'OPEN',
                created_at   TIMESTAMP DEFAULT NOW(),
                updated_at   TIMESTAMP DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS signal_history (
                id          SERIAL PRIMARY KEY,
                symbol      VARCHAR(20),
                signal_type VARCHAR(50),
                direction   VARCHAR(10),
                price       DECIMAL(20,8),
                score       INTEGER,
                grade       VARCHAR(5),
                confidence  INTEGER,
                strategies  TEXT,
                sent_at     TIMESTAMP DEFAULT NOW()
            )
        """)
        conn.commit()
        log.info("✅ DB جاهز v4.0")
    except Exception as e:
        log.error(f"❌ DB setup: {e}")
    finally:
        conn.close()

def save_active_trade(symbol, entry_price, direction, sl, tp1, tp2, tp3,
                      strategy="", score=0, grade="C", confidence=0):
    conn = get_connection()
    if not conn: return
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO active_trades
                (symbol,entry_price,direction,sl,tp1,tp2,tp3,
                 strategy,score,grade,confidence,status,updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'OPEN',NOW())
            ON CONFLICT (symbol) DO UPDATE SET
                entry_price=EXCLUDED.entry_price, direction=EXCLUDED.direction,
                sl=EXCLUDED.sl, tp1=EXCLUDED.tp1, tp2=EXCLUDED.tp2,
                tp3=EXCLUDED.tp3, strategy=EXCLUDED.strategy,
                score=EXCLUDED.score, grade=EXCLUDED.grade,
                confidence=EXCLUDED.confidence, status='OPEN', updated_at=NOW()
        """, (symbol,entry_price,direction,sl,tp1,tp2,tp3,
              strategy,score,grade,confidence))
        conn.commit()
    except Exception as e:
        log.error(f"❌ save_trade {symbol}: {e}")
    finally:
        conn.close()

def load_active_trades() -> dict:
    conn = get_connection()
    if not conn: return {}
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT * FROM active_trades WHERE status='OPEN'")
        return {
            r['symbol']: {
                "entry":      float(r['entry_price']),
                "sl":         float(r['sl']),
                "tp1":        float(r['tp1']),
                "tp2":        float(r['tp2']),
                "tp3":        float(r['tp3']),
                "direction":  r['direction'],
                "tp1_hit":    r['tp1_hit'],
                "tp2_hit":    r['tp2_hit'],
                "grade":      r.get('grade','C'),
                "confidence": r.get('confidence',0),
                "score":      r.get('score',0),
            }
            for r in cur.fetchall()
        }
    except Exception as e:
        log.error(f"❌ load_trades: {e}")
        return {}
    finally:
        conn.close()

def update_trade_sl(symbol, new_sl):
    conn = get_connection()
    if not conn: return
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE active_trades SET sl=%s,updated_at=NOW() WHERE symbol=%s AND status='OPEN'",
            (new_sl, symbol))
        conn.commit()
    except Exception as e:
        log.error(f"❌ update_sl: {e}")
    finally:
        conn.close()

def update_tp_hit(symbol, tp_num):
    conn = get_connection()
    if not conn: return
    try:
        cur = conn.cursor()
        cur.execute(
            f"UPDATE active_trades SET tp{tp_num}_hit=TRUE,updated_at=NOW() WHERE symbol=%s",
            (symbol,))
        conn.commit()
    except Exception as e:
        log.error(f"❌ tp_hit: {e}")
    finally:
        conn.close()

def close_trade(symbol):
    conn = get_connection()
    if not conn: return
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE active_trades SET status='CLOSED',updated_at=NOW() WHERE symbol=%s",
            (symbol,))
        conn.commit()
    except Exception as e:
        log.error(f"❌ close_trade: {e}")
    finally:
        conn.close()

def save_signal_history(symbol, sig_type, direction, price,
                        score, grade, confidence, strategies):
    conn = get_connection()
    if not conn: return
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO signal_history
                (symbol,signal_type,direction,price,score,grade,confidence,strategies)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        """, (symbol,sig_type,direction,price,score,grade,confidence,strategies))
        conn.commit()
    except Exception as e:
        log.error(f"❌ signal_history: {e}")
    finally:
        conn.close()

# ============================================================
# 📡 Telegram
# ============================================================
def send_telegram(message: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("⚠️ Telegram غير مُعَد")
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(url, json={
            "chat_id":    TELEGRAM_CHAT_ID,
            "text":       message,
            "parse_mode": "HTML"
        }, timeout=15)
        ok = r.status_code == 200
        if not ok:
            log.warning(f"⚠️ Telegram {r.status_code}: {r.text[:150]}")
        return ok
    except Exception as e:
        log.error(f"❌ Telegram: {e}")
        return False

# ============================================================
# 🎨 نظام التقييم
# ============================================================
def compute_grade_confidence(score, triggered_count, dir_ratio):
    s  = min(score / 10, 1.0)
    c  = dir_ratio
    cv = min(triggered_count / 4, 1.0)
    conf = int((s*0.45 + c*0.35 + cv*0.20) * 100)
    conf = max(0, min(100, conf))
    if   score >= 8 and conf >= 80: grade = "S"
    elif score >= 6 and conf >= 70: grade = "A"
    elif score >= 4 and conf >= 58: grade = "B"
    else:                           grade = "C"
    return grade, conf

def grade_emoji(g):
    return {"S":"💎","A":"🥇","B":"🥈","C":"🥉"}.get(g,"🥉")

def conf_bar(c):
    f = round(c/10)
    return "█"*f + "░"*(10-f)

# ============================================================
# 📨 رسائل Telegram
# ============================================================
def fmt_signal(sym, direction, price, sl, tp1, tp2, tp3,
               score, grade, conf, strategies, details):
    arrow   = "🟢 LONG  📈" if direction=="LONG" else "🔴 SHORT 📉"
    sl_p    = abs((sl  - price)/price*100)
    tp1_p   = abs((tp1 - price)/price*100)
    tp2_p   = abs((tp2 - price)/price*100)
    tp3_p   = abs((tp3 - price)/price*100)
    rr      = tp2_p/sl_p if sl_p > 0 else 0
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    return f"""
🐋 <b>رادار الحيتان v4.0 WebSocket</b>
⚡ <i>إشارة فورية لحظية</i>  |  🕐 {now_str}

{arrow}
💰 <b>العملة:</b> <code>{sym}</code>
💵 <b>سعر الدخول:</b> <code>{price:.6f}</code>

🛡️ <b>وقف الخسارة:</b> <code>{sl:.6f}</code>  <i>(-{sl_p:.1f}%)</i>
🎯 <b>الهدف 1:</b> <code>{tp1:.6f}</code>  <i>(+{tp1_p:.1f}%)</i>
🎯 <b>الهدف 2:</b> <code>{tp2:.6f}</code>  <i>(+{tp2_p:.1f}%)</i>
🎯 <b>الهدف 3:</b> <code>{tp3:.6f}</code>  <i>(+{tp3_p:.1f}%)</i>
📐 <b>نسبة R:R:</b>  <code>1 : {rr:.1f}</code>

📊 <b>الاستراتيجيات:</b>
{strategies}

🔍 <b>التفاصيل:</b>
<i>{details}</i>

{grade_emoji(grade)} <b>الدرجة:</b> <code>{grade}</code>  |  🎯 <b>الثقة:</b> <code>{conf}%</code>
<code>[{conf_bar(conf)}]</code>
🏆 <b>النقاط:</b> <code>{score}/10</code>

⚠️ <i>تحليل فني فقط — ليست نصيحة مالية</i>
""".strip()

def fmt_tp(sym, tp_num, price, grade, conf):
    return f"""
✅ <b>تحقق الهدف {tp_num}!</b>

💰 <b>العملة:</b> <code>{sym}</code>
💵 <b>السعر:</b> <code>{price:.6f}</code>
{grade_emoji(grade)} درجة الإشارة: {grade} ({conf}%)
💡 <i>SL تحرّك لنقطة الدخول تلقائياً</i>
""".strip()

def fmt_sl(sym, price):
    return f"""
🔴 <b>ضرب وقف الخسارة</b>

💰 <b>العملة:</b> <code>{sym}</code>
💵 <b>السعر:</b> <code>{price:.6f}</code>
❌ تم إغلاق الصفقة تلقائياً
""".strip()

# ============================================================
# 🧠 الاستراتيجيات العشر
# ============================================================
def strat_whale(df):
    avg = df['volume'].rolling(20).mean().iloc[-1]
    vol = df['volume'].iloc[-1]
    r   = vol / avg if avg > 0 else 0
    if r < WHALE_VOL_MULT:
        return {"triggered": False}
    d = "LONG" if df['close'].iloc[-1] > df['open'].iloc[-1] else "SHORT"
    s = 3 if r >= WHALE_VOL_MULT*2 else 2 if r >= WHALE_VOL_MULT*1.5 else 1
    return {"triggered":True,"direction":d,"score":s,
            "detail":f"🐋 حجم ×{r:.1f}"}

def strat_rsi_macd(df):
    close = df['close']
    rsi_v = ta.momentum.RSIIndicator(close, 14).rsi().iloc[-1]
    macd  = ta.trend.MACD(close)
    ml, sg = macd.macd(), macd.macd_signal()
    up   = ml.iloc[-2] < sg.iloc[-2] and ml.iloc[-1] > sg.iloc[-1]
    down = ml.iloc[-2] > sg.iloc[-2] and ml.iloc[-1] < sg.iloc[-1]
    if up   and rsi_v < RSI_OVERSOLD  + 20:
        return {"triggered":True,"direction":"LONG","score":2,"detail":f"📊 MACD↑ RSI={rsi_v:.1f}"}
    if down and rsi_v > RSI_OVERBOUGHT - 20:
        return {"triggered":True,"direction":"SHORT","score":2,"detail":f"📊 MACD↓ RSI={rsi_v:.1f}"}
    return {"triggered":False}

def strat_breakout(df):
    highs = df['high'].iloc[-BREAKOUT_LOOKBACK:-1]
    lows  = df['low'].iloc[-BREAKOUT_LOOKBACK:-1]
    c     = df['close'].iloc[-1]
    avg_v = df['volume'].rolling(20).mean().iloc[-1]
    vol_ok= df['volume'].iloc[-1] > avg_v * 1.2
    if c > highs.max() and vol_ok:
        return {"triggered":True,"direction":"LONG","score":1,"detail":f"💥 اختراق {highs.max():.4f}"}
    if c < lows.min()  and vol_ok:
        return {"triggered":True,"direction":"SHORT","score":1,"detail":f"💥 كسر {lows.min():.4f}"}
    return {"triggered":False}

def strat_trend(df):
    close = df['close']
    e21   = ta.trend.EMAIndicator(close,21).ema_indicator().iloc[-1]
    e55   = ta.trend.EMAIndicator(close,55).ema_indicator().iloc[-1]
    p     = close.iloc[-1]
    if p > e21 > e55:
        return {"triggered":True,"direction":"LONG","score":1,"detail":f"📈 EMA21({e21:.3f})>EMA55"}
    if p < e21 < e55:
        return {"triggered":True,"direction":"SHORT","score":1,"detail":f"📉 EMA21({e21:.3f})<EMA55"}
    return {"triggered":False}

def strat_bb(df):
    close = df['close']
    bb    = ta.volatility.BollingerBands(close, 20, 2)
    up    = bb.bollinger_hband().iloc[-1]
    lo    = bb.bollinger_lband().iloc[-1]
    mid   = bb.bollinger_mavg().iloc[-1]
    p, pp = close.iloc[-1], close.iloc[-2]
    w     = (up-lo)/mid if mid>0 else 0
    if pp<=lo and p>lo and w<0.15:
        return {"triggered":True,"direction":"LONG","score":2,"detail":f"📉→ BB ارتداد أدنى ({lo:.4f})"}
    if pp>=up and p<up and w<0.15:
        return {"triggered":True,"direction":"SHORT","score":2,"detail":f"📈→ BB ارتداد أعلى ({up:.4f})"}
    avg_v = df['volume'].rolling(20).mean().iloc[-1]
    if p>up and df['volume'].iloc[-1]>avg_v*1.8:
        return {"triggered":True,"direction":"LONG","score":1,"detail":f"🚀 BB اختراق ({up:.4f})"}
    if p<lo and df['volume'].iloc[-1]>avg_v*1.8:
        return {"triggered":True,"direction":"SHORT","score":1,"detail":f"🔻 BB اختراق ({lo:.4f})"}
    return {"triggered":False}

def strat_stoch(df):
    close = df['close']
    st    = ta.momentum.StochRSIIndicator(close, 14, 3, 3)
    k,d   = st.stochrsi_k().iloc[-1], st.stochrsi_d().iloc[-1]
    pk,pd = st.stochrsi_k().iloc[-2], st.stochrsi_d().iloc[-2]
    if pk<pd and k>d and k<0.30:
        return {"triggered":True,"direction":"LONG","score":2,"detail":f"⚡ StochRSI K({k:.2f})↑ تشبع بيع"}
    if pk>pd and k<d and k>0.70:
        return {"triggered":True,"direction":"SHORT","score":2,"detail":f"⚡ StochRSI K({k:.2f})↓ تشبع شراء"}
    return {"triggered":False}

def strat_sr(df):
    price  = df['close'].iloc[-1]
    levels = list(df['high'].iloc[-SR_LOOKBACK:]) + list(df['low'].iloc[-SR_LOOKBACK:])
    zones, used = [], [False]*len(levels)
    for i,lv in enumerate(levels):
        if used[i]: continue
        cl = [lv]
        for j,lv2 in enumerate(levels):
            if i!=j and not used[j] and abs(lv-lv2)/max(lv,1e-10)<SR_ZONE_PCT*2:
                cl.append(lv2); used[j]=True
        if len(cl)>=2: zones.append(np.mean(cl))
        used[i]=True
    for z in zones:
        if abs(price-z)/max(z,1e-10) < SR_ZONE_PCT:
            if price>z: return {"triggered":True,"direction":"LONG","score":2,"detail":f"🧲 دعم {z:.4f}"}
            else:       return {"triggered":True,"direction":"SHORT","score":2,"detail":f"🧱 مقاومة {z:.4f}"}
    return {"triggered":False}

def strat_vp(df):
    close,vol = df['close'], df['volume']
    p = close.iloc[-1]
    lo,hi = close.min(), close.max()
    if hi==lo: return {"triggered":False}
    bins = np.linspace(lo,hi,VP_BINS+1)
    vb   = np.zeros(VP_BINS)
    for i in range(len(df)):
        idx = min(int((close.iloc[i]-lo)/(hi-lo)*VP_BINS), VP_BINS-1)
        vb[idx] += vol.iloc[i]
    poc_idx   = int(np.argmax(vb))
    poc_price = (bins[poc_idx]+bins[poc_idx+1])/2
    poc_pct   = abs(p-poc_price)/max(poc_price,1e-10)
    tv=vb.sum(); va=0; li=hi_=poc_idx
    while va/max(tv,1)<0.70 and (li>0 or hi_<VP_BINS-1):
        al=vb[li-1] if li>0 else 0; ah=vb[hi_+1] if hi_<VP_BINS-1 else 0
        if al>=ah and li>0: li-=1; va+=al
        elif hi_<VP_BINS-1: hi_+=1; va+=ah
        else: break
    va_lo=(bins[li]+bins[li+1])/2; va_hi=(bins[hi_]+bins[hi_+1])/2
    if p<va_lo and poc_pct<0.05:
        return {"triggered":True,"direction":"LONG","score":1,"detail":f"📦 VP من أسفل (POC={poc_price:.4f})"}
    if p>va_hi and poc_pct<0.05:
        return {"triggered":True,"direction":"SHORT","score":1,"detail":f"📦 VP من أعلى (POC={poc_price:.4f})"}
    return {"triggered":False}

def strat_divergence(df, lb=14):
    close = df['close']
    rsi   = ta.momentum.RSIIndicator(close,14).rsi()
    lp    = close.iloc[-lb:]; lr = rsi.iloc[-lb:]
    p_min,p_max = lp.min(),lp.max()
    r_min,r_max = lr.min(),lr.max()
    cp,cr = close.iloc[-1], rsi.iloc[-1]
    bull = abs(cp-p_min)/max(p_min,1e-10)<0.02 and cr>r_min+5 and cr<50
    bear = abs(cp-p_max)/max(p_max,1e-10)<0.02 and cr<r_max-5 and cr>50
    if bull: return {"triggered":True,"direction":"LONG","score":2,"detail":f"🔀 تباعد إيجابي RSI={cr:.1f}"}
    if bear: return {"triggered":True,"direction":"SHORT","score":2,"detail":f"🔀 تباعد سلبي RSI={cr:.1f}"}
    return {"triggered":False}

def strat_candle(df):
    o,h,l,c   = df['open'].iloc[-1],df['high'].iloc[-1],df['low'].iloc[-1],df['close'].iloc[-1]
    po,pc      = df['open'].iloc[-2],df['close'].iloc[-2]
    body       = abs(c-o); pb = abs(pc-po)
    cr         = h-l
    if cr==0: return {"triggered":False}
    us,ls = h-max(o,c), min(o,c)-l
    if ls>body*2 and us<body*0.5 and c>o and ls/cr>0.6:
        return {"triggered":True,"direction":"LONG","score":1,"detail":"🔨 Hammer"}
    if us>body*2 and ls<body*0.5 and c<o and us/cr>0.6:
        return {"triggered":True,"direction":"SHORT","score":1,"detail":"⭐ Shooting Star"}
    if pc>po and c>o and c>po and o<pc and body>pb*1.0:
        return {"triggered":True,"direction":"LONG","score":2,"detail":"🕯️ Bullish Engulfing"}
    if pc<po and c<o and c<po and o>pc and body>pb*1.0:
        return {"triggered":True,"direction":"SHORT","score":2,"detail":"🕯️ Bearish Engulfing"}
    if body/cr<0.08:
        e10=ta.trend.EMAIndicator(df['close'],10).ema_indicator().iloc[-1]
        if c>e10*1.01:
            return {"triggered":True,"direction":"SHORT","score":1,"detail":"🌀 Doji انعكاس"}
    return {"triggered":False}

# ── خريطة الاستراتيجيات ──
STRATEGIES = {
    "whale":  (strat_whale,      "🐋 حيتان"),
    "rsi":    (strat_rsi_macd,   "📊 RSI/MACD"),
    "break":  (strat_breakout,   "💥 اختراق"),
    "trend":  (strat_trend,      "📈 ترند"),
    "bb":     (strat_bb,         "📉 بولينجر"),
    "stoch":  (strat_stoch,      "⚡ Stoch RSI"),
    "sr":     (strat_sr,         "🧲 دعم/مقاومة"),
    "vp":     (strat_vp,         "📦 VP"),
    "div":    (strat_divergence, "🔀 تباعد"),
    "candle": (strat_candle,     "🕯️ شموع"),
}

# ============================================================
# 🎯 حساب المستويات ATR
# ============================================================
def calc_levels(df, direction):
    atr   = ta.volatility.AverageTrueRange(df['high'],df['low'],df['close'],14
                ).average_true_range().iloc[-1]
    price = df['close'].iloc[-1]
    if direction=="LONG":
        return price, price-atr*1.5, price+atr*1.5, price+atr*3.0, price+atr*5.0
    else:
        return price, price+atr*1.5, price-atr*1.5, price-atr*3.0, price-atr*5.0

# ============================================================
# 🔍 تحليل عملة
# ============================================================
def analyze(df: pd.DataFrame,
            min_score=MIN_SCORE,
            min_conf=MIN_CONFIDENCE) -> dict | None:

    results = {}
    for key,(fn,_) in STRATEGIES.items():
        try:
            results[key] = fn(df)
        except Exception as e:
            log.debug(f"Strategy {key}: {e}")
            results[key] = {"triggered":False}

    triggered = {k:v for k,v in results.items() if v["triggered"]}
    if not triggered:
        return None

    long_s  = sum(v["score"] for v in triggered.values() if v["direction"]=="LONG")
    short_s = sum(v["score"] for v in triggered.values() if v["direction"]=="SHORT")
    if long_s == short_s:
        return None

    direction     = "LONG" if long_s>short_s else "SHORT"
    dir_t         = {k:v for k,v in triggered.items() if v["direction"]==direction}
    total_score   = sum(v["score"] for v in dir_t.values())
    dir_ratio     = total_score / max(long_s+short_s, 1)
    grade, conf   = compute_grade_confidence(total_score, len(dir_t), dir_ratio)

    if total_score<min_score or conf<min_conf:
        return None

    strat_names = "\n".join(f"  ✅ {STRATEGIES[k][1]}" for k in dir_t)
    details     = " | ".join(v["detail"] for v in dir_t.values())

    return {
        "direction":  direction,
        "score":      total_score,
        "grade":      grade,
        "confidence": conf,
        "strategies": strat_names,
        "details":    details,
    }

# ============================================================
# 🔄 مراقبة الصفقات بالسعر الحالي
# ============================================================
def monitor_trade(symbol: str, current_price: float,
                  active_trades: dict) -> None:
    trade = active_trades.get(symbol)
    if not trade:
        return

    direction = trade['direction']
    entry     = trade['entry']
    grade     = trade.get('grade','C')
    conf      = trade.get('confidence',0)

    # وقف الخسارة
    sl_hit = ((direction=="LONG"  and current_price<=trade['sl']) or
              (direction=="SHORT" and current_price>=trade['sl']))
    if sl_hit:
        send_telegram(fmt_sl(symbol, current_price))
        close_trade(symbol)
        del active_trades[symbol]
        log.info(f"🔴 SL hit: {symbol}")
        return

    # TP1
    if not trade['tp1_hit']:
        hit = ((direction=="LONG" and current_price>=trade['tp1']) or
               (direction=="SHORT" and current_price<=trade['tp1']))
        if hit:
            trade['tp1_hit'] = True
            update_tp_hit(symbol, 1)
            send_telegram(fmt_tp(symbol, 1, current_price, grade, conf))
            update_trade_sl(symbol, entry)
            trade['sl'] = entry
            log.info(f"✅ TP1 hit: {symbol}")
    # TP2
    if trade['tp1_hit'] and not trade['tp2_hit']:
        hit = ((direction=="LONG" and current_price>=trade['tp2']) or
               (direction=="SHORT" and current_price<=trade['tp2']))
        if hit:
            trade['tp2_hit'] = True
            update_tp_hit(symbol, 2)
            send_telegram(fmt_tp(symbol, 2, current_price, grade, conf))
            log.info(f"✅ TP2 hit: {symbol}")
    # TP3
    if trade['tp2_hit']:
        hit = ((direction=="LONG" and current_price>=trade['tp3']) or
               (direction=="SHORT" and current_price<=trade['tp3']))
        if hit:
            send_telegram(fmt_tp(symbol, 3, current_price, grade, conf))
            close_trade(symbol)
            del active_trades[symbol]
            log.info(f"✅ TP3 hit: {symbol} — صفقة مكتملة!")

# ============================================================
# 🌐 WEBSOCKET ENGINE
# ============================================================
class WhaleRadarWS:

    def __init__(self):
        self.client         = None
        self.twm            = None                    # ThreadedWebsocketManager
        self.candle_store   = CandleStore()
        self.signal_manager = SignalManager()
        self.active_trades  : dict = {}
        self.symbols        : list = []
        self._analyze_lock  = threading.Lock()
        self._start_time    = datetime.now()
        self._last_health   = datetime.now()
        self._total_signals = 0
        self._streams       = []                      # stream keys

    # ── الاتصال ──────────────────────────────────────────
    def connect(self):
        self.client = Client(BINANCE_API_KEY, BINANCE_SECRET_KEY)
        self.client.ping()
        log.info("✅ Binance متصل")

    # ── جلب أفضل العملات ────────────────────────────────
    def fetch_symbols(self):
        tickers = self.client.get_ticker()
        pairs = [
            t for t in tickers
            if t['symbol'].endswith('USDT')
            and float(t['quoteVolume']) > 500_000
            and t['symbol'] not in ('USDCUSDT','BUSDUSDT','TUSDUSDT','USDTUSDT')
        ]
        pairs.sort(key=lambda x: float(x['quoteVolume']), reverse=True)
        self.symbols = [t['symbol'] for t in pairs[:TOP_SYMBOLS_COUNT]]
        log.info(f"📋 {len(self.symbols)} عملة مراقبة")
        return self.symbols

    # ── تحميل الشموع التاريخية ───────────────────────────
    def preload_candles(self):
        """تحميل أول 100 شمعة لكل عملة قبل بدء WebSocket"""
        log.info("⏳ تحميل الشموع التاريخية...")
        send_telegram(
            f"⏳ <b>تحميل البيانات...</b>\n"
            f"جاري تحميل {len(self.symbols)} عملة × 100 شمعة"
        )
        for i, sym in enumerate(self.symbols):
            try:
                klines = self.client.get_klines(
                    symbol=sym, interval=KLINE_INTERVAL, limit=CANDLES_REQUIRED
                )
                for k in klines:
                    candle = {
                        'time':   k[0],
                        'open':   k[1], 'high': k[2],
                        'low':    k[3], 'close': k[4],
                        'volume': k[5]
                    }
                    self.candle_store.update(sym, candle)
                if (i+1) % 10 == 0:
                    log.info(f"  📥 {i+1}/{len(self.symbols)} محملة")
                time.sleep(0.1)   # تجنب rate limit
            except Exception as e:
                log.error(f"❌ preload {sym}: {e}")

        ready = self.candle_store.symbol_count()
        log.info(f"✅ {ready} عملة جاهزة للتحليل")

    # ── معالج WebSocket ──────────────────────────────────
    def _on_kline(self, msg: dict):
        try:
            if msg.get('e') == 'error':
                log.error(f"WS error: {msg}")
                return

            k   = msg['k']
            sym = msg['s']

            candle = {
                'time':   k['t'],
                'open':   k['o'], 'high': k['h'],
                'low':    k['l'], 'close': k['c'],
                'volume': k['v']
            }
            self.candle_store.update(sym, candle)

            # مراقبة الصفقات المفتوحة بكل تحديث سعر
            if sym in self.active_trades:
                monitor_trade(sym, float(k['c']), self.active_trades)

            # التحليل فقط عند إغلاق الشمعة
            if not k['x']:
                return

            log.debug(f"🕯️ شمعة مغلقة: {sym} @ {k['c']}")
            self._run_analysis(sym)

        except Exception as e:
            log.error(f"❌ _on_kline: {e}")

    # ── تشغيل التحليل ────────────────────────────────────
    def _run_analysis(self, symbol: str):
        # منع التحليل المتزامن لنفس العملة
        with self._analyze_lock:
            df = self.candle_store.get_df(symbol)
            if df is None:
                return

            # محاولة بالمعايير الطبيعية أولاً
            result = analyze(df, MIN_SCORE, MIN_CONFIDENCE)

            # إذا فشل → وضع الطوارئ
            if result is None:
                result = analyze(df, MIN_SCORE_RELAXED, MIN_CONF_RELAXED)
                if result:
                    log.info(f"⚠️ {symbol} إشارة طوارئ (معايير مخففة)")

            if result is None:
                return

            # التحقق من الـ cooldown
            if not self.signal_manager.can_signal(symbol):
                return

            # حساب المستويات
            try:
                price, sl, tp1, tp2, tp3 = calc_levels(df, result['direction'])
            except Exception as e:
                log.error(f"❌ calc_levels {symbol}: {e}")
                return

            # بناء وإرسال الرسالة
            msg = fmt_signal(
                symbol, result['direction'], price,
                sl, tp1, tp2, tp3,
                result['score'], result['grade'], result['confidence'],
                result['strategies'], result['details']
            )
            sent = send_telegram(msg)
            if not sent:
                return

            # حفظ وتسجيل
            self.signal_manager.record(symbol)
            save_active_trade(
                symbol, price, result['direction'], sl, tp1, tp2, tp3,
                result['strategies'], result['score'],
                result['grade'], result['confidence']
            )
            save_signal_history(
                symbol, "WS_SIGNAL", result['direction'], price,
                result['score'], result['grade'],
                result['confidence'], result['strategies']
            )
            self.active_trades[symbol] = {
                "entry": price, "sl": sl,
                "tp1": tp1, "tp2": tp2, "tp3": tp3,
                "direction": result['direction'],
                "tp1_hit": False, "tp2_hit": False,
                "grade": result['grade'], "confidence": result['confidence'],
                "score": result['score'],
            }
            self._total_signals += 1
            log.info(
                f"🔔 إشارة #{self._total_signals}: {symbol} "
                f"{result['direction']} {result['grade']} "
                f"{result['confidence']}% نقاط={result['score']}"
            )

    # ── بدء الـ WebSocket ────────────────────────────────
    def start_streams(self):
        self.twm = ThreadedWebsocketManager(
            api_key=BINANCE_API_KEY,
            api_secret=BINANCE_SECRET_KEY
        )
        self.twm.start()

        # Binance يسمح بـ 200 stream في وقت واحد
        # نقسم العملات على streams مجمّعة (كل stream = عملة واحدة)
        for sym in self.symbols:
            key = self.twm.start_kline_socket(
                callback=self._on_kline,
                symbol=sym,
                interval=KLINE_INTERVAL
            )
            self._streams.append(key)
            time.sleep(0.05)

        log.info(f"✅ {len(self._streams)} WebSocket stream نشط")

    # ── إيقاف ────────────────────────────────────────────
    def stop(self):
        if self.twm:
            self.twm.stop()
        log.info("🛑 WebSocket أُوقف")

    # ── رسالة الصحة الدورية ──────────────────────────────
    def health_check(self):
        now = datetime.now()
        if (now - self._last_health).seconds < HEALTH_INTERVAL:
            return
        self._last_health = now
        uptime = str(now - self._start_time).split('.')[0]
        ready  = self.candle_store.symbol_count()
        sig_h  = self.signal_manager.signals_this_hour()
        send_telegram(
            f"💚 <b>رادار الحيتان — نبضة صحة</b>\n\n"
            f"⏱️ وقت التشغيل: <code>{uptime}</code>\n"
            f"📡 عملات مراقبة: <code>{ready}/{len(self.symbols)}</code>\n"
            f"🔔 إجمالي الإشارات: <code>{self._total_signals}</code>\n"
            f"📊 إشارات الساعة: <code>{sig_h}/{MAX_SIGNALS_HOUR}</code>\n"
            f"💼 صفقات مفتوحة: <code>{len(self.active_trades)}</code>\n"
            f"🕐 {now.strftime('%H:%M:%S')}"
        )

    # ── الحلقة الرئيسية ──────────────────────────────────
    def run(self):
        setup_database()
        self.connect()
        self.active_trades = load_active_trades()
        log.info(f"📂 صفقات مفتوحة: {len(self.active_trades)}")

        self.fetch_symbols()
        self.preload_candles()

        send_telegram(
            "🐋 <b>رادار الحيتان v4.0 WebSocket</b>\n\n"
            f"⚡ <b>اتصال لحظي نشط!</b>\n"
            f"📡 {len(self.symbols)} عملة مراقبة في الوقت الفعلي\n"
            f"🔟 10 استراتيجيات تعمل على كل شمعة\n"
            f"⏱️ الإشارات تصل فور إغلاق الشمعة ({KLINE_INTERVAL})\n\n"
            "🟢 <b>الرادار جاهز — إشارات قادمة!</b>"
        )

        self.start_streams()

        # حلقة المراقبة الرئيسية (خفيفة — WebSocket يعمل في الخلفية)
        try:
            while True:
                time.sleep(30)
                self.health_check()

        except KeyboardInterrupt:
            log.info("🛑 توقف يدوي")
            send_telegram(
                f"🛑 <b>رادار الحيتان أُوقف</b>\n"
                f"📊 إجمالي الإشارات: {self._total_signals}\n"
                f"⏱️ وقت التشغيل: {str(datetime.now()-self._start_time).split('.')[0]}"
            )
            self.stop()


# ============================================================
# 🚀 تشغيل
# ============================================================
if __name__ == "__main__":
    radar = WhaleRadarWS()
    radar.run()
