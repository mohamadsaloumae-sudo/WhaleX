import os
import psycopg2
import time
import requests
import warnings
import telebot
from psycopg2.extras import RealDictCursor
from cryptography.fernet import Fernet
from dotenv import load_dotenv

load_dotenv()
warnings.filterwarnings("ignore")

ADMIN_ID = int(os.getenv("ADMIN_ID"))
BOT_TOKEN = os.getenv("BOT_TOKEN")
F_CHANNEL_ID = int(os.getenv("F_CHANNEL_ID"))
S_CHANNEL_ID = int(os.getenv("S_CHANNEL_ID"))
M_CHANNEL_ID = int(os.getenv("M_CHANNEL_ID"))
FREE_CHANNEL_ID = os.getenv("FREE_CHANNEL_ID")

BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_SECRET_KEY = os.getenv("BINANCE_SECRET_KEY")

FEES_COLLECTOR_WALLET = os.getenv("FEES_COLLECTOR_WALLET")
WALLET_ADDRESS = os.getenv("WALLET_ADDRESS")
TRONGRID_API_KEY = os.getenv("TRONGRID_API_KEY")

ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY").encode()
cipher_suite = Fernet(ENCRYPTION_KEY)

def encrypt_key(private_key):
    return cipher_suite.encrypt(private_key.encode()).decode()

def decrypt_key(encrypted_key):
    return cipher_suite.decrypt(encrypted_key.encode()).decode()

DATABASE_URL = os.getenv("DATABASE_URL")

def get_connection():
    try: return psycopg2.connect(DATABASE_URL, sslmode='require', connect_timeout=15)
    except: return None

def calculate_system_fee(margin_amount, leverage_str):
    try:
        leverage = int(''.join(filter(str.isdigit, leverage_str)))
        position_size = margin_amount * leverage
        return round(position_size * 0.001, 4)
    except:
        return round(margin_amount * 0.01, 4)

def save_user_to_db(user_id, username, address, encrypted_key):
    conn = get_connection()
    if not conn: return False
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO user_wallets (user_id, username, evm_address, encrypted_key, status)
            VALUES (%s, %s, %s, %s, 'No Active')
            ON CONFLICT (user_id) DO UPDATE SET username = EXCLUDED.username
        """, (user_id, username, address, encrypted_key))
        conn.commit()
        return True
    except: return False
    finally: conn.close()

def get_user_wallet_details(user_id):
    conn = get_connection()
    if not conn: return None
    try:
        cur = conn.cursor()
        cur.execute("SELECT evm_address, encrypted_key FROM user_wallets WHERE user_id = %s", (user_id,))
        return cur.fetchone()
    except: return None
    finally: conn.close()

def get_user_sol_details(user_id):
    conn = get_connection()
    if not conn: return None
    try:
        cur = conn.cursor()
        cur.execute("SELECT sol_address FROM user_wallets WHERE user_id = %s", (user_id,))
        res = cur.fetchone()
        return res if res and res[0] else None
    except: return None
    finally: conn.close()

def log_new_whale(tx_hash, symbol, usd_val, sender, source_type, tier="TIER_1", direction="LONG"):
    conn = get_connection()
    if not conn: return
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO signals_queue (tx_hash, symbol, usd_val, sender, sender_name, source_type, tier, direction, titanium_decision)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'APPROVED') ON CONFLICT (tx_hash) DO NOTHING
        """, (tx_hash, symbol, usd_val, sender, sender, source_type, tier, direction))
        conn.commit()
    except: pass
    finally: conn.close()

def init_all_dbs():
    conn = get_connection()
    if not conn: return
    cursor = conn.cursor()
    
    commands = [
        "ALTER TABLE user_wallets ADD COLUMN IF NOT EXISTS sol_address TEXT;",
        "ALTER TABLE user_wallets ADD COLUMN IF NOT EXISTS current_balance REAL DEFAULT 0.0;",
        "ALTER TABLE user_wallets ADD COLUMN IF NOT EXISTS system_balance REAL DEFAULT 0.0;",
        "ALTER TABLE user_wallets ADD COLUMN IF NOT EXISTS plan_type TEXT DEFAULT 'NONE';",
        "ALTER TABLE user_wallets ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'No Active';",
        "ALTER TABLE user_wallets ADD COLUMN IF NOT EXISTS encrypted_key TEXT;",
        "ALTER TABLE user_wallets ADD COLUMN IF NOT EXISTS active_dex TEXT;",
        "ALTER TABLE user_wallets ADD COLUMN IF NOT EXISTS auto_budget REAL DEFAULT 0.0;",
        "ALTER TABLE user_wallets ADD COLUMN IF NOT EXISTS auto_trade_count INTEGER DEFAULT 0;",
        "ALTER TABLE active_trades ADD COLUMN IF NOT EXISTS user_id BIGINT;",
        "ALTER TABLE active_trades ADD COLUMN IF NOT EXISTS pair TEXT;",
        "ALTER TABLE active_trades ADD COLUMN IF NOT EXISTS current REAL;",
        "ALTER TABLE active_trades ADD COLUMN IF NOT EXISTS pnl REAL;"
    ]
    
    for cmd in commands:
        try:
            cursor.execute(cmd)
            conn.commit()
        except: conn.rollback()

    cursor.execute('''CREATE TABLE IF NOT EXISTS user_wallets (user_id BIGINT PRIMARY KEY, username TEXT, evm_address TEXT, encrypted_key TEXT, plan_type TEXT DEFAULT 'NONE', status TEXT DEFAULT 'No Active', created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS signals_queue (tx_hash TEXT PRIMARY KEY, symbol TEXT, usd_val REAL, sender TEXT, sender_name TEXT, source_type TEXT, tier TEXT, direction TEXT, titanium_decision TEXT, timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS active_trades (symbol TEXT PRIMARY KEY, entry_price REAL, direction TEXT, sl REAL, tp1 REAL, tp2 REAL, tp3 REAL, status TEXT DEFAULT 'OPEN', start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS treasury_logs (id SERIAL PRIMARY KEY, user_id BIGINT, amount REAL, tx_hash TEXT, fee_type TEXT, timestamp DOUBLE PRECISION)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS processed_txs (tx_hash TEXT PRIMARY KEY, timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS manual_orders (id SERIAL PRIMARY KEY, user_id BIGINT, symbol TEXT, direction TEXT, leverage INTEGER DEFAULT 1, status TEXT DEFAULT 'PENDING', tx_hash TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    
    conn.commit()
    conn.close()

def save_active_trade(symbol, entry_price, direction, sl, tp1, tp2, tp3):
    conn = get_connection()
    if not conn: return
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO active_trades (symbol, entry_price, direction, sl, tp1, tp2, tp3, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, 'OPEN')
            ON CONFLICT (symbol) DO UPDATE SET entry_price = EXCLUDED.entry_price, status = 'OPEN'
        """, (symbol, entry_price, direction, sl, tp1, tp2, tp3))
        conn.commit()
    except: pass
    finally: conn.close()

def load_active_trades():
    conn = get_connection()
    if not conn: return {}
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT * FROM active_trades WHERE status = 'OPEN'")
        rows = cur.fetchall()
        
        trades_dict = {}
        for r in rows:
            trades_dict[r['symbol']] = {
                "entry": r['entry_price'], "sl": r['sl'], "tp1": r['tp1'], "tp2": r['tp2'], "tp3": r['tp3'],
                "direction": r['direction'], "tp1_hit": False, "tp2_hit": False, "extended": False, "avg_vol": 500000
            }
        return trades_dict
    except: return {}
    finally: conn.close()

def update_trade_sl(symbol, new_sl):
    conn = get_connection()
    if not conn: return
    try:
        cur = conn.cursor()
        cur.execute("UPDATE active_trades SET sl = %s WHERE symbol = %s", (new_sl, symbol))
        conn.commit()
    except: pass
    finally: conn.close()

def close_active_trade(symbol):
    conn = get_connection()
    if not conn: return
    try:
        cur = conn.cursor()
        cur.execute("UPDATE active_trades SET status = 'CLOSED' WHERE symbol = %s", (symbol,))
        conn.commit()
    except: pass
    finally: conn.close()

def create_manual_order(user_id, symbol, direction, leverage=1):
    conn = get_connection()
    if not conn: return False
    try:
        cur = conn.cursor()
        cur.execute("INSERT INTO manual_orders (user_id, symbol, direction, leverage) VALUES (%s, %s, %s, %s)", (user_id, symbol, direction, leverage))
        conn.commit()
        return True
    except: return False
    finally: conn.close()

def get_pending_manual_orders():
    conn = get_connection()
    if not conn: return []
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, user_id, symbol, direction, leverage FROM manual_orders WHERE status = 'PENDING' LIMIT 10")
        return cur.fetchall()
    except: return []
    finally: conn.close()

def mark_manual_order_executed(order_id, tx_hash):
    conn = get_connection()
    if not conn: return False
    try:
        cur = conn.cursor()
        cur.execute("UPDATE manual_orders SET status = 'EXECUTED', tx_hash = %s WHERE id = %s", (tx_hash, order_id))
        conn.commit()
        return True
    except: return False
    finally: conn.close()

def get_user_system_balance(user_id):
    conn = get_connection()
    if not conn: return 0.0
    try:
        cur = conn.cursor()
        cur.execute("SELECT system_balance FROM user_wallets WHERE user_id = %s", (user_id,))
        res = cur.fetchone()
        return res[0] if res else 0.0
    except: return 0.0
    finally: conn.close()

def update_subscription_status(user_id, plan, duration, new_balance):
    conn = get_connection()
    if not conn: return False
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE user_wallets 
            SET system_balance = %s, plan_type = %s, status = 'ACTIVE' 
            WHERE user_id = %s
        """, (new_balance, plan, user_id))
        conn.commit()
        return True
    except: return False
    finally: conn.close()

def get_wallet_stats(user_id):
    return {'total': 0.00, 'pnl': 0.00, 'pnl_perc': 0.00}

def get_all_balances(user_id):
    return {'eth': 0.00, 'sol': 0.00, 'bnb': 0.00, 'base': 0.00, 'arb': 0.00}

def execute_blockchain_transfer(user_id, asset, target_addr, amount):
    return True

def open_dex_swap(call):
    bot = telebot.TeleBot(BOT_TOKEN)
    bot.answer_callback_query(call.id, "سيتم فتح واجهة المبادلة اللامركزية قريباً.", show_alert=True)

def link_user_to_dex(user_id, dex_name):
    conn = get_connection()
    if not conn: return False
    try:
        cur = conn.cursor()
        cur.execute("UPDATE user_wallets SET active_dex = %s WHERE user_id = %s", (dex_name, user_id))
        conn.commit()
        return True
    except: return False
    finally: conn.close()

def save_full_auto_settings(user_id, budget, count):
    conn = get_connection()
    if not conn: return False
    try:
        cur = conn.cursor()
        cur.execute("UPDATE user_wallets SET auto_budget = %s, auto_trade_count = %s WHERE user_id = %s", (budget, count, user_id))
        conn.commit()
        return True
    except: return False
    finally: conn.close()

def get_user_active_trades(user_id):
    conn = get_connection()
    if not conn: return []
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT * FROM active_trades WHERE user_id = %s AND status = 'OPEN'", (user_id,))
        return cur.fetchall()
    except: return []
    finally: conn.close()

def get_all_active_user_addresses():
    conn = get_connection()
    if not conn: return {}
    try:
        cur = conn.cursor()
        cur.execute("SELECT evm_address, user_id FROM user_wallets WHERE evm_address IS NOT NULL")
        return {row[0].lower(): row[1] for row in cur.fetchall()}
    except: return {}
    finally: conn.close()

def is_tx_processed(tx_hash):
    conn = get_connection()
    if not conn: return True 
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM processed_txs WHERE tx_hash = %s", (tx_hash,))
        return bool(cur.fetchone())
    except: return True
    finally: conn.close()

def calculate_gas_reservation(amount):
    gas_fee = amount * 0.005
    tradeable = amount - gas_fee
    return round(tradeable, 4), round(gas_fee, 4)

def update_multi_chain_balance(user_id, chain, symbol, tradeable_amount):
    conn = get_connection()
    if not conn: return False
    try:
        cur = conn.cursor()
        cur.execute("UPDATE user_wallets SET current_balance = current_balance + %s WHERE user_id = %s", (tradeable_amount, user_id))
        conn.commit()
        return True
    except: return False
    finally: conn.close()

def log_transaction(user_id, tx_hash, amount, symbol, tx_type):
    conn = get_connection()
    if not conn: return
    try:
        cur = conn.cursor()
        cur.execute("INSERT INTO processed_txs (tx_hash) VALUES (%s) ON CONFLICT DO NOTHING", (tx_hash,))
        cur.execute("INSERT INTO treasury_logs (user_id, amount, tx_hash, fee_type, timestamp) VALUES (%s, %s, %s, %s, %s)", 
                    (user_id, amount, tx_hash, tx_type, time.time()))
        conn.commit()
    except: pass
    finally: conn.close()

if __name__ == "__main__":
    init_all_dbs()
