CREATE TABLE IF NOT EXISTS active_trades (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) UNIQUE NOT NULL,
    entry_price DECIMAL(20,8),
    direction VARCHAR(10),
    sl DECIMAL(20,8),
    tp1 DECIMAL(20,8),
    tp2 DECIMAL(20,8),
    tp3 DECIMAL(20,8),
    tp1_hit BOOLEAN DEFAULT FALSE,
    tp2_hit BOOLEAN DEFAULT FALSE,
    strategy TEXT,
    score INTEGER DEFAULT 0,
    grade VARCHAR(5) DEFAULT 'C',
    confidence INTEGER DEFAULT 0,
    status VARCHAR(10) DEFAULT 'OPEN',
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS signal_history (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20),
    signal_type VARCHAR(50),
    direction VARCHAR(10),
    price DECIMAL(20,8),
    score INTEGER,
    grade VARCHAR(5),
    confidence INTEGER,
    strategies TEXT,
    sent_at TIMESTAMP DEFAULT NOW()
);
