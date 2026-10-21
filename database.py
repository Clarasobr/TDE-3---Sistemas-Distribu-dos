import sqlite3

DB_NAME = "transacoes.db"

def init_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS transacoes (
            id TEXT PRIMARY KEY,
            cliente_id INTEGER,
            valor REAL,
            cidade TEXT,
            timestamp REAL,
            fraude_detectada TEXT
        )
    ''')
    conn.commit()
    conn.close()

def salvar_transacao(transacao, fraudes):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    fraude_str = ",".join(fraudes) if fraudes else "NENHUMA"
    
    cursor.execute('''
        INSERT INTO transacoes (id, cliente_id, valor, cidade, timestamp, fraude_detectada)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (
        transacao['id'],
        transacao['cliente_id'],
        transacao['valor'],
        transacao['cidade'],
        transacao['timestamp'],
        fraude_str
    ))
    conn.commit()
    conn.close()
