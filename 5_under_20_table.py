from shared.connect_postgresql import get_connection

def create_strategy_contrarian_entry_table():
    create_query = """
    CREATE TABLE IF NOT EXISTS strategy_contrarian_entry (
        ticker TEXT NOT NULL,
        entry_date DATE NOT NULL,
        low NUMERIC,
        rsi_14 NUMERIC,
        bb_upper_break BOOLEAN,
        vol_ma_break BOOLEAN,
        trade_amount_change NUMERIC,
        PRIMARY KEY (ticker, entry_date)
    );
    """

    conn = get_connection()
    cur = conn.cursor()
    cur.execute(create_query)
    conn.commit()
    cur.close()
    conn.close()
    print("✅ strategy_contrarian_entry 테이블 생성 완료.")

if __name__ == "__main__":
    create_strategy_contrarian_entry_table()
