from shared.connect_postgresql import get_connection

def create_strategy_breakout_table():
    query = """
    CREATE TABLE test (
        ticker TEXT NOT NULL,
        entry_date DATE NOT NULL,
        close_price NUMERIC,
        next_open_pct NUMERIC,
        next_high_pct NUMERIC,
        rsi_14 NUMERIC,
        bb_upper_break BOOLEAN,
        vol_ma_break BOOLEAN,
        price_ma_60 NUMERIC,        -- ✅ 변경된 이름
        price_ma_120 NUMERIC,       -- ✅ 변경된 이름
        trade_amount_change NUMERIC,
        PRIMARY KEY (ticker, entry_date)
    );

    """
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    cur.close()
    conn.close()
    print("✅ strategy_breakout_result 테이블 생성 완료")

if __name__ == "__main__":
    create_strategy_breakout_table()
