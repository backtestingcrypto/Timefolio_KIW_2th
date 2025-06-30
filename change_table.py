from shared.connect_postgresql import get_connection

def alter_strategy_breakout_result_table():
    alter_query = """
    ALTER TABLE strategy_breakout_result
    ADD COLUMN IF NOT EXISTS daily_market_cap NUMERIC;
    """

    conn = get_connection()
    cur = conn.cursor()
    cur.execute(alter_query)
    conn.commit()
    cur.close()
    conn.close()
    print("✅ strategy_breakout_result 테이블에 컬럼 추가 완료")

if __name__ == "__main__":
    alter_strategy_breakout_result_table()
