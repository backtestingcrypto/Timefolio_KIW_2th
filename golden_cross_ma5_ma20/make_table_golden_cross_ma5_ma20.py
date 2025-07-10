import psycopg2
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from shared.connect_postgresql import get_connection

def create_golden_cross_table():
    conn = get_connection()
    cur = conn.cursor()

    create_table_query = """
    CREATE TABLE IF NOT EXISTS golden_cross_ma5_ma20 (
        ticker TEXT NOT NULL,
        entry_date DATE NOT NULL,

        rsi_14 NUMERIC,
        rsi_change NUMERIC,
        rsi_change_pct NUMERIC,

        bb_upper_break BOOLEAN,
        vol_ma_break BOOLEAN,
        trade_amount_change NUMERIC,
        trade_amount NUMERIC,

        avg_trade_amount_5d NUMERIC,       -- ✅ 추가된 컬럼
        daily_market_cap NUMERIC,          -- ✅ 추가된 컬럼

        next_open_pct NUMERIC,
        next_close_pct NUMERIC,
        next_high_pct NUMERIC,
        next_low_pct NUMERIC,

        pct_close_vs_prev_close NUMERIC,
        pct_high_vs_prev_close NUMERIC,

        ma_order TEXT,

        PRIMARY KEY (ticker, entry_date)
    );
    """

    try:
        cur.execute(create_table_query)
        conn.commit()
        print("✅ 테이블 'golden_cross_ma5_ma20' 생성 완료!")
    except Exception as e:
        print("❌ 테이블 생성 중 오류 발생:", e)
        conn.rollback()
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    create_golden_cross_table()
