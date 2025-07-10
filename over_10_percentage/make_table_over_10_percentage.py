import psycopg2
import sys
import os

# ✅ shared 디렉토리 경로 설정
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from shared.connect_postgresql import get_connection

def create_high_spike_analysis_table():
    query = """
    CREATE TABLE IF NOT EXISTS over_10_percentage (
        ticker TEXT NOT NULL,
        date DATE NOT NULL,
        prev_close NUMERIC,
        high_pct NUMERIC,
        low_pct NUMERIC,
        close_pct NUMERIC,
        open_pct NUMERIC,
        next_high_pct NUMERIC,
        next_low_pct NUMERIC,
        next_open_pct NUMERIC,
        next_close_pct NUMERIC,
        rsi_14 NUMERIC,
        bb_upper_break BOOLEAN,
        vol_ma_break BOOLEAN,
        trade_amount_change NUMERIC,
        ma_order TEXT,
        daily_market_cap NUMERIC,
        PRIMARY KEY (ticker, date)
    );
    """

    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()
        cur.close()
        conn.close()
        print("✅ high_spike_analysis 테이블이 성공적으로 생성되었습니다.")
    except Exception as e:
        print("❌ 테이블 생성 중 오류 발생:", e)

if __name__ == "__main__":
    create_high_spike_analysis_table()
