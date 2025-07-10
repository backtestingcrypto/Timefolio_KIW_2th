import pandas as pd
from pykrx import stock
from datetime import datetime
import sys
import os

# ✅ 경로 설정
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from shared.connect_postgresql import get_connection
from shared.It_config import schema_name, table_name, full_table_name

def get_tickers():
    """sector_code가 'It'인 종목들의 ticker_numeric과 sector_code를 가져옴"""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT ticker_numeric, sector_code FROM kr_stock_data WHERE sector_code = 'IT';")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

def save_ohlcv_by_date(start_date: str, end_date: str):
    tickers_info = get_tickers()
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    conn = get_connection()
    cur = conn.cursor()

    # ✅ 스키마 생성 (없으면)
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
    conn.commit()

    # ✅ 테이블 생성 (없으면)
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {full_table_name} (
            date DATE,
            ticker_numeric TEXT,
            sector_code TEXT,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            volume BIGINT,
            PRIMARY KEY (date, ticker_numeric)
        );
    """)
    conn.commit()

    for ticker, sector_code in tickers_info:
        print(f"\n📈 종목 {ticker} 처리 중...")

        try:
            df = stock.get_market_ohlcv_by_date(start_date, end_date, ticker)
        except Exception as e:
            print(f"❌ {ticker} OHLCV 조회 실패: {e}")
            continue

        if df.empty:
            print(f"⚠️ {ticker}: 데이터 없음")
            continue

        df.reset_index(inplace=True)

        for _, row in df.iterrows():
            date = row["날짜"].date()
            open_ = float(row["시가"])
            high = float(row["고가"])
            low = float(row["저가"])
            close = float(row["종가"])
            volume = int(row["거래량"])

            # ✅ INSERT or UPDATE
            cur.execute(f"""
                INSERT INTO {full_table_name} (date, ticker_numeric, sector_code, open, high, low, close, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (date, ticker_numeric) DO UPDATE SET
                    sector_code = EXCLUDED.sector_code,
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume;
            """, (date, ticker, sector_code, open_, high, low, close, volume))

        conn.commit()
        print(f"✅ {ticker} 저장 완료")

    cur.close()
    conn.close()
    print(f"\n🎉 전체 {full_table_name} OHLCV 저장 완료")

if __name__ == "__main__":
    today = datetime.today().strftime("%Y-%m-%d")
    save_ohlcv_by_date("2018-01-01", today)
