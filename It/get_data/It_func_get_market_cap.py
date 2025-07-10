import pandas as pd
from tqdm import tqdm
from pykrx import stock
import sys
import os

# ✅ 경로 설정
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from shared.connect_postgresql import get_connection
from shared.It_config import schema_name, table_name, full_table_name


def get_tickers_and_date_range():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f"SELECT DISTINCT ticker_numeric FROM {full_table_name}")
    tickers = [row[0] for row in cur.fetchall()]

    cur.execute(f"SELECT MIN(date), MAX(date) FROM {full_table_name}")
    start_date, end_date = cur.fetchone()
    cur.close()
    conn.close()

    return tickers, start_date.strftime("%Y%m%d"), end_date.strftime("%Y%m%d")

def add_columns_if_not_exist():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f"ALTER TABLE {full_table_name} ADD COLUMN IF NOT EXISTS daily_market_cap BIGINT;")
    cur.execute(f"ALTER TABLE {full_table_name} ADD COLUMN IF NOT EXISTS trade_amount BIGINT;")
    conn.commit()
    cur.close()
    conn.close()

def update_market_cap_and_amount():
    add_columns_if_not_exist()
    tickers, start_date, end_date = get_tickers_and_date_range()
    conn = get_connection()
    cur = conn.cursor()

    for ticker in tqdm(tickers, desc=f"📈 {full_table_name} 테이블 시가총액/거래대금 갱신"):
        try:
            df = stock.get_market_cap(start_date, end_date, ticker)
        except Exception as e:
            print(f"❌ {ticker} 조회 실패: {e}")
            continue

        df = df.reset_index()
        df['ticker_numeric'] = ticker
        df['date'] = pd.to_datetime(df['날짜'])

        for _, row in df.iterrows():
            cur.execute(f"""
                UPDATE {full_table_name}
                SET daily_market_cap = %s,
                    trade_amount = %s
                WHERE ticker_numeric = %s AND date = %s
            """, (
                int(row['시가총액']),
                int(row['거래대금']),
                row['ticker_numeric'],
                row['date']
            ))

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ {full_table_name}의 시가총액 및 거래대금 업데이트 완료")

if __name__ == "__main__":
    update_market_cap_and_amount()
