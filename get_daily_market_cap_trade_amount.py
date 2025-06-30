import pandas as pd
from tqdm import tqdm
from pykrx import stock
from shared.connect_postgresql import get_connection

def get_tickers_and_date_range():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT ticker FROM daily_details_stocks")
    tickers = [row[0] for row in cur.fetchall()]

    cur.execute("SELECT MIN(date), MAX(date) FROM daily_details_stocks")
    start_date, end_date = cur.fetchone()
    cur.close()
    conn.close()

    return tickers, start_date.strftime("%Y%m%d"), end_date.strftime("%Y%m%d")

def update_market_cap_and_amount():
    tickers, start_date, end_date = get_tickers_and_date_range()
    conn = get_connection()
    cur = conn.cursor()

    for ticker in tqdm(tickers, desc="📈 종목별 처리"):
        try:
            df = stock.get_market_cap(start_date, end_date, ticker)
        except Exception as e:
            print(f"❌ {ticker} 조회 실패: {e}")
            continue

        df = df.reset_index()
        df['ticker'] = ticker
        df['date'] = pd.to_datetime(df['날짜'])

        for _, row in df.iterrows():
            cur.execute("""
                UPDATE daily_details_stocks
                SET daily_market_cap = %s,
                    trade_amount = %s
                WHERE ticker = %s AND date = %s
            """, (row['시가총액'], row['거래대금'], row['ticker'], row['date']))

    conn.commit()
    cur.close()
    conn.close()
    print("✅ 모든 시가총액 및 거래대금 업데이트 완료")

if __name__ == "__main__":
    update_market_cap_and_amount()
