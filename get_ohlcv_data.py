import pandas as pd
from pykrx import stock
from datetime import datetime, timedelta
from shared.connect_postgresql import get_connection

def get_all_tickers():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT ticker_numeric FROM kr_stock_data;")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [row[0] for row in rows]

def save_ohlcv_by_date(start_date: str, end_date: str):
    tickers = get_all_tickers()

    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    conn = get_connection()
    cur = conn.cursor()

    for ticker in tickers:
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

            # INSERT or UPDATE
            cur.execute("""
                INSERT INTO daily_details_stocks (ticker, date, open, high, low, close, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ticker, date) DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume;
            """, (ticker, date, open_, high, low, close, volume))

        conn.commit()
        print(f"✅ {ticker} 저장 완료")

    cur.close()
    conn.close()
    print("\n🎉 전체 OHLCV 저장 완료")

if __name__ == "__main__":
    today = datetime.today().strftime("%Y-%m-%d")
    save_ohlcv_by_date("2018-01-01", today)
