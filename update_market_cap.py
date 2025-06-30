import time
from pykrx import stock
from shared.connect_postgresql import get_connection
from pykrx import stock

# ✅ 가장 최근 거래일 (휴일, 주말에도 대응)
def get_latest_trading_day():
    return stock.get_nearest_business_day_in_a_week()

# ✅ 시가총액 가져오기
def get_market_cap_from_pykrx(ticker_numeric: str, date: str) -> int | None:
    try:
        df = stock.get_market_cap_by_ticker(date)
        if ticker_numeric in df.index:
            return int(df.loc[ticker_numeric]['시가총액'])
        else:
            print(f"⚠️ {ticker_numeric}: pykrx에서 시총 없음")
    except Exception as e:
        print(f"[예외] {ticker_numeric}: {e}")
    return None

# ✅ 전체 업데이트
def update_all_market_caps():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("SELECT ticker_numeric FROM kr_stock_data;")
    rows = cur.fetchall()
    tickers = [r[0].zfill(6) for r in rows]

    latest_date = get_latest_trading_day()
    print(f"▶ {latest_date} 기준 총 {len(tickers)}개 종목 시가총액 업데이트 시작")

    for ticker in tickers:
        cap = get_market_cap_from_pykrx(ticker, latest_date)
        if cap:
            cur.execute("""
                UPDATE kr_stock_data
                SET market_cap = %s
                WHERE ticker_numeric = %s;
            """, (cap, ticker))
            print(f"✅ {ticker}: {cap:,} 원")
        else:
            print(f"❌ {ticker}: 시총 없음")
        time.sleep(0.1)

    conn.commit()
    cur.close()
    conn.close()
    print("🎉 시가총액 업데이트 완료")

# ✅ 실행
if __name__ == "__main__":
    update_all_market_caps()
