import pandas as pd
from pykrx import stock
from datetime import datetime, timedelta
from shared.connect_postgresql import get_connection

# 1. kr_stock_data에서 모든 ticker_numeric 가져오기
def get_all_tickers():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT ticker_numeric FROM kr_stock_data;")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [row[0] for row in rows]

# 2. 외국인 한도소진률을 일자별로 조회 및 저장
def save_foreign_limit_ratios(start_date: str, end_date: str):
    tickers = get_all_tickers()

    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    conn = get_connection()
    cur = conn.cursor()

    current_dt = start_dt
    while current_dt <= end_dt:
        if current_dt.weekday() >= 5:  # 주말 건너뜀
            current_dt += timedelta(days=1)
            continue

        date_str = current_dt.strftime("%Y%m%d")
        print(f"\n📅 {date_str} 처리 중...")

        try:
            df = stock.get_exhaustion_rates_of_foreign_investment(date_str)

            # 디버깅 출력
            print("📋 컬럼 목록:", df.columns.tolist())
            print(df.head(2))

            # 구버전 컬럼명 -> 한글 매핑
            column_map = {
                "FORN_LMT_EXHST_RT": "한도소진률",
                "LIST_SHRS": "상장주식수",
                "FORN_HD_QTY": "보유수량",
                "FORN_SHR_RT": "지분율",
                "FORN_ORD_LMT_QTY": "한도수량"
            }
            df.rename(columns=column_map, inplace=True)

            if df.empty or "한도소진률" not in df.columns:
                print(f"⚠️ {date_str}: '한도소진률' 컬럼 없음 또는 데이터 없음 → 건너뜀")
                current_dt += timedelta(days=1)
                continue

        except Exception as e:
            print(f"❌ {date_str} 조회 실패: {e}")
            current_dt += timedelta(days=1)
            continue

        for ticker in tickers:
            if ticker not in df.index:
                continue

            try:
                ratio = float(df.loc[ticker, "한도소진률"])  # float 변환 필수!
            except Exception as e:
                print(f"⚠️ {date_str} {ticker} 접근 오류: {e}")
                continue

            # INSERT (중복 시 UPDATE)
            cur.execute("""
                INSERT INTO daily_details_stocks (ticker, date, foreign_limit_ratio)
                VALUES (%s, %s, %s)
                ON CONFLICT (ticker, date) DO UPDATE SET foreign_limit_ratio = EXCLUDED.foreign_limit_ratio;
            """, (ticker, current_dt.date(), ratio))

        conn.commit()
        print(f"✅ {date_str} 저장 완료")
        current_dt += timedelta(days=1)

    cur.close()
    conn.close()
    print("🎉 전체 저장 완료")

# 3. 실행
if __name__ == "__main__":
    today_str = datetime.today().strftime("%Y-%m-%d")
    save_foreign_limit_ratios("2018-01-01", today_str)
