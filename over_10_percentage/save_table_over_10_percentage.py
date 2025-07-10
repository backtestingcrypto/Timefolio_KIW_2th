import pandas as pd
import sys
import os

# 경로 설정
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from shared.connect_postgresql import get_connection

def insert_over_10_percentage():
    conn = get_connection()
    cur = conn.cursor()

    # ✅ 1. 모든 데이터 조회 (조건 X)
    query = """
    SELECT *
    FROM daily_details_stocks
    ORDER BY ticker, date;
    """
    df = pd.read_sql(query, conn)

    # ✅ 2. 날짜 정렬 및 prev/next 계산
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["ticker", "date"]).copy()

    df["prev_close"] = df.groupby("ticker")["close"].shift(1)
    df["next_high"] = df.groupby("ticker")["high"].shift(-1)
    df["next_low"] = df.groupby("ticker")["low"].shift(-1)
    df["next_open"] = df.groupby("ticker")["open"].shift(-1)
    df["next_close"] = df.groupby("ticker")["close"].shift(-1)

    # ✅ 3. prev_close가 존재하고 high가 전일 대비 10% 이상 상승한 경우
    df = df[df["prev_close"].notna()]
    df = df[df["high"] >= df["prev_close"] * 1.10]

    # ✅ 4. 퍼센트 변화 계산
    df["high_pct"] = (df["high"] - df["prev_close"]) / df["prev_close"] * 100
    df["low_pct"] = (df["low"] - df["prev_close"]) / df["prev_close"] * 100
    df["close_pct"] = (df["close"] - df["prev_close"]) / df["prev_close"] * 100
    df["open_pct"] = (df["open"] - df["prev_close"]) / df["prev_close"] * 100

    df["next_high_pct"] = (df["next_high"] - df["close"]) / df["close"] * 100
    df["next_low_pct"] = (df["next_low"] - df["close"]) / df["close"] * 100
    df["next_open_pct"] = (df["next_open"] - df["close"]) / df["close"] * 100
    df["next_close_pct"] = (df["next_close"] - df["close"]) / df["close"] * 100

    # ✅ 5. 조건 계산
    df["bb_upper_break"] = df["high"] >= df["bb_upper_20"]
    df["vol_ma_break"] = df["volume"] > df["vol_ma_20"]
    df["trade_amount_change"] = (df["trade_amount"] - df["avg_trade_amount_5d"]) / df["avg_trade_amount_5d"] * 100

    # ✅ 6. 이동평균선 순서 문자열 생성
    def ma_order(row):
        ma_dict = {
            "5": row["price_ma_5"],
            "20": row["price_ma_20"],
            "60": row["price_ma_60"],
            "120": row["price_ma_120"],
        }
        sorted_ma = sorted(ma_dict.items(), key=lambda x: x[1])
        return ">".join([ma[0] for ma in sorted_ma])

    df["ma_order"] = df.apply(ma_order, axis=1)

    # ✅ 7. 저장 전에 avg_trade_amount_5d 조건 필터링
    df = df[df["avg_trade_amount_5d"] >= 3_000_000_000]

    # ✅ 8. 저장할 컬럼 선택
    insert_df = df[[
        "ticker", "date", "prev_close",
        "high_pct", "low_pct", "close_pct", "open_pct",
        "next_high_pct", "next_low_pct", "next_open_pct", "next_close_pct",
        "rsi_14", "bb_upper_break", "vol_ma_break",
        "trade_amount_change", "ma_order", "daily_market_cap"
    ]].copy()

    # ✅ 9. INSERT 쿼리
    insert_query = """
    INSERT INTO over_10_percentage (
        ticker, date, prev_close,
        high_pct, low_pct, close_pct, open_pct,
        next_high_pct, next_low_pct, next_open_pct, next_close_pct,
        rsi_14, bb_upper_break, vol_ma_break,
        trade_amount_change, ma_order, daily_market_cap
    ) VALUES (
        %(ticker)s, %(date)s, %(prev_close)s,
        %(high_pct)s, %(low_pct)s, %(close_pct)s, %(open_pct)s,
        %(next_high_pct)s, %(next_low_pct)s, %(next_open_pct)s, %(next_close_pct)s,
        %(rsi_14)s, %(bb_upper_break)s, %(vol_ma_break)s,
        %(trade_amount_change)s, %(ma_order)s, %(daily_market_cap)s
    )
    ON CONFLICT (ticker, date) DO NOTHING;
    """

    # ✅ 10. 데이터 저장
    for _, row in insert_df.iterrows():
        cur.execute(insert_query, row.to_dict())

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ 총 {len(insert_df)}개 행이 over_10_percentage 테이블에 저장되었습니다.")

if __name__ == "__main__":
    insert_over_10_percentage()
