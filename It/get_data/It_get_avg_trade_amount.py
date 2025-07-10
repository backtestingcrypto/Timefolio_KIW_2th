import pandas as pd
from tqdm import tqdm
import sys
import os

# ✅ 경로 설정
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from shared.connect_postgresql import get_connection
from shared.It_config import full_table_name


def add_and_update_trade_amount_averages():
    conn = get_connection()
    cur = conn.cursor()

    # ✅ 컬럼 추가
    cur.execute(f"ALTER TABLE {full_table_name} ADD COLUMN IF NOT EXISTS avg_trade_amount_5d BIGINT;")
    cur.execute(f"ALTER TABLE {full_table_name} ADD COLUMN IF NOT EXISTS avg_trade_amount_4d_prev BIGINT;")
    conn.commit()

    # ✅ 티커 목록 조회
    cur.execute(f"SELECT DISTINCT ticker_numeric FROM {full_table_name};")
    tickers = [row[0] for row in cur.fetchall()]

    for ticker in tqdm(tickers, desc="📊 거래대금 평균 계산"):
        df = pd.read_sql(f"""
            SELECT date, trade_amount
            FROM {full_table_name}
            WHERE ticker_numeric = '{ticker}'
            ORDER BY date
        """, conn)

        if df.empty or len(df) < 5:
            continue

        # ✅ rolling 평균 계산
        df["avg_trade_amount_5d"] = df["trade_amount"].rolling(window=5).mean().round().astype("Int64")
        df["avg_trade_amount_4d_prev"] = df["trade_amount"].shift(1).rolling(window=4).mean().round().astype("Int64")

        for _, row in df.iterrows():
            if pd.isna(row["avg_trade_amount_5d"]) and pd.isna(row["avg_trade_amount_4d_prev"]):
                continue

            cur.execute(f"""
                UPDATE {full_table_name}
                SET avg_trade_amount_5d = %s,
                    avg_trade_amount_4d_prev = %s
                WHERE ticker_numeric = %s AND date = %s;
            """, (
                row["avg_trade_amount_5d"],
                row["avg_trade_amount_4d_prev"],
                ticker,
                row["date"]
            ))

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ {full_table_name} 평균 거래대금 저장 완료")

if __name__ == "__main__":
    add_and_update_trade_amount_averages()
