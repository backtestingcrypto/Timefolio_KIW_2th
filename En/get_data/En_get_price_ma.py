import pandas as pd
from tqdm import tqdm
import sys
import os

# ✅ 경로 설정
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from shared.connect_postgresql import get_connection
from shared.En_config import schema_name, table_name, full_table_name

def update_price_moving_averages():
    conn = get_connection()
    cur = conn.cursor()

    # ✅ 컬럼 추가 (없으면)
    alter_queries = [
        f"ALTER TABLE {full_table_name} ADD COLUMN IF NOT EXISTS price_ma_5 DOUBLE PRECISION;",
        f"ALTER TABLE {full_table_name} ADD COLUMN IF NOT EXISTS price_ma_20 DOUBLE PRECISION;",
        f"ALTER TABLE {full_table_name} ADD COLUMN IF NOT EXISTS price_ma_60 DOUBLE PRECISION;",
        f"ALTER TABLE {full_table_name} ADD COLUMN IF NOT EXISTS price_ma_120 DOUBLE PRECISION;"
    ]
    for query in alter_queries:
        cur.execute(query)
    conn.commit()

    # ✅ 종목 리스트 가져오기
    cur.execute(f"SELECT DISTINCT ticker_numeric FROM {full_table_name};")
    tickers = [row[0] for row in cur.fetchall()]

    for ticker in tqdm(tickers, desc="📈 이동평균 계산 중"):
        df = pd.read_sql(f"""
            SELECT date, close
            FROM {full_table_name}
            WHERE ticker_numeric = '{ticker}'
            ORDER BY date;
        """, conn)

        if df.empty or len(df) < 5:
            continue

        # ✅ 이동평균 계산
        df["price_ma_5"] = df["close"].rolling(window=5).mean().round(2)
        df["price_ma_20"] = df["close"].rolling(window=20).mean().round(2)
        df["price_ma_60"] = df["close"].rolling(window=60).mean().round(2)
        df["price_ma_120"] = df["close"].rolling(window=120).mean().round(2)

        for _, row in df.iterrows():
            cur.execute(f"""
                UPDATE {full_table_name}
                SET price_ma_5 = %s,
                    price_ma_20 = %s,
                    price_ma_60 = %s,
                    price_ma_120 = %s
                WHERE ticker_numeric = %s AND date = %s;
            """, (
                row["price_ma_5"],
                row["price_ma_20"],
                row["price_ma_60"],
                row["price_ma_120"],
                ticker,
                row["date"]
            ))

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ {full_table_name} 모든 이동평균 저장 완료")

if __name__ == "__main__":
    update_price_moving_averages()
