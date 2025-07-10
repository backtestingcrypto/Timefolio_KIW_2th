import pandas as pd
from tqdm import tqdm
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from shared.connect_postgresql import get_connection

def update_bollinger_band():
    conn = get_connection()
    cur = conn.cursor()

    # 📌 컬럼 추가
    alter_queries = [
        "ALTER TABLE energy.en ADD COLUMN IF NOT EXISTS bb_upper_20 FLOAT;",
        "ALTER TABLE energy.en ADD COLUMN IF NOT EXISTS bb_middle_20 FLOAT;",
        "ALTER TABLE energy.en ADD COLUMN IF NOT EXISTS bb_lower_20 FLOAT;"
    ]
    for query in alter_queries:
        cur.execute(query)
    conn.commit()

    # 📌 티커 목록 조회
    cur.execute("SELECT DISTINCT ticker_numeric FROM energy.en;")
    tickers = [row[0] for row in cur.fetchall()]

    for ticker in tqdm(tickers, desc="📊 Bollinger Band 계산 중"):
        df = pd.read_sql(f"""
            SELECT date, close
            FROM energy.en
            WHERE ticker_numeric = '{ticker}'
            ORDER BY date;
        """, conn)

        if len(df) < 20:
            continue

        df["bb_middle_20"] = df["close"].rolling(window=20).mean()
        df["bb_std_20"] = df["close"].rolling(window=20).std()
        df["bb_upper_20"] = (df["bb_middle_20"] + 2 * df["bb_std_20"]).round(2)
        df["bb_lower_20"] = (df["bb_middle_20"] - 2 * df["bb_std_20"]).round(2)
        df["bb_middle_20"] = df["bb_middle_20"].round(2)

        for _, row in df.iterrows():
            if pd.isna(row["bb_middle_20"]):
                continue

            cur.execute("""
                UPDATE energy.en
                SET bb_upper_20 = %s,
                    bb_middle_20 = %s,
                    bb_lower_20 = %s
                WHERE ticker_numeric = %s AND date = %s;
            """, (
                row["bb_upper_20"],
                row["bb_middle_20"],
                row["bb_lower_20"],
                ticker,
                row["date"]
            ))

    conn.commit()
    cur.close()
    conn.close()
    print("✅ Bollinger Band 저장 완료")

if __name__ == "__main__":
    update_bollinger_band()
