import pandas as pd
from tqdm import tqdm
import sys
import os

# ✅ 경로 설정
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from shared.connect_postgresql import get_connection

# ✅ 테이블명 설정
schema_name = "energy"
table_name = "en"
full_table_name = f"{schema_name}.{table_name}"

def update_bb_upper_break():
    conn = get_connection()
    cur = conn.cursor()

    # 📌 컬럼 추가
    cur.execute(f"""
        ALTER TABLE {full_table_name}
        ADD COLUMN IF NOT EXISTS bb_upper_break BOOLEAN;
    """)
    conn.commit()

    # 📌 티커 목록 조회
    cur.execute(f"SELECT DISTINCT ticker_numeric FROM {full_table_name};")
    tickers = [row[0] for row in cur.fetchall()]

    for ticker in tqdm(tickers, desc="📈 BB 상단 돌파 여부 계산"):
        df = pd.read_sql(f"""
            SELECT date, open, close, bb_upper_20
            FROM {full_table_name}
            WHERE ticker_numeric = '{ticker}'
            ORDER BY date;
        """, conn)

        if df.empty or df["bb_upper_20"].isna().all():
            continue

        # ✅ 조건: open < bb_upper_20 and close > bb_upper_20
        df["bb_upper_break"] = (df["open"] < df["bb_upper_20"]) & (df["close"] > df["bb_upper_20"])

        for _, row in df.iterrows():
            if pd.isna(row["bb_upper_20"]):
                continue

            cur.execute(f"""
                UPDATE {full_table_name}
                SET bb_upper_break = %s
                WHERE ticker_numeric = %s AND date = %s;
            """, (
                bool(row["bb_upper_break"]),
                ticker,
                row["date"]
            ))

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ {full_table_name} BB 상단 돌파 여부 저장 완료")

if __name__ == "__main__":
    update_bb_upper_break()
