import pandas as pd
from tqdm import tqdm
import sys
import os

# ✅ 경로 설정
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from shared.connect_postgresql import get_connection
from shared.En_config import schema_name, table_name, full_table_name

def update_volume_ma():
    conn = get_connection()
    cur = conn.cursor()

    # 📌 컬럼 추가
    alter_queries = [
        f"ALTER TABLE {full_table_name} ADD COLUMN IF NOT EXISTS vol_ma_5 FLOAT;",
        f"ALTER TABLE {full_table_name} ADD COLUMN IF NOT EXISTS vol_ma_20 FLOAT;"
    ]
    for query in alter_queries:
        cur.execute(query)
    conn.commit()

    # 📌 티커 목록 조회
    cur.execute(f"SELECT DISTINCT ticker_numeric FROM {full_table_name};")
    tickers = [row[0] for row in cur.fetchall()]

    for ticker in tqdm(tickers, desc="📊 거래량 이동평균 계산 중"):
        df = pd.read_sql(f"""
            SELECT date, volume
            FROM {full_table_name}
            WHERE ticker_numeric = %s
            ORDER BY date;
        """, conn, params=(ticker,))

        if len(df) < 20:
            continue

        df["vol_ma_5"] = df["volume"].rolling(window=5).mean().round(2)
        df["vol_ma_20"] = df["volume"].rolling(window=20).mean().round(2)

        for _, row in df.iterrows():
            if pd.isna(row["vol_ma_5"]) and pd.isna(row["vol_ma_20"]):
                continue

            cur.execute(f"""
                UPDATE {full_table_name}
                SET vol_ma_5 = %s,
                    vol_ma_20 = %s
                WHERE ticker_numeric = %s AND date = %s;
            """, (
                row["vol_ma_5"],
                row["vol_ma_20"],
                ticker,
                row["date"]
            ))

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ {full_table_name} 거래량 이동평균 저장 완료")

if __name__ == "__main__":
    update_volume_ma()
