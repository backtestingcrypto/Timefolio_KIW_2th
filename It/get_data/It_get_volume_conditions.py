import pandas as pd
from tqdm import tqdm
import sys
import os

# ✅ 경로 설정
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from shared.connect_postgresql import get_connection
from shared.It_config import schema_name, table_name, full_table_name

def update_volume_conditions():
    conn = get_connection()
    cur = conn.cursor()

    # ✅ 필요한 컬럼 생성
    alter_queries = [
        f"ALTER TABLE {full_table_name} ADD COLUMN IF NOT EXISTS vol_ma_break BOOLEAN;",
        f"ALTER TABLE {full_table_name} ADD COLUMN IF NOT EXISTS vol_gt_ma5 BOOLEAN;",
        f"ALTER TABLE {full_table_name} ADD COLUMN IF NOT EXISTS vol_gt_ma20 BOOLEAN;"
    ]
    for q in alter_queries:
        cur.execute(q)
    conn.commit()

    # ✅ 티커 목록 조회
    cur.execute(f"SELECT DISTINCT ticker_numeric FROM {full_table_name};")
    tickers = [row[0] for row in cur.fetchall()]

    for ticker in tqdm(tickers, desc="📊 거래량 조건 저장 중"):
        df = pd.read_sql(f"""
            SELECT date, volume, vol_ma_5, vol_ma_20
            FROM {full_table_name}
            WHERE ticker_numeric = %s
            ORDER BY date;
        """, conn, params=(ticker,))

        if df.empty:
            continue

        # ✅ 결측치 제거
        df = df.dropna(subset=["volume", "vol_ma_5", "vol_ma_20"])
        if df.empty:
            continue

        # ✅ 조건 계산
        df["vol_ma_break"] = df["vol_ma_5"] > df["vol_ma_20"]
        df["vol_gt_ma5"] = df["volume"] > df["vol_ma_5"]
        df["vol_gt_ma20"] = df["volume"] > df["vol_ma_20"]

        # ✅ DB 업데이트
        for _, row in df.iterrows():
            cur.execute(f"""
                UPDATE {full_table_name}
                SET vol_ma_break = %s,
                    vol_gt_ma5 = %s,
                    vol_gt_ma20 = %s
                WHERE ticker_numeric = %s AND date = %s;
            """, (
                bool(row["vol_ma_break"]),
                bool(row["vol_gt_ma5"]),
                bool(row["vol_gt_ma20"]),
                ticker,
                row["date"]
            ))

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ {full_table_name} 거래량 조건 저장 완료")

if __name__ == "__main__":
    update_volume_conditions()
