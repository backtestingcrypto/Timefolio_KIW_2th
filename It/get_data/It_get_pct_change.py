import pandas as pd
from tqdm import tqdm
import sys
import os

# ✅ 경로 설정
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from shared.connect_postgresql import get_connection
from shared.It_config import schema_name, table_name, full_table_name

def add_and_update_pct_changes():
    conn = get_connection()
    cur = conn.cursor()

    # ✅ 필요한 컬럼 추가
    col_queries = [
        f"ALTER TABLE {full_table_name} ADD COLUMN IF NOT EXISTS pct_change_prev_close FLOAT;",
        f"ALTER TABLE {full_table_name} ADD COLUMN IF NOT EXISTS pct_change_close FLOAT;",
        f"ALTER TABLE {full_table_name} ADD COLUMN IF NOT EXISTS pct_change_next_close FLOAT;",
        f"ALTER TABLE {full_table_name} ADD COLUMN IF NOT EXISTS pct_change_next2_close FLOAT;"
    ]
    for query in col_queries:
        cur.execute(query)
    conn.commit()

    # ✅ ticker 목록
    cur.execute(f"SELECT DISTINCT ticker_numeric FROM {full_table_name};")
    tickers = [row[0] for row in cur.fetchall()]

    for ticker in tqdm(tickers, desc="📊 수익률 계산"):
        df = pd.read_sql(f"""
            SELECT date, close
            FROM {full_table_name}
            WHERE ticker_numeric = '{ticker}'
            ORDER BY date;
        """, conn)

        if df.empty or len(df) < 3:
            continue

        # ✅ 수익률 계산
        df["pct_change_close"] = ((df["close"] / df["close"].shift(1) - 1) * 100).round(2)
        df["pct_change_prev_close"] = ((df["close"].shift(1) / df["close"].shift(2) - 1) * 100).round(2)
        df["pct_change_next_close"] = ((df["close"].shift(-1) / df["close"] - 1) * 100).round(2)
        df["pct_change_next2_close"] = ((df["close"].shift(-2) / df["close"] - 1) * 100).round(2)

        # ✅ DB 업데이트
        for _, row in df.iterrows():
            if pd.isna(row["pct_change_prev_close"]) and pd.isna(row["pct_change_close"]) \
               and pd.isna(row["pct_change_next_close"]) and pd.isna(row["pct_change_next2_close"]):
                continue

            cur.execute(f"""
                UPDATE {full_table_name}
                SET pct_change_prev_close = %s,
                    pct_change_close = %s,
                    pct_change_next_close = %s,
                    pct_change_next2_close = %s
                WHERE ticker_numeric = %s AND date = %s;
            """, (
                row["pct_change_prev_close"],
                row["pct_change_close"],
                row["pct_change_next_close"],
                row["pct_change_next2_close"],
                ticker,
                row["date"]
            ))

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ {full_table_name} 수익률 저장 완료")

if __name__ == "__main__":
    add_and_update_pct_changes()
