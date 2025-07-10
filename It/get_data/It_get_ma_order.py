import pandas as pd
from tqdm import tqdm
import sys
import os

# ✅ 경로 설정
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from shared.connect_postgresql import get_connection
from shared.It_config import schema_name, table_name, full_table_name

def get_ma_order(row):
    ma_values = {
        "5": row["price_ma_5"],
        "20": row["price_ma_20"],
        "60": row["price_ma_60"],
        "120": row["price_ma_120"]
    }
    filtered_ma = {k: v for k, v in ma_values.items() if pd.notna(v)}
    if len(filtered_ma) < 2:
        return None
    sorted_keys = sorted(filtered_ma, key=lambda x: filtered_ma[x], reverse=True)
    return ">".join(sorted_keys)

def add_ma_order_column():
    conn = get_connection()
    cur = conn.cursor()

    # ✅ ma_order 컬럼 추가
    cur.execute(f"ALTER TABLE {full_table_name} ADD COLUMN IF NOT EXISTS ma_order TEXT;")
    conn.commit()

    # ✅ 필요한 데이터 로딩
    df = pd.read_sql(f"""
        SELECT date, ticker_numeric,
               price_ma_5, price_ma_20, price_ma_60, price_ma_120
        FROM {full_table_name}
    """, conn)

    # ✅ ma_order 계산
    df["ma_order"] = df.apply(get_ma_order, axis=1)

    # ✅ ma_order가 있는 경우에만 업데이트
    for _, row in tqdm(df[df["ma_order"].notna()].iterrows(), total=df["ma_order"].notna().sum(), desc="🧩 ma_order 저장"):
        cur.execute(f"""
            UPDATE {full_table_name}
            SET ma_order = %s
            WHERE date = %s AND ticker_numeric = %s;
        """, (row["ma_order"], row["date"], row["ticker_numeric"]))

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ {full_table_name} ma_order 컬럼 저장 완료")

if __name__ == "__main__":
    add_ma_order_column()
