import pandas as pd
import psycopg2
from psycopg2 import sql
from tqdm import tqdm
import os
import sys

# ✅ 경로 설정
try:
    current_dir = os.path.dirname(__file__)
except NameError:
    current_dir = os.getcwd()
sys.path.append(os.path.abspath(os.path.join(current_dir, "../..")))

from shared.connect_postgresql import get_connection
from shared.It_config import full_table_name  # 예: "schema.table"

# ✅ 피보나치 영역 계산 함수 (상/하한가 처리 포함)
def get_fibo_zone(value, high, low, pct_change_close):
    if high == low:
        if pct_change_close is not None:
            if pct_change_close >= 28:
                return "LimitUp"
            elif pct_change_close <= -28:
                return "LimitDown"
        return "Undefined"
    ratio = (high - value) / (high - low)
    if ratio <= 0.236:
        return "Zone0"
    elif ratio <= 0.382:
        return "Zone1"
    elif ratio <= 0.5:
        return "Zone2"
    elif ratio <= 0.618:
        return "Zone3"
    elif ratio <= 0.786:
        return "Zone4"
    else:
        return "Zone5"

# ✅ DB 연결
conn = get_connection()
cur = conn.cursor()

# ✅ 컬럼 존재 여부 확인 및 없으면 추가
cur.execute(f"""
    SELECT column_name
    FROM information_schema.columns
    WHERE table_name = %s AND column_name = 'candle_pattern';
""", (full_table_name.split('.')[-1],))

if cur.fetchone() is None:
    print("📌 candle_pattern 컬럼이 없어 추가합니다.")
    cur.execute(f"""ALTER TABLE {full_table_name} ADD COLUMN candle_pattern TEXT;""")
    conn.commit()

# ✅ 데이터 불러오기 (pct_change_close 포함)
df = pd.read_sql(f"""
    SELECT date, ticker_numeric, open, close, high, low, pct_change_close
    FROM {full_table_name}
    WHERE candle_pattern IS NULL
""", conn)

if df.empty:
    print("✅ 모든 데이터에 candle_pattern이 이미 저장되어 있습니다.")
    conn.close()
    exit()

# ✅ 피보나치 캔들 패턴 계산
def get_pattern(row):
    open_zone = get_fibo_zone(row["open"], row["high"], row["low"], row["pct_change_close"])
    close_zone = get_fibo_zone(row["close"], row["high"], row["low"], row["pct_change_close"])
    return f"{open_zone}/{close_zone}"

df["candle_pattern"] = df.apply(get_pattern, axis=1)

# ✅ DB 업데이트 (tqdm으로 진행 표시)
update_query = sql.SQL("""
    UPDATE {table}
    SET candle_pattern = %s
    WHERE date = %s AND ticker_numeric = %s
""").format(table=sql.Identifier(*full_table_name.split(".")))

for _, row in tqdm(df.iterrows(), total=len(df), desc="🔥 캔들 패턴 저장 중"):
    cur.execute(update_query, (row["candle_pattern"], row["date"], row["ticker_numeric"]))

conn.commit()
conn.close()
print(f"✅ 총 {len(df)}개 레코드의 candle_pattern을 업데이트했습니다.")
