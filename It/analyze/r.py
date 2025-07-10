import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import psycopg2
import sys
import os
import itertools
import platform
import matplotlib.font_manager as fm

# ✅ 경로 및 한글 폰트 설정
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from shared.connect_postgresql import get_connection
from shared.It_config import full_table_name

if platform.system() == "Windows":
    font_path = "C:/Windows/Fonts/malgun.ttf"
    font_prop = fm.FontProperties(fname=font_path)
    plt.rcParams["font.family"] = font_prop.get_name()
else:
    plt.rcParams["font.family"] = "NanumGothic"

# ✅ 날짜 범위 설정
start_date = "2024-01-01"
end_date = "2025-06-30"

# ✅ DB 연결 및 데이터 불러오기
conn = get_connection()
query = f"""
SELECT candle_pattern, pct_change_next_close, daily_market_cap,
       open, high, low, close, volume, vol_gt_ma5, ma_order
FROM {full_table_name}
WHERE date BETWEEN '{start_date}' AND '{end_date}'
  AND open > 0 AND high > 0 AND low > 0 AND close > 0 AND volume > 0
  AND daily_market_cap IS NOT NULL
  AND vol_gt_ma5 IS NOT NULL
  AND ma_order IS NOT NULL
  AND candle_pattern IS NOT NULL;
"""
df = pd.read_sql(query, conn)

# ✅ 시가총액 그룹화 함수 정의
def market_cap_bucket(cap):
    buckets = [
        (50e9, "~500억"), (100e9, "~1000억"), (150e9, "~1500억"),
        (200e9, "~2000억"), (250e9, "~2500억"), (300e9, "~3000억"),
        (400e9, "~4000억"), (500e9, "~5000억"), (600e9, "~6000억"),
        (700e9, "~7000억"), (800e9, "~8000억"), (900e9, "~9000억"),
        (1e12, "~1조"), (2e12, "~2조"), (2.5e12, "~2.5조"),
        (3e12, "~3조"), (3.5e12, "~3.5조"), (4e12, "~4조"),
        (4.5e12, "~4.5조"), (5e12, "~5조")
    ]
    for limit, label in buckets:
        if cap <= limit:
            return label
    return "5조+"

# ✅ 시가총액 구간 추가
df["market_cap_group"] = df["daily_market_cap"].apply(market_cap_bucket)

# ✅ 조건 변수 설정
conditions = ["candle_pattern", "ma_order", "vol_gt_ma5", "market_cap_group"]
fixed_combinations = list(itertools.combinations(conditions, 2))

# ✅ 시총 정렬 기준 정의
cap_order = [
    "~500억", "~1000억", "~1500억", "~2000억", "~2500억", "~3000억", "~4000억", "~5000억",
    "~6000억", "~7000억", "~8000억", "~9000억", "~1조", "~2조", "~2.5조", "~3조",
    "~3.5조", "~4조", "~4.5조", "~5조", "5조+"
]

# ✅ 히트맵 반복 생성
for fixed in fixed_combinations:
    varying = [col for col in conditions if col not in fixed]
    fixed_unique_pairs = df[list(fixed)].drop_duplicates()

    for _, fixed_values in fixed_unique_pairs.iterrows():
        filtered = df.copy()
        for col in fixed:
            filtered = filtered[filtered[col] == fixed_values[col]]
        if filtered.empty:
            continue

        pivot = filtered.pivot_table(
            values="pct_change_next_close",
            index=varying[0],
            columns=varying[1],
            aggfunc="mean"
        )

        # ✅ 시가총액 순서 정렬 적용
        if varying[0] == "market_cap_group":
            pivot = pivot.reindex(index=cap_order)
        if varying[1] == "market_cap_group":
            pivot = pivot.reindex(columns=cap_order)

        plt.figure(figsize=(8, 6))
        sns.heatmap(pivot, annot=True, fmt=".3f", cmap="RdYlGn", center=0)
        plt.title(f"평균 수익률 히트맵\n[고정 조건] {fixed[0]} = {fixed_values[fixed[0]]}, {fixed[1]} = {fixed_values[fixed[1]]}")
        plt.xlabel(varying[1])
        plt.ylabel(varying[0])
        plt.tight_layout()
        plt.show()
        plt.close()
