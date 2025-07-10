import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import psycopg2
import sys
import os
import platform
import matplotlib.font_manager as fm

# 🔧 경로 설정
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from shared.connect_postgresql import get_connection
from shared.It_config import full_table_name

# 🔧 한글 폰트 설정
if platform.system() == "Windows":
    font_path = "C:/Windows/Fonts/malgun.ttf"
    font_prop = fm.FontProperties(fname=font_path)
    plt.rcParams["font.family"] = font_prop.get_name()
else:
    plt.rcParams["font.family"] = "NanumGothic"

# ✅ 날짜 범위
start_date = "2024-01-01"
end_date = "2025-06-30"

# ✅ DB 연결 및 쿼리 실행
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

# ✅ 시총 구간 함수
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

# ✅ 시총 그룹 추가
df["market_cap_group"] = df["daily_market_cap"].apply(market_cap_bucket)

# ✅ 시총 그룹 리스트 정렬 기준
def cap_sort_key(x):
    x = x.replace("~", "").replace("억", "e8").replace("조", "e12").replace("+", "")
    try:
        return float(eval(x))
    except:
        return float("inf")

market_cap_groups = sorted(df["market_cap_group"].unique(), key=cap_sort_key)

# ✅ 시총별 히트맵
for cap in market_cap_groups:
    filtered = df[df["market_cap_group"] == cap]
    if filtered.empty:
        continue

    grouped = filtered.groupby(["candle_pattern", "ma_order"])["pct_change_next_close"].agg(
        k="count",
        avg_return="mean",
        stddev_return="std"
    ).reset_index()

    # ✅ 필터: k ≥ 5 and Sharpe-like ≥ 1
    grouped["risk_adjusted_consistency"] = grouped["avg_return"] / grouped["stddev_return"]
    grouped = grouped[(grouped["k"] >= 5) & (grouped["risk_adjusted_consistency"] >= 1)]

    if grouped.empty:
        continue

    pivot = grouped.pivot(index="candle_pattern", columns="ma_order", values="avg_return")

    plt.figure(figsize=(10, 6))
    sns.heatmap(pivot, annot=True, fmt=".3f", cmap="RdYlGn", center=0)
    plt.title(f"📊 평균 수익률 히트맵 (k≥5, Sharpe≥1)\n시총: {cap}")
    plt.xlabel("MA 배열 순서")
    plt.ylabel("캔들 패턴")
    plt.tight_layout()
    plt.show()
    plt.close()
