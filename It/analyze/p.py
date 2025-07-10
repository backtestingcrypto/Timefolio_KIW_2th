import pandas as pd
import psycopg2
import sys
import os

# 경로 설정
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from shared.connect_postgresql import get_connection
from shared.It_config import full_table_name

# 날짜 범위 설정
start_date = "2018-01-01"
end_date = "2025-06-30"

# DB 연결
conn = get_connection()

# 📥 데이터 조회
query = f"""
SELECT candle_pattern, pct_change_next_close, daily_market_cap,
       open, high, low, close, volume, vol_gt_ma5, ma_order
FROM {full_table_name}
WHERE date BETWEEN '{start_date}' AND '{end_date}'
  AND open > 0 AND high > 0 AND low > 0 AND close > 0 AND volume > 0
  AND daily_market_cap IS NOT NULL
  AND vol_gt_ma5 IS NOT NULL
  AND ma_order IS NOT NULL;
"""
df = pd.read_sql(query, conn)

# ✅ 전체 캔들 수
n = len(df)

# ✅ 유효한 데이터 필터
df_valid = df[df["candle_pattern"].notnull()]

# ✅ 시가총액 구간 함수 정의
def market_cap_bucket(cap):
    if cap <= 50_000_000_000:
        return "~500억"
    elif cap <= 100_000_000_000:
        return "~1000억"
    elif cap <= 150_000_000_000:
        return "~1500억"
    elif cap <= 200_000_000_000:
        return "~2000억"
    elif cap <= 250_000_000_000:
        return "~2500억"
    elif cap <= 300_000_000_000:
        return "~3000억"
    elif cap <= 350_000_000_000:
        return "~3500억"
    elif cap <= 400_000_000_000:
        return "~4000억"
    elif cap <= 450_000_000_000:
        return "~4500억"
    elif cap <= 500_000_000_000:
        return "~5000억"
    elif cap <= 600_000_000_000:
        return "~6000억"
    elif cap <= 700_000_000_000:
        return "~7000억"
    elif cap <= 800_000_000_000:
        return "~8000억"
    elif cap <= 900_000_000_000:
        return "~9000억"
    elif cap <= 1_000_000_000_000:
        return "~1조"
    elif cap <= 2_000_000_000_000:
        return "~2조"
    elif cap <= 2_500_000_000_000:
        return "~2.5조"
    elif cap <= 3_000_000_000_000:
        return "~3조"
    elif cap <= 3_500_000_000_000:
        return "~3.5조"
    elif cap <= 4_000_000_000_000:
        return "~4조"
    elif cap <= 4_500_000_000_000:
        return "~4.5조"
    elif cap <= 5_000_000_000_000:
        return "~5조"
    else:
        return "5조+"

# ✅ 시총 구간 추가
df_valid["market_cap_group"] = df_valid["daily_market_cap"].apply(market_cap_bucket)

# ✅ 그룹 키 조합
df_valid["pattern_cap_vol_ma_group"] = (
    df_valid["candle_pattern"] + "-" +
    df_valid["market_cap_group"] + "-" +
    df_valid["vol_gt_ma5"].astype(str) + "-" +
    df_valid["ma_order"]
)

# ✅ 그룹 분석
summary = df_valid.groupby("pattern_cap_vol_ma_group")["pct_change_next_close"].agg(
    k="count",
    avg_return="mean",
    stddev_return="std"
).reset_index()

# ✅ 전체 대비 진입 비율 및 Sharpe-like 지표
summary["n"] = n
summary["entry_ratio (%)"] = (summary["k"] / summary["n"] * 100).round(2)
summary["risk_adjusted_consistency"] = summary["avg_return"] / summary["stddev_return"]

# ✅ 조건 필터링: k ≥ 2 and 리스크 대비 수익 ≥ 1
summary = summary[(summary["k"] >= 1) & (summary["risk_adjusted_consistency"] >= 2)]

# ✅ 정렬 및 출력
summary = summary.sort_values(by="risk_adjusted_consistency", ascending=True)
print(summary.to_string(index=False))
