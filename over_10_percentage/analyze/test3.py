import pandas as pd
import matplotlib.pyplot as plt
import sys
import os
import numpy as np
from tqdm import tqdm
from itertools import product

# 경로 설정
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from shared.connect_postgresql import get_connection

# 한글 폰트 설정
plt.rcParams['font.family'] = 'Malgun Gothic'
plt.rcParams['axes.unicode_minus'] = False

# ✅ 데이터 불러오기
conn = get_connection()
query = """
SELECT 
    date AS entry_date, 
    ticker, 
    next_high_pct, 
    next_low_pct, 
    next_close_pct,
    next_open_pct,  
    bb_upper_break, 
    vol_ma_break,
    sector_code
FROM over_10_with_sector
WHERE next_high_pct IS NOT NULL
  AND next_low_pct IS NOT NULL
  AND next_close_pct IS NOT NULL
  AND next_open_pct IS NOT NULL
  AND bb_upper_break IS NOT NULL
  AND vol_ma_break IS NOT NULL
ORDER BY date;
"""
df = pd.read_sql(query, conn)
conn.close()

# ✅ 날짜 변환 및 조건 필터링 (상단 + 거래량 돌파)
df["entry_date"] = pd.to_datetime(df["entry_date"])
df_filtered = df[
    (df["bb_upper_break"] == True) &
    (df["vol_ma_break"] == True)
].copy()

# ✅ IT 섹터만 필터링
it_df = df_filtered[df_filtered["sector_code"] == "IT"].copy()

# ✅ 전략 수익률 계산 함수
def apply_strategy(row, x, y):
    if row["next_open_pct"] <= -y:
        return row["next_open_pct"]
    else:
        if row["next_low_pct"] <= -y:
            return -y
        elif row["next_high_pct"] > x:
            return x
        else:
            return row["next_close_pct"]

# ✅ 시뮬레이션 함수
def simulate(df_case, initial_capital=1, fee_rate=0.0015 * 2, max_ratio_per_ticker=0.05):
    capital = initial_capital
    capital_over_time = []
    grouped = df_case.groupby("entry_date")

    for date, group in grouped:
        group = group.dropna(subset=["strategy_pct"])
        group = group.groupby("ticker").mean(numeric_only=True).reset_index()

        if group.empty:
            capital_over_time.append((date, capital))
            continue

        n = len(group)
        invest_per_ticker = min(capital / n, capital * max_ratio_per_ticker)

        profit = 0
        for _, row in group.iterrows():
            pct = row["strategy_pct"]
            if pd.isna(pct):
                continue
            result = invest_per_ticker * (1 + pct / 100 - fee_rate)
            profit += result

        unused = capital - invest_per_ticker * n
        capital = profit + unused
        capital_over_time.append((date, capital))

    return pd.DataFrame(capital_over_time, columns=["entry_date", "capital"])

# ✅ 하이퍼파라미터 탐색 범위
x_range = range(1, 30, 1)  # 목표 수익률: 1% ~ 14%
y_range = range(1, 10, 1)   # 손절 기준: 1% ~ 4%

results = []

# ✅ tqdm으로 진행률 표시
for x, y in tqdm(product(x_range, y_range), total=len(x_range)*len(y_range), desc="탐색 중"):
    temp_df = it_df.copy()
    temp_df["strategy_pct"] = temp_df.apply(apply_strategy, axis=1, args=(x, y))
    result_df = simulate(temp_df)

    if not result_df.empty and not pd.isna(result_df["capital"].iloc[-1]):
        final_capital = result_df["capital"].iloc[-1]
        results.append((x, y, final_capital))

# ✅ 결과 정리
result_df = pd.DataFrame(results, columns=["x", "y", "final_capital"])
best = result_df.sort_values("final_capital", ascending=False).iloc[0]

# ✅ 결과 출력
print("📈 최적 하이퍼파라미터 조합 (IT 섹터 기준):")
print(f"- 목표 수익률 x = {best['x']}%")
print(f"- 손절 기준 y = {best['y']}%")
print(f"- 최종 누적 자산 = {best['final_capital']:.2f}배")
