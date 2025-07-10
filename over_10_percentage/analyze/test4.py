import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from tqdm import tqdm
import numpy as np
import sys
import os

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
    bb_upper_break, 
    vol_ma_break,
    sector_code
FROM over_10_with_sector
WHERE next_high_pct IS NOT NULL
  AND next_low_pct IS NOT NULL
  AND next_close_pct IS NOT NULL
  AND bb_upper_break IS NOT NULL
  AND vol_ma_break IS NOT NULL
ORDER BY date;
"""
df = pd.read_sql(query, conn)
conn.close()

# ✅ 날짜 변환 및 조건 필터링
df["entry_date"] = pd.to_datetime(df["entry_date"])
df_filtered = df[
    (df["bb_upper_break"] == True) &
    (df["vol_ma_break"] == True)
].copy()

# ✅ 전략 수익률 계산 함수
def apply_strategy(row, x, y):
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

# ✅ 하이퍼파라미터 범위
x_range = np.arange(1.0, 30.5, 0.5)
y_range = np.arange(0.5, 5.5, 0.5)

# ✅ 모든 섹터에 대해 반복
sectors = sorted(df_filtered["sector_code"].dropna().unique())

for sector in sectors:
    print(f"\n📊 섹터: {sector} 탐색 중...")

    sector_df = df_filtered[df_filtered["sector_code"] == sector].copy()
    results = []

    for x in tqdm(x_range, desc=f"  x loop for sector {sector}"):
        for y in y_range:
            temp_df = sector_df.copy()
            temp_df["strategy_pct"] = temp_df.apply(apply_strategy, axis=1, args=(x, y))
            result_df = simulate(temp_df)

            if not result_df.empty and not pd.isna(result_df["capital"].iloc[-1]):
                final_capital = result_df["capital"].iloc[-1]
                results.append((x, y, final_capital))

    # 결과 DataFrame
    result_df = pd.DataFrame(results, columns=["x", "y", "final_capital"])

    if result_df.empty:
        print(f"❌ {sector} 섹터는 유효한 전략 결과가 없습니다.")
        continue

    # ✅ Top 5 전략 출력
    top5 = result_df.sort_values("final_capital", ascending=False).head(5)
    print(f"\n📌 [Top 5 전략 - {sector} 섹터]")
    for i, row in top5.iterrows():
        print(f"  {i+1}. x={row['x']:.1f}%, y={row['y']:.1f}% → {row['final_capital']:.2f}배")

    # ✅ Heatmap
    pivot = result_df.pivot(index="y", columns="x", values="final_capital")
    plt.figure(figsize=(12, 6))
    sns.heatmap(pivot, annot=True, fmt=".2f", cmap="YlGnBu")
    plt.title(f"[{sector}] 최종 누적 자산 Heatmap (목표 x%, 손절 y%)")
    plt.xlabel("목표 수익률 x (%)")
    plt.ylabel("손절 기준 y (%)")
    plt.tight_layout()
    plt.show()

    # ✅ 누적 수익률 그래프 (Top 1 조합)
    best_x = top5.iloc[0]["x"]
    best_y = top5.iloc[0]["y"]
    temp_df = sector_df.copy()
    temp_df["strategy_pct"] = temp_df.apply(apply_strategy, axis=1, args=(best_x, best_y))
    best_result = simulate(temp_df)

    plt.figure(figsize=(12, 5))
    plt.plot(best_result["entry_date"], best_result["capital"], label=f"x={best_x:.1f}%, y={best_y:.1f}%")
    plt.title(f"[{sector}] Top 전략 누적 수익률")
    plt.xlabel("날짜")
    plt.ylabel("누적 자산")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()
