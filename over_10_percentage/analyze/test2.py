import pandas as pd
import matplotlib.pyplot as plt
from tqdm import tqdm
from itertools import product
import sys
import os

# 경로 설정 (Jupyter 환경에서는 아래 줄 생략 가능)
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
    next_open_pct,
    next_close_pct,
    bb_upper_break,
    vol_ma_break,
    ma_order,
    sector_code,
    daily_market_cap
FROM over_10_with_sector
WHERE next_high_pct IS NOT NULL
  AND next_low_pct IS NOT NULL
  AND next_open_pct IS NOT NULL
  AND next_close_pct IS NOT NULL
  AND bb_upper_break = TRUE
  AND vol_ma_break = TRUE
  AND ma_order IN ('120>60>5>20', '60>20>5>120')
"""
df = pd.read_sql(query, conn)
conn.close()
df["entry_date"] = pd.to_datetime(df["entry_date"])

# ✅ 시가총액 그룹화 함수
def categorize_market_cap(cap):
    if cap < 5e11:
        return "0~500억"
    elif cap < 1e12:
        return "500~1000억"
    elif cap < 1.5e12:
        return "1000~1500억"
    elif cap < 2e12:
        return "1500~2000억"
    elif cap < 2.5e12:
        return "2000~2500억"
    elif cap < 5e12:
        return "2500억~5000억"
    elif cap < 7.5e12:
        return "5000억~7500억"
    elif cap < 1e13:
        return "7500억~1조"
    elif cap < 1.25e13:
        return "1조~1조2500억"
    elif cap < 1.5e13:
        return "1조2500억~1조5000억"
    else:
        return "1조5000억 이상"


df["market_cap_group"] = df["daily_market_cap"].apply(categorize_market_cap)

# ✅ 전략 수익률 계산 함수
def apply_strategy(row, x, y):
    if row["next_open_pct"] <= -y:
        return row["next_open_pct"]
    elif row["next_low_pct"] <= -y:
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
            if pd.isna(pct): continue
            result = invest_per_ticker * (1 + pct / 100 - fee_rate)
            profit += result

        unused = capital - invest_per_ticker * n
        capital = profit + unused
        capital_over_time.append((date, capital))

    return pd.DataFrame(capital_over_time, columns=["entry_date", "capital"])

# ✅ 하이퍼파라미터 범위
x_range = range(5, 31, 1)   # 목표 수익률 (%)
y_range = range(1, 11, 1)   # 손절 기준 (%)
sectors = df["sector_code"].dropna().unique()
groups = df["market_cap_group"].dropna().unique()

# ✅ 섹터 & 시가총액 그룹별로 결과 분석
for sector in sectors:
    for group in groups:
        print(f"\n🔍 Sector: {sector} | MarketCap Group: {group}")
        subset = df[(df["sector_code"] == sector) & (df["market_cap_group"] == group)].copy()

        results = []
        for x, y in tqdm(product(x_range, y_range), total=len(x_range)*len(y_range), desc=f"{sector}-{group} 분석 중"):
            temp_df = subset.copy()
            temp_df["strategy_pct"] = temp_df.apply(apply_strategy, axis=1, args=(x, y))
            sim_df = simulate(temp_df)
            if not sim_df.empty and not pd.isna(sim_df["capital"].iloc[-1]):
                results.append({
                    "x": x,
                    "y": y,
                    "sector_code": sector,
                    "market_cap_group": group,
                    "final_capital": sim_df["capital"].iloc[-1]
                })

        if not results:
            print("❗ 결과 없음")
            continue

        result_df = pd.DataFrame(results).sort_values("final_capital", ascending=False)

        # ✅ 상위 전략 출력
        print("📊 Top 5 전략 조합:")
        print(result_df.head(5))

        # ✅ 누적 수익률 그래프
        top5 = result_df.head(5)
        plt.figure(figsize=(12, 6))
        for _, row in top5.iterrows():
            temp_df = subset.copy()
            temp_df["strategy_pct"] = temp_df.apply(apply_strategy, axis=1, args=(row["x"], row["y"]))
            sim_df = simulate(temp_df)
            plt.plot(sim_df["entry_date"], sim_df["capital"], label=f'x={row["x"]}, y={row["y"]}')

        plt.title(f"[{sector} | {group}] Top 5 전략 누적 수익률")
        plt.xlabel("날짜")
        plt.ylabel("누적 자산")
        plt.legend()
        plt.tight_layout()
        plt.show()
