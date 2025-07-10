import pandas as pd
import matplotlib.pyplot as plt
import sys
import os

# ✅ 경로 설정
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from shared.connect_postgresql import get_connection

# ✅ 폰트 설정 (윈도우 환경에서 한글 깨짐 방지)
plt.rcParams['font.family'] = 'Malgun Gothic'
plt.rcParams['axes.unicode_minus'] = False

# ✅ 분석할 컬럼 지정 (다음날 시가 기준 수익률, 고가 수익률 등 자유롭게 변경)
target_column = "next_close_pct"  # 예: "next_high_pct", "next_close_pct" 등

# ✅ 데이터 불러오기
conn = get_connection()
query = f"""
SELECT date AS entry_date, ticker, {target_column}, ma_order
FROM over_10_percentage
WHERE {target_column} IS NOT NULL
ORDER BY entry_date;
"""
df = pd.read_sql(query, conn)
conn.close()

# ✅ 날짜 정렬 및 전처리
df["entry_date"] = pd.to_datetime(df["entry_date"])
df = df[df[target_column].notna()]

# ✅ 시뮬레이션 함수 정의
def simulate(df_case, target_column, initial_capital=1, fee_rate=0.0015 * 2, max_ratio_per_ticker=0.5):
    capital = initial_capital
    capital_over_time = []
    grouped = df_case.groupby("entry_date")

    for date, group in grouped:
        group = group.groupby("ticker").mean(numeric_only=True).reset_index()
        n = len(group)
        if n == 0:
            capital_over_time.append((date, capital))
            continue

        invest_per_ticker = min(capital / n, capital * max_ratio_per_ticker)
        profit = 0
        for _, row in group.iterrows():
            pct = row[target_column]
            result = invest_per_ticker * (1 + pct / 100 - fee_rate)
            profit += result

        unused = capital - invest_per_ticker * n
        capital = profit + unused
        capital_over_time.append((date, capital))

    return pd.DataFrame(capital_over_time, columns=["entry_date", "capital"])

# ✅ 상위 ma_order 조합 선택
top_orders = df["ma_order"].value_counts().head(24).index.tolist()
# 특정 조합 제외 예시
# top_orders = [o for o in top_orders if o != '20>5>60>120']

# ✅ 그래프 시각화
plt.figure(figsize=(14, 8))
for ma_order in top_orders:
    df_filtered = df[df["ma_order"] == ma_order].copy()
    if not df_filtered.empty:
        result_df = simulate(df_filtered, target_column)
        final_return = result_df["capital"].iloc[-1]
        plt.plot(result_df["entry_date"], result_df["capital"],
                 label=f"{ma_order} ({len(df_filtered)}회, 최종 {final_return:.2f}배)")

plt.title(f"ma_order별 누적 수익률 비교 - 기준: {target_column}")
plt.xlabel("날짜")
plt.ylabel("누적 자산")
plt.legend()
plt.grid(True)
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
