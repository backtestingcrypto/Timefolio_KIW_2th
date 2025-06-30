import pandas as pd
import matplotlib.pyplot as plt
from shared.connect_postgresql import get_connection

# 한글 폰트 설정
plt.rcParams['font.family'] = 'Malgun Gothic'
plt.rcParams['axes.unicode_minus'] = False

# ✅ 분석 기간 지정
start_date = '2025-01-01'
end_date = '2025-06-30'

# ✅ 비교할 max_ratio_per_ticker 값 리스트
ratios = [0.1, 0.2, 0.25, 0.33, 0.5, 1.0]

# PostgreSQL 데이터 불러오기
conn = get_connection()
query = f"""
SELECT entry_date, ticker, next_open_pct, bb_upper_break, vol_ma_break, next_high_pct
FROM test
WHERE next_high_pct IS NOT NULL
  AND entry_date BETWEEN '{start_date}' AND '{end_date}'
ORDER BY entry_date;
"""
df = pd.read_sql(query, conn)
conn.close()

# ✅ 사용할 조건 정의 (예시: TT만)
condition = (df["bb_upper_break"] == True) & (df["vol_ma_break"] == True)
df_case = df[condition].copy()

# 시뮬레이션 함수
def simulate(df_case, initial_capital=100, fee_rate=0.0015 * 2, max_ratio_per_ticker=0.2):
    capital = initial_capital
    capital_over_time = []
    total_trades = 0

    grouped = df_case.groupby("entry_date")

    for date, group in grouped:
        group = group.groupby("ticker").mean(numeric_only=True).reset_index()
        n = len(group)
        if n == 0:
            capital_over_time.append((date, capital))
            continue

        raw_invest = capital / n
        invest_per_ticker = min(raw_invest, capital * max_ratio_per_ticker)

        profit = 0
        for _, row in group.iterrows():
            pct = row["next_high_pct"]
            result = invest_per_ticker * (1 + pct / 100 - fee_rate)
            profit += result
            total_trades += 1

        unused = capital - invest_per_ticker * n
        capital = profit + unused
        capital_over_time.append((date, capital))

    return pd.DataFrame(capital_over_time, columns=["entry_date", "capital"]), total_trades

# ✅ 각 max_ratio_per_ticker에 대해 시뮬레이션 실행
results = {}
trade_counts = {}
for ratio in ratios:
    label = f"비중 {ratio:.2f}"
    result_df, total_trades = simulate(df_case, max_ratio_per_ticker=ratio)
    results[label] = result_df
    trade_counts[label] = total_trades

# ✅ 누적 수익률 그래프
plt.figure(figsize=(14, 8))
for label, result_df in results.items():
    trades = trade_counts[label]
    plt.plot(result_df["entry_date"], result_df["capital"], label=f"{label} (매매 {trades}회)")

plt.title(f"max_ratio_per_ticker별 누적 수익률 비교 ({start_date} ~ {end_date}) - TT 조건")
plt.xlabel("날짜")
plt.ylabel("자산")
plt.legend(ncol=2)
plt.grid(True)
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
