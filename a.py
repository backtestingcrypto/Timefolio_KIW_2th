import pandas as pd
import matplotlib.pyplot as plt
from shared.connect_postgresql import get_connection

# ✅ 한글 폰트 설정
plt.rcParams['font.family'] = 'Malgun Gothic'
plt.rcParams['axes.unicode_minus'] = False

# ✅ PostgreSQL에서 데이터 불러오기
conn = get_connection()
query = """
SELECT entry_date, ticker, next_open_pct, bb_upper_break, vol_ma_break, rsi_14
FROM test
WHERE next_open_pct IS NOT NULL AND rsi_14 IS NOT NULL
ORDER BY entry_date;
"""
df = pd.read_sql(query, conn)
conn.close()

# ✅ 조건 필터링: bb_upper_break = True AND vol_ma_break = True AND RSI 50 이상 60 미만
df_filtered = df[
    (df["bb_upper_break"] == True) &
    (df["vol_ma_break"] == True) &
    (df["rsi_14"] >= 50) & (df["rsi_14"] < 100)
].copy()

# ✅ 시뮬레이션 함수 정의
def simulate(df_case, initial_capital=100, fee_rate=0.0015 * 2, max_ratio_per_ticker=0.5):
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
            pct = row["next_open_pct"]
            result = invest_per_ticker * (1 + pct / 100 - fee_rate)
            profit += result
            total_trades += 1

        unused = capital - invest_per_ticker * n
        capital = profit + unused
        capital_over_time.append((date, capital))

    return pd.DataFrame(capital_over_time, columns=["entry_date", "capital"]), total_trades

# ✅ 시뮬레이션 실행
if not df_filtered.empty:
    result_df, total_trades = simulate(df_filtered)

    # ✅ 누적 수익률 그래프 출력
    plt.figure(figsize=(14, 8))
    plt.plot(result_df["entry_date"], result_df["capital"], label=f"RSI 50–60 (매매 {total_trades}회)")
    plt.title("RSI 50–60 구간 누적 수익률 (bb_upper & vol_ma break 조건)")
    plt.xlabel("날짜")
    plt.ylabel("자산")
    plt.legend()
    plt.grid(True)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()
else:
    print("해당 조건을 만족하는 데이터가 없습니다.")
