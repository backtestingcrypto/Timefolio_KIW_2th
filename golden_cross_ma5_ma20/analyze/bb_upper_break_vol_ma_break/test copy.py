import pandas as pd
import matplotlib.pyplot as plt
import sys
import os

# ✅ 경로 설정
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))
from shared.connect_postgresql import get_connection

# ✅ 한글 폰트 설정
plt.rcParams['font.family'] = 'Malgun Gothic'
plt.rcParams['axes.unicode_minus'] = False

# ✅ DB에서 데이터 불러오기
conn = get_connection()
query = """
SELECT entry_date, ticker, next_open_pct, bb_upper_break, vol_ma_break, daily_market_cap
FROM golden_cross_ma5_ma20
WHERE next_open_pct IS NOT NULL
  AND bb_upper_break = TRUE
  AND vol_ma_break = TRUE
  AND daily_market_cap IS NOT NULL
  AND daily_market_cap < 300000000000
ORDER BY entry_date;
"""
df = pd.read_sql(query, conn)
conn.close()

# ✅ 시뮬레이션 함수 정의
def simulate(df_case, initial_capital=1, fee_rate=0.0015 * 2, max_ratio_per_ticker=0.5):
    capital = initial_capital
    capital_over_time = []
    grouped = df_case.groupby("entry_date")

    for date, group in grouped:
        group = group[group["next_open_pct"].notna()]
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
            if pd.isna(pct):
                continue
            result = invest_per_ticker * (1 + pct / 100 - fee_rate)
            profit += result

        unused = capital - invest_per_ticker * n
        capital = profit + unused
        capital_over_time.append((date, capital))

    return pd.DataFrame(capital_over_time, columns=["entry_date", "capital"])

# ✅ 세분화된 시가총액 구간 정의 (단위: 원)
market_cap_bins = [
    (0, 50_000_000_000),      # 0 ~ 500억
    (50_000_000_000, 100_000_000_000),   # 500억 ~ 1,000억
    (100_000_000_000, 150_000_000_000),  # 1,000억 ~ 1,500억
    (150_000_000_000, 200_000_000_000),  # 1,500억 ~ 2,000억
    (200_000_000_000, 250_000_000_000),  # 2,000억 ~ 2,500억
    (250_000_000_000, 300_000_000_000),  # 2,500억 ~ 3,000억
]

labels = [
    "0~500억", "500~1,000억", "1,000~1,500억",
    "1,500~2,000억", "2,000~2,500억", "2,500~3,000억"
]

# ✅ 시각화
plt.figure(figsize=(14, 8))
for (low, high), label in zip(market_cap_bins, labels):
    df_filtered = df[(df["daily_market_cap"] >= low) & (df["daily_market_cap"] < high)].copy()

    if not df_filtered.empty:
        result_df = simulate(df_filtered)
        plt.plot(result_df["entry_date"], result_df["capital"],
                 label=f"{label} (매매 회수: {len(df_filtered)}회)")

plt.title("시가총액 0~3천억 내 500억 단위 구간별 누적 수익률 (상단+거래량 돌파)")
plt.xlabel("날짜")
plt.ylabel("누적 자산")
plt.legend()
plt.grid(True)
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
