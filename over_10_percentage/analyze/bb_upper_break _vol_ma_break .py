import pandas as pd
import matplotlib.pyplot as plt
import sys
import os

# 경로 설정
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from shared.connect_postgresql import get_connection

# ✅ 한글 폰트 설정 (Windows용)
plt.rcParams['font.family'] = 'Malgun Gothic'
plt.rcParams['axes.unicode_minus'] = False

# ✅ 데이터 불러오기
conn = get_connection()
query = """
SELECT date AS entry_date, ticker, next_open_pct, bb_upper_break, vol_ma_break
FROM over_10_percentage
WHERE next_open_pct IS NOT NULL
AND bb_upper_break IS NOT NULL AND vol_ma_break IS NOT NULL
ORDER BY entry_date;
"""
df = pd.read_sql(query, conn)
conn.close()

# ✅ 날짜 변환
df["entry_date"] = pd.to_datetime(df["entry_date"])
df = df[df["next_open_pct"].notna()]

# ✅ 시뮬레이션 함수
def simulate(df_case, initial_capital=1, fee_rate=0.0015 * 2, max_ratio_per_ticker=0.5):
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
            pct = row["next_open_pct"]
            result = invest_per_ticker * (1 + pct / 100 - fee_rate)
            profit += result

        unused = capital - invest_per_ticker * n
        capital = profit + unused
        capital_over_time.append((date, capital))

    return pd.DataFrame(capital_over_time, columns=["entry_date", "capital"])

# ✅ 조건 조합 정의
conditions = [
    ("A. 상단돌파 & 거래량돌파", True, True),
    ("B. 상단돌파만", True, False),
    ("C. 거래량돌파만", False, True),
    ("D. 둘 다 아님", False, False),
]

# ✅ 시뮬레이션 및 시각화
plt.figure(figsize=(14, 8))
for label, bb_val, vol_val in conditions:
    df_filtered = df[
        (df["bb_upper_break"] == bb_val) & (df["vol_ma_break"] == vol_val)
    ].copy()

    if not df_filtered.empty:
        result_df = simulate(df_filtered)
        final_return = result_df["capital"].iloc[-1]
        plt.plot(result_df["entry_date"], result_df["capital"],
                 label=f"{label} ({len(df_filtered)}회, 최종 {final_return:.2f}배)")

plt.title("상단/거래량 돌파 조합별 누적 수익률 비교")
plt.xlabel("날짜")
plt.ylabel("누적 자산")
plt.legend()
plt.grid(True)
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
