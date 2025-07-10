import pandas as pd
import matplotlib.pyplot as plt
import sys
import os

# ✅ 경로 설정
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from shared.connect_postgresql import get_connection

# ✅ 한글 폰트 설정
plt.rcParams['font.family'] = 'Malgun Gothic'
plt.rcParams['axes.unicode_minus'] = False

# ✅ DB에서 데이터 불러오기
conn = get_connection()
query = """
SELECT entry_date, ticker, next_open_pct, bb_upper_break, vol_ma_break
FROM golden_cross_ma5_ma20
WHERE next_open_pct != 'NaN'
AND bb_upper_break IS NOT NULL
AND vol_ma_break IS NOT NULL
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
        # ✅ next_open_pct NaN 제거
        group = group[group["next_open_pct"].notna()]

        # ✅ 종목별 평균 수익률 계산
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

# ✅ 조건별 시뮬레이션 조건 정의
conditions = [
    ("A. 상단돌파 & 거래량돌파", (True, True)),
    ("B. 상단돌파만", (True, False)),
    ("C. 거래량돌파만", (False, True)),
    ("D. 둘 다 없음", (False, False)),
]

# ✅ 시뮬레이션 및 시각화
plt.figure(figsize=(14, 8))

for label, (bb, vol) in conditions:
    df_filtered = df[
        (df["bb_upper_break"] == bb) & (df["vol_ma_break"] == vol)
    ].copy()

    if not df_filtered.empty:
        result_df = simulate(df_filtered)
        plt.plot(result_df["entry_date"], result_df["capital"],
                 label=f"{label} (매매 회수: {len(df_filtered)}회)")

plt.title("조건별 누적 수익률 비교")
plt.xlabel("날짜")
plt.ylabel("누적 자산")
plt.legend()
plt.grid(True)
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
