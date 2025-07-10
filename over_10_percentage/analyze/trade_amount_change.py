import pandas as pd
import matplotlib.pyplot as plt
import sys
import os

# 경로 설정
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from shared.connect_postgresql import get_connection

# 한글 폰트 설정
plt.rcParams['font.family'] = 'Malgun Gothic'
plt.rcParams['axes.unicode_minus'] = False

# ✅ 분석할 수익률 컬럼
target_column = "next_close_pct"  # 👉 여기를 바꾸면 다른 칼럼도 분석 가능!

# ✅ 데이터 로딩
conn = get_connection()
query = f"""
SELECT date AS entry_date, ticker, {target_column}, trade_amount_change, bb_upper_break
FROM over_10_percentage
WHERE {target_column} IS NOT NULL AND trade_amount_change IS NOT NULL AND bb_upper_break IS NOT NULL
ORDER BY entry_date;
"""
df = pd.read_sql(query, conn)
conn.close()

df["entry_date"] = pd.to_datetime(df["entry_date"])
df = df[df[target_column].notna()]

# ✅ 시뮬레이션 함수
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

# ✅ 조건 조합 정의
ranges = [
    ("매우 낮음 (<0)", df["trade_amount_change"] < 0),
    ("보통 이하 (0~0.5)", (df["trade_amount_change"] >= 0) & (df["trade_amount_change"] < 0.5)),
    ("보통 이상 (0.5~1.5)", (df["trade_amount_change"] >= 0.5) & (df["trade_amount_change"] < 1.5)),
    ("급증 (≥1.5)", df["trade_amount_change"] >= 1.5),
]

bb_conditions = [
    ("상단 돌파", True),
    ("상단 돌파 아님", False),
]

# ✅ 시각화
plt.figure(figsize=(14, 8))

for range_label, range_cond in ranges:
    for bb_label, bb_val in bb_conditions:
        label = f"{range_label} & {bb_label}"
        df_filtered = df[range_cond & (df["bb_upper_break"] == bb_val)].copy()
        if not df_filtered.empty:
            result_df = simulate(df_filtered, target_column)
            final_return = result_df["capital"].iloc[-1]
            plt.plot(result_df["entry_date"], result_df["capital"],
                     label=f"{label} ({len(df_filtered)}회, {final_return:.2f}배)")

plt.title(f"{target_column} 기준 - 거래대금 변화 & 상단돌파 조합별 누적 수익률")
plt.xlabel("날짜")
plt.ylabel("누적 자산")
plt.legend()
plt.grid(True)
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
