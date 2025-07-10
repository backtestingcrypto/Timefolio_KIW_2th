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
target_column = "next_open_pct"  # 👉 여기만 바꾸면 됨! (예: "next_close_pct", "next_high_pct")

# ✅ 데이터 불러오기
conn = get_connection()
query = f"""
SELECT 
    date AS entry_date, 
    ticker, 
    {target_column}, 
    bb_upper_break, 
    vol_ma_break,
    sector_code
FROM over_10_with_sector
WHERE {target_column} IS NOT NULL
  AND bb_upper_break IS NOT NULL
  AND vol_ma_break IS NOT NULL
ORDER BY date;
"""
df = pd.read_sql(query, conn)
conn.close()

# ✅ 날짜 변환
df["entry_date"] = pd.to_datetime(df["entry_date"])

# ✅ 조건 필터링: 상단 돌파 & 거래량 돌파 + NaN 방지
df_filtered = df[
    (df["bb_upper_break"] == True) &
    (df["vol_ma_break"] == True) &
    (df[target_column].notna())
].copy()

# ✅ NaN 안전한 시뮬레이션 함수
def simulate(df_case, initial_capital=1, fee_rate=0.0015 * 2, max_ratio_per_ticker=0.1):
    capital = initial_capital
    capital_over_time = []
    grouped = df_case.groupby("entry_date")

    for date, group in grouped:
        group = group.dropna(subset=[target_column])
        group = group.groupby("ticker").mean(numeric_only=True).reset_index()

        if group.empty:
            capital_over_time.append((date, capital))
            continue

        n = len(group)
        invest_per_ticker = min(capital / n, capital * max_ratio_per_ticker)

        profit = 0
        for _, row in group.iterrows():
            pct = row[target_column]
            if pd.isna(pct):
                continue
            result = invest_per_ticker * (1 + pct / 100 - fee_rate)
            profit += result

        unused = capital - invest_per_ticker * n
        capital = profit + unused
        capital_over_time.append((date, capital))

    return pd.DataFrame(capital_over_time, columns=["entry_date", "capital"])

# ✅ 섹터 코드 목록 추출 및 정렬
sector_codes = df_filtered["sector_code"].dropna().unique()
sector_codes.sort()

# ✅ 섹터별 시뮬레이션 및 시각화
plt.figure(figsize=(14, 8))

for sector in sector_codes:
    sector_df = df_filtered[df_filtered["sector_code"] == sector]
    if sector_df.empty:
        continue

    result_df = simulate(sector_df)

    if not result_df.empty and not pd.isna(result_df["capital"].iloc[-1]):
        final_return = result_df["capital"].iloc[-1]
        label_text = f"{sector} ({len(sector_df)}회, {final_return:.2f}배)"
    else:
        label_text = f"{sector} ({len(sector_df)}회, 계산불가)"

    plt.plot(result_df["entry_date"], result_df["capital"], label=label_text)

plt.title(f"[{target_column}] 상단+거래량 돌파 조건: 섹터별 누적 수익률 비교")
plt.xlabel("날짜")
plt.ylabel("누적 자산")
plt.legend()
plt.grid(True)
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
