import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
from tqdm import tqdm
from itertools import product
import platform
import sys
import os

# ✅ 한글 폰트 설정
if platform.system() == "Windows":
    font_path = "C:/Windows/Fonts/malgun.ttf"
    font_prop = fm.FontProperties(fname=font_path)
    plt.rcParams["font.family"] = font_prop.get_name()
else:
    plt.rcParams["font.family"] = "NanumGothic"

# ✅ 경로 설정 (Jupyter나 인터프리터 호환)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from shared.connect_postgresql import get_connection
from shared.En_config import schema_name, table_name, full_table_name

# ✅ 날짜 필터
start_date = "2024-01-01"
end_date = "2025-06-30"

# ✅ 조건 컬럼
bool_columns = ["bb_upper_break", "vol_ma_break", "vol_gt_ma5", "vol_gt_ma20"]

# ✅ 시가총액 구간
market_cap_bins = [
    0, 500e8, 1000e8, 1500e8, 2000e8, 2500e8, 3000e8, 3500e8, 4000e8, 4500e8, 5000e8,
    6000e8, 7000e8, 8000e8, 9000e8, 10000e8,
    12000e8, 14000e8, 16000e8, 18000e8, 20000e8,
    25000e8, 30000e8, 35000e8, 40000e8, 45000e8, 50000e8,
    float("inf")
]
market_cap_labels = [
    "0-500억", "500-1000억", "1000-1500억", "1500-2000억", "2000-2500억", "2500-3000억",
    "3000-3500억", "3500-4000억", "4000-4500억", "4500-5000억",
    "5000-6000억", "6000-7000억", "7000-8000억", "8000-9000억", "9000-1조",
    "1조-1.2조", "1.2조-1.4조", "1.4조-1.6조", "1.6조-1.8조", "1.8조-2조",
    "2조-2.5조", "2.5조-3조", "3조-3.5조", "3.5조-4조", "4조-4.5조", "4.5조-5조",
    "5조 이상"
]

# ✅ 등락률 및 RSI 구간
pct_bins = list(range(-30, 33, 3))
pct_labels = [f"{i}~{i+3}%" for i in pct_bins[:-1]]

rsi_bins = list(range(0, 110, 10))
rsi_labels = [f"{i}~{i+10}" for i in rsi_bins[:-1]]

# ✅ ma_order 그룹
ma_order_groups = {
    "2단": ["5>20", "20>5"],
    "3단": ["5>20>60", "5>60>20", "20>5>60", "20>60>5", "60>5>20", "60>20>5"],
    "4단": [
        "5>20>60>120", "5>20>120>60", "5>60>20>120", "5>60>120>20",
        "5>120>20>60", "5>120>60>20", "20>5>60>120", "20>5>120>60",
        "20>60>5>120", "20>60>120>5", "20>120>5>60", "20>120>60>5",
        "60>5>20>120", "60>5>120>20", "60>20>5>120", "60>20>120>5",
        "60>120>5>20", "60>120>20>5", "120>5>20>60", "120>5>60>20",
        "120>20>5>60", "120>20>60>5", "120>60>5>20", "120>60>20>5"
    ]
}

# ✅ 데이터 로딩
conn = get_connection()
query = f"""
    SELECT date, ticker_numeric, pct_change_close, pct_change_next_close, pct_change_next2_close,
           bb_upper_break, vol_ma_break, vol_gt_ma5, vol_gt_ma20, ma_order,
           rsi_14, daily_market_cap
    FROM {schema_name}.{table_name}
    WHERE pct_change_next2_close IS NOT NULL
      AND ma_order IS NOT NULL
      AND daily_market_cap IS NOT NULL
      AND pct_change_close IS NOT NULL
      AND rsi_14 IS NOT NULL
      AND date BETWEEN '{start_date}' AND '{end_date}'
"""
df = pd.read_sql(query, conn)
conn.close()

# ✅ 구간 라벨링
df["market_cap_group"] = pd.cut(df["daily_market_cap"], bins=market_cap_bins, labels=market_cap_labels)
df["pct_group"] = pd.cut(df["pct_change_close"], bins=pct_bins, labels=pct_labels)
df["rsi_group"] = pd.cut(df["rsi_14"], bins=rsi_bins, labels=rsi_labels)

# ✅ 분석 및 그래프
for cap_group in df["market_cap_group"].dropna().unique():
    df_cap = df[df["market_cap_group"] == cap_group]
    print(f"\n🧮 시가총액 구간: {cap_group} — 총 데이터: {len(df_cap)}")

    for group_name, ma_orders in ma_order_groups.items():
        results = []

        for ma_order in tqdm(ma_orders, desc=f"📊 {group_name} 조건 @ {cap_group}"):
            df_ma = df_cap[df_cap["ma_order"] == ma_order]
            if df_ma.empty:
                continue

            for combo in product([True, False], repeat=len(bool_columns)):
                for pct_group in df_ma["pct_group"].dropna().unique():
                    for rsi_group in df_ma["rsi_group"].dropna().unique():
                        condition = " & ".join([f"{col} == {val}" for col, val in zip(bool_columns, combo)])
                        subset = df_ma.query(
                            f"{condition} and pct_group == @pct_group and rsi_group == @rsi_group"
                        )

                        if len(subset) >= 5:
                            results.append({
                                "ma_order": ma_order,
                                **dict(zip(bool_columns, combo)),
                                "pct_group": pct_group,
                                "rsi_group": rsi_group,
                                "count": len(subset),
                                "avg_pct_change_next_close": round(subset["pct_change_next_close"].mean(), 2),
                                "avg_pct_change_next2_close": round(subset["pct_change_next2_close"].mean(), 2)
                            })

        if results:
            result_df = pd.DataFrame(results).sort_values(by="avg_pct_change_next_close", ascending=False)
            top5 = result_df.head(5)

            for _, row in top5.iterrows():
                ma_order = row["ma_order"]
                pct_group = row["pct_group"]
                rsi_group = row["rsi_group"]
                condition = " & ".join([f"{col} == {row[col]}" for col in bool_columns])
                subset = df_cap.query(
                    f"(ma_order == '{ma_order}') and pct_group == @pct_group and rsi_group == @rsi_group and {condition}"
                ).copy()

                subset = subset.sort_values(by="date")
                subset["date"] = pd.to_datetime(subset["date"])
                subset.set_index("date", inplace=True)

                subset["weighted_return"] = subset["pct_change_next_close"].clip(-0.5, 1.0).fillna(0)
                daily_return = subset["weighted_return"].groupby(level=0).mean()
                cumulative = (1 + daily_return).cumprod()

                label = f"{ma_order} | " + ", ".join([f"{col}={row[col]}" for col in bool_columns])
                label += f" | {pct_group} | RSI: {rsi_group}"
                plt.plot(cumulative.index, cumulative.values, label=label)

            plt.title(f"📈 누적 수익률 조합 (시총: {cap_group}, 그룹: {group_name})")
            plt.xlabel("날짜")
            plt.ylabel("누적 수익률")
            plt.legend(loc="upper left", fontsize=8)
            plt.grid(True)
            plt.tight_layout()
            plt.show()
        else:
            print(f"⚠️ {group_name} 그룹 @ {cap_group} — 유효한 결과 없음")
