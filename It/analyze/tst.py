import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import sys
import os

# ✅ 경로 설정
try:
    current_dir = os.path.dirname(__file__)
except NameError:
    current_dir = os.getcwd()
sys.path.append(os.path.abspath(os.path.join(current_dir, "../..")))

from shared.connect_postgresql import get_connection
from shared.It_config import schema_name, table_name
import platform

# ✅ 한글 폰트 설정
if platform.system() == "Windows":
    plt.rcParams["font.family"] = fm.FontProperties(fname="C:/Windows/Fonts/malgun.ttf").get_name()
else:
    plt.rcParams["font.family"] = "NanumGothic"

# ✅ DB에서 조건에 맞는 데이터 조회
conn = get_connection()
query = f"""
    SELECT date, pct_change_close, pct_change_next_close,
           bb_upper_break, vol_ma_break, vol_gt_ma5, vol_gt_ma20,
           ma_order, rsi_14, daily_market_cap, avg_trade_amount_5d
    FROM {schema_name}.{table_name}
    WHERE pct_change_next_close IS NOT NULL
      AND pct_change_close IS NOT NULL
      AND rsi_14 IS NOT NULL
      AND daily_market_cap IS NOT NULL
      AND avg_trade_amount_5d IS NOT NULL
      AND ma_order = '5>20>60>120'
      AND bb_upper_break = false
      AND vol_ma_break = true
      AND vol_gt_ma5 = true
      AND vol_gt_ma20 = true
      AND pct_change_close >= -3 AND pct_change_close < 0
      AND rsi_14 >= 50 AND rsi_14 < 60
      AND daily_market_cap >= 300000000000 AND daily_market_cap < 350000000000
      AND avg_trade_amount_5d >= 3000000000
"""
df = pd.read_sql(query, conn)
conn.close()

# ✅ 필터링 결과 확인
total_trades = len(df)
print(f"🔎 조건에 부합하는 매매 횟수: {total_trades}회")
if df.empty:
    print("❌ 조건에 부합하는 데이터가 없습니다.")
    exit()

# ✅ 누적 수익률 계산
df = df.sort_values("date")
df["date"] = pd.to_datetime(df["date"])
df.set_index("date", inplace=True)
df["weighted_return"] = df["pct_change_next_close"].clip(-0.3, 0.3).fillna(0)

daily_return = df["weighted_return"].groupby(level=0).mean()
cumulative = (1 + daily_return).cumprod()

# ✅ 그래프 출력
plt.figure(figsize=(10, 5))
plt.plot(cumulative.index, cumulative.values, label="조건 만족 조합")
plt.title(f"📈 단일 조건 누적 수익률 (시총 3000~3500억, 4단, RSI 50~60, 매매횟수: {total_trades})")
plt.xlabel("날짜")
plt.ylabel("누적 수익률")
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.show()
