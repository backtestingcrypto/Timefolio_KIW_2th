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

# ✅ DB에서 데이터 불러오기 (조건: 상단+거래량 돌파 & 시총 500억~1500억)
conn = get_connection()
query = """
SELECT next_high_pct
FROM golden_cross_ma5_ma20
WHERE next_high_pct IS NOT NULL
  AND bb_upper_break = TRUE
  AND vol_ma_break = TRUE
  AND daily_market_cap BETWEEN 50000000000 AND 150000000000;
"""
df = pd.read_sql(query, conn)
conn.close()

# ✅ 이상치 제거 (분포 확인용: -20% ~ +20%)
df = df[(df["next_high_pct"] >= -30) & (df["next_high_pct"] <= 30)]

# ✅ 히스토그램 시각화
plt.figure(figsize=(12, 6))
plt.hist(df["next_high_pct"], bins=50, edgecolor='black', alpha=0.7)
plt.title("next_high_pct 분포도 (시총 500억~1500억, 상단+거래량 돌파)")
plt.xlabel("next_high_pct (%)")
plt.ylabel("빈도수")
plt.grid(True)
plt.tight_layout()
plt.show()
