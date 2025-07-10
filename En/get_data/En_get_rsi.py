import pandas as pd
import sys
import os

# ✅ 경로 설정
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from shared.connect_postgresql import get_connection
from shared.En_config import schema_name, table_name, full_table_name

def calculate_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()

    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)

    avg_gain = gain.rolling(window=period, min_periods=period).mean()
    avg_loss = loss.rolling(window=period, min_periods=period).mean()

    rsi = pd.Series(index=series.index, dtype=float)

    for i in range(period, len(series)):
        if i == period:
            prev_avg_gain = avg_gain.iloc[period - 1]
            prev_avg_loss = avg_loss.iloc[period - 1]
        else:
            prev_avg_gain = (prev_avg_gain * (period - 1) + gain.iloc[i]) / period
            prev_avg_loss = (prev_avg_loss * (period - 1) + loss.iloc[i]) / period

        if prev_avg_loss == 0:
            rsi.iloc[i] = 100
        else:
            rs = prev_avg_gain / prev_avg_loss
            rsi.iloc[i] = 100 - (100 / (1 + rs))

    return rsi.round(2)

def update_rsi_to_en_table():
    conn = get_connection()
    cur = conn.cursor()

    # ✅ rsi_14 컬럼 추가 (없으면)
    cur.execute(f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_schema = '{schema_name}' AND table_name = '{table_name}' AND column_name = 'rsi_14'
            ) THEN
                ALTER TABLE {full_table_name} ADD COLUMN rsi_14 FLOAT;
            END IF;
        END$$;
    """)
    conn.commit()

    # ✅ ticker 목록 조회
    cur.execute(f"SELECT DISTINCT ticker_numeric FROM {full_table_name};")
    tickers = [row[0] for row in cur.fetchall()]

    for ticker in tickers:
        print(f"📈 {ticker} RSI 계산 중...")

        df = pd.read_sql(f"""
            SELECT date, close
            FROM {full_table_name}
            WHERE ticker_numeric = %s
            ORDER BY date
        """, conn, params=(ticker,))

        if df.empty or len(df) < 15:
            print(f"⚠️ {ticker}: 데이터 부족")
            continue

        df["rsi_14"] = calculate_rsi(df["close"], period=14)

        for _, row in df.iterrows():
            if pd.isna(row["rsi_14"]):
                continue
            cur.execute(f"""
                UPDATE {full_table_name}
                SET rsi_14 = %s
                WHERE ticker_numeric = %s AND date = %s;
            """, (row["rsi_14"], ticker, row["date"]))

        conn.commit()
        print(f"✅ {ticker} 완료")

    cur.close()
    conn.close()
    print(f"\n🎉 {full_table_name} 모든 종목 RSI 저장 완료")

if __name__ == "__main__":
    update_rsi_to_en_table()
