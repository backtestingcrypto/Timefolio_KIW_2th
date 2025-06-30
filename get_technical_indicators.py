import pandas as pd
from tqdm import tqdm
from shared.connect_postgresql import get_connection

# RSI 계산 함수
def compute_rsi(series, period=14):
    delta = series.diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)
    ma_up = up.rolling(window=period).mean()
    ma_down = down.rolling(window=period).mean()
    rs = ma_up / ma_down
    rsi = 100 - (100 / (1 + rs))
    return rsi

def fetch_data():
    conn = get_connection()
    df = pd.read_sql("SELECT ticker, date, close, volume FROM daily_details_stocks", conn)
    conn.close()
    return df

def calculate_indicators(df):
    df = df.sort_values(['ticker', 'date']).copy()

    result = []
    for ticker, group in tqdm(df.groupby('ticker'), desc="📈 종목별 처리"):
        g = group.copy()
        g['rsi_14'] = compute_rsi(g['close'], 14)
        g['bb_middle'] = g['close'].rolling(window=20).mean()
        g['bb_std'] = g['close'].rolling(window=20).std()
        g['bb_upper_20'] = g['bb_middle'] + 2 * g['bb_std']
        g['bb_lower_20'] = g['bb_middle'] - 2 * g['bb_std']
        g['vol_ma_5'] = g['volume'].rolling(window=5).mean()
        g['vol_ma_20'] = g['volume'].rolling(window=20).mean()
        for window in [5, 20, 60, 120]:
            g[f'price_ma_{window}'] = g['close'].rolling(window=window).mean()
        result.append(g)

    df_all = pd.concat(result)
    return df_all

def update_indicators(df):
    conn = get_connection()
    cur = conn.cursor()

    for _, row in tqdm(df.iterrows(), total=len(df)):
        cur.execute("""
            UPDATE daily_details_stocks
            SET
                rsi_14 = %s,
                bb_upper_20 = %s,
                bb_lower_20 = %s,
                vol_ma_5 = %s,
                vol_ma_20 = %s,
                price_ma_5 = %s,
                price_ma_20 = %s,
                price_ma_60 = %s,
                price_ma_120 = %s
            WHERE ticker = %s AND date = %s
        """, (
            row['rsi_14'], row['bb_upper_20'], row['bb_lower_20'],
            row['vol_ma_5'], row['vol_ma_20'],
            row['price_ma_5'], row['price_ma_20'],
            row['price_ma_60'], row['price_ma_120'],
            row['ticker'], row['date']
        ))

    conn.commit()
    cur.close()
    conn.close()
    print("✅ 지표 업데이트 완료")

if __name__ == "__main__":
    df = fetch_data()
    df_ind = calculate_indicators(df)
    update_indicators(df_ind)
