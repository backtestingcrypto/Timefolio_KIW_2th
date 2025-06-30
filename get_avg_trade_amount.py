import pandas as pd
from tqdm import tqdm
from shared.connect_postgresql import get_connection

# 1. 데이터 불러오기
def fetch_data():
    conn = get_connection()
    df = pd.read_sql("SELECT ticker, date, trade_amount FROM daily_details_stocks", conn)
    conn.close()
    return df

# 2. rolling 평균 계산
def calculate_avg_trade_amount(df):
    df = df.sort_values(['ticker', 'date']).copy()
    df['avg_trade_amount_4d'] = df.groupby('ticker')['trade_amount'].transform(lambda x: x.rolling(window=4, min_periods=1).mean())
    df['avg_trade_amount_5d'] = df.groupby('ticker')['trade_amount'].transform(lambda x: x.rolling(window=5, min_periods=1).mean())
    return df

# 3. 결과 업데이트
def update_averages(df):
    conn = get_connection()
    cur = conn.cursor()

    for _, row in tqdm(df.iterrows(), total=len(df)):
        if pd.notnull(row['avg_trade_amount_4d']) or pd.notnull(row['avg_trade_amount_5d']):
            cur.execute("""
                UPDATE daily_details_stocks
                SET avg_trade_amount_4d = %s,
                    avg_trade_amount_5d = %s
                WHERE ticker = %s AND date = %s
            """, (
                row['avg_trade_amount_4d'],
                row['avg_trade_amount_5d'],
                row['ticker'],
                row['date']
            ))

    conn.commit()
    cur.close()
    conn.close()
    print("✅ 평균 거래대금 업데이트 완료")

# 실행
if __name__ == "__main__":
    df = fetch_data()
    df = calculate_avg_trade_amount(df)
    update_averages(df)
