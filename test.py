import pandas as pd
from tqdm import tqdm
from shared.connect_postgresql import get_connection

def fetch_data():
    conn = get_connection()
    df = pd.read_sql("SELECT * FROM daily_details_stocks", conn)
    conn.close()
    return df.sort_values(["ticker", "date"])

def calculate_advance_strategy(df):
    results = []

    for ticker, group in tqdm(df.groupby("ticker"), desc="📈 종목별 분석"):
        group = group.reset_index(drop=True)
        for i in range(1, len(group) - 1):  # 마지막 날은 next_day 없음
            today = group.loc[i]
            yesterday = group.loc[i - 1]
            next_day = group.loc[i + 1]

            # 조건: 5일선이 20일선 돌파 + 유동성/시총 필터
            if (
                pd.notnull(today['price_ma_5']) and pd.notnull(today['price_ma_20']) and
                pd.notnull(today['avg_trade_amount_5d']) and pd.notnull(today['daily_market_cap']) and
                yesterday['price_ma_5'] <= yesterday['price_ma_20'] and
                today['price_ma_5'] > today['price_ma_20'] and
                today['avg_trade_amount_5d'] >= 3_000_000_000 and
                today['daily_market_cap'] < 1_000_000_000_000
            ):
                close_price = today['close']
                next_open = next_day['open']
                next_high = next_day['high']

                next_open_pct = (next_open - close_price) / close_price * 100 if pd.notnull(next_open) else None
                next_high_pct = (next_high - close_price) / close_price * 100 if pd.notnull(next_high) else None

                bb_upper_break = bool(today['close'] > today['bb_upper_20']) if pd.notnull(today['bb_upper_20']) else None
                vol_ma_break = bool(today['vol_ma_5'] > today['vol_ma_20']) if pd.notnull(today['vol_ma_5']) and pd.notnull(today['vol_ma_20']) else None

                trade_amount_change = (
                    (today['trade_amount'] - yesterday['trade_amount']) / yesterday['trade_amount'] * 100
                    if pd.notnull(today['trade_amount']) and pd.notnull(yesterday['trade_amount']) and yesterday['trade_amount'] != 0 else None
                )

                results.append({
                    "ticker": ticker,
                    "entry_date": today['date'],
                    "close_price": close_price,
                    "next_open_pct": next_open_pct,
                    "next_high_pct": next_high_pct,
                    "rsi_14": today['rsi_14'],
                    "bb_upper_break": bb_upper_break,
                    "vol_ma_break": vol_ma_break,
                    "price_ma_60": today['price_ma_60'],
                    "price_ma_120": today['price_ma_120'],
                    "trade_amount_change": trade_amount_change
                })

    return pd.DataFrame(results)

def insert_results(df):
    conn = get_connection()
    cur = conn.cursor()

    for _, row in tqdm(df.iterrows(), total=len(df)):
        cur.execute("""
            INSERT INTO test (
                ticker, entry_date, close_price,
                next_open_pct, next_high_pct,
                rsi_14, bb_upper_break, vol_ma_break,
                price_ma_60, price_ma_120, trade_amount_change
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (ticker, entry_date) DO NOTHING;
        """, (
            row['ticker'], row['entry_date'], row['close_price'],
            row['next_open_pct'], row['next_high_pct'],
            row['rsi_14'], row['bb_upper_break'], row['vol_ma_break'],
            row['price_ma_60'], row['price_ma_120'], row['trade_amount_change']
        ))

    conn.commit()
    cur.close()
    conn.close()
    print("✅ 전략 결과 저장 완료")

if __name__ == "__main__":
    df_all = fetch_data()
    df_result = calculate_advance_strategy(df_all)
    insert_results(df_result)
