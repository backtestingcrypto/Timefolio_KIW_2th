import pandas as pd
import sys
import os
from datetime import timedelta

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from shared.connect_postgresql import get_connection

def detect_ma_order(row):
    ma_values = {
        'ma5': row['price_ma_5'],
        'ma20': row['price_ma_20'],
        'ma60': row['price_ma_60'],
        'ma120': row['price_ma_120']
    }
    sorted_ma = sorted(ma_values.items(), key=lambda x: x[1], reverse=True)
    return ','.join([ma[0] for ma in sorted_ma])

def insert_golden_cross_data():
    conn = get_connection()
    cur = conn.cursor()

    df = pd.read_sql("""
        SELECT *
        FROM daily_details_stocks
        WHERE price_ma_5 IS NOT NULL AND price_ma_20 IS NOT NULL
        ORDER BY ticker, date
    """, conn)

    df['date'] = pd.to_datetime(df['date'])
    df.sort_values(['ticker', 'date'], inplace=True)

    df['price_ma_5_prev'] = df.groupby('ticker')['price_ma_5'].shift(1)
    df['price_ma_20_prev'] = df.groupby('ticker')['price_ma_20'].shift(1)
    df['prev_close'] = df.groupby('ticker')['close'].shift(1)
    df['rsi_14_prev'] = df.groupby('ticker')['rsi_14'].shift(1)

    # ✅ 골든크로스 + 거래대금 조건 필터링
    golden_cross_df = df[
        (df['price_ma_5_prev'] <= df['price_ma_20_prev']) &
        (df['price_ma_5'] > df['price_ma_20']) &
        (df['avg_trade_amount_5d'] >= 3_000_000_000)
    ].copy()

    # 다음날 데이터 병합
    next_day = df[['ticker', 'date', 'open', 'close', 'high', 'low']].copy()
    next_day['date'] = next_day['date'] - timedelta(days=1)
    next_day.rename(columns={
        'open': 'next_open',
        'close': 'next_close',
        'high': 'next_high',
        'low': 'next_low'
    }, inplace=True)

    golden_cross_df = pd.merge(golden_cross_df, next_day, on=['ticker', 'date'], how='left')

    # 파생변수 계산
    golden_cross_df['next_open_pct'] = (golden_cross_df['next_open'] - golden_cross_df['close']) / golden_cross_df['close'] * 100
    golden_cross_df['next_close_pct'] = (golden_cross_df['next_close'] - golden_cross_df['close']) / golden_cross_df['close'] * 100
    golden_cross_df['next_high_pct'] = (golden_cross_df['next_high'] - golden_cross_df['close']) / golden_cross_df['close'] * 100
    golden_cross_df['next_low_pct'] = (golden_cross_df['next_low'] - golden_cross_df['close']) / golden_cross_df['close'] * 100
    golden_cross_df['pct_close_vs_prev_close'] = (golden_cross_df['close'] - golden_cross_df['prev_close']) / golden_cross_df['prev_close'] * 100
    golden_cross_df['pct_high_vs_prev_close'] = (golden_cross_df['high'] - golden_cross_df['prev_close']) / golden_cross_df['prev_close'] * 100
    golden_cross_df['rsi_change'] = golden_cross_df['rsi_14'] - golden_cross_df['rsi_14_prev']
    golden_cross_df['rsi_change_pct'] = golden_cross_df['rsi_change'] / golden_cross_df['rsi_14_prev'] * 100
    golden_cross_df['ma_order'] = golden_cross_df.apply(detect_ma_order, axis=1)

    # ✅ 수정된 INSERT 쿼리
    insert_query = """
        INSERT INTO golden_cross_ma5_ma20 (
            ticker, entry_date, rsi_14, rsi_change, rsi_change_pct,
            bb_upper_break, vol_ma_break, trade_amount_change, trade_amount,
            avg_trade_amount_5d, daily_market_cap,  -- ✅ 추가
            next_open_pct, next_close_pct, next_high_pct, next_low_pct,
            pct_close_vs_prev_close, pct_high_vs_prev_close, ma_order
        ) VALUES (
            %(ticker)s, %(entry_date)s, %(rsi_14)s, %(rsi_change)s, %(rsi_change_pct)s,
            %(bb_upper_break)s, %(vol_ma_break)s, %(trade_amount_change)s, %(trade_amount)s,
            %(avg_trade_amount_5d)s, %(daily_market_cap)s,
            %(next_open_pct)s, %(next_close_pct)s, %(next_high_pct)s, %(next_low_pct)s,
            %(pct_close_vs_prev_close)s, %(pct_high_vs_prev_close)s, %(ma_order)s
        ) ON CONFLICT (ticker, entry_date) DO NOTHING;
    """

    for _, row in golden_cross_df.iterrows():
        data = {
            'ticker': row['ticker'],
            'entry_date': row['date'],
            'rsi_14': row['rsi_14'],
            'rsi_change': row['rsi_change'],
            'rsi_change_pct': row['rsi_change_pct'],
            'bb_upper_break': row['close'] > row['bb_upper_20'] if pd.notnull(row['bb_upper_20']) else None,
            'vol_ma_break': row['volume'] > row['vol_ma_20'] if pd.notnull(row['vol_ma_20']) else None,
            'trade_amount_change': (
                (row['trade_amount'] - row['avg_trade_amount_5d']) / row['avg_trade_amount_5d'] * 100
                if pd.notnull(row['avg_trade_amount_5d']) and row['avg_trade_amount_5d'] != 0 else None
            ),
            'trade_amount': row['trade_amount'],
            'avg_trade_amount_5d': row['avg_trade_amount_5d'],
            'daily_market_cap': row['daily_market_cap'],
            'next_open_pct': row['next_open_pct'],
            'next_close_pct': row['next_close_pct'],
            'next_high_pct': row['next_high_pct'],
            'next_low_pct': row['next_low_pct'],
            'pct_close_vs_prev_close': row['pct_close_vs_prev_close'],
            'pct_high_vs_prev_close': row['pct_high_vs_prev_close'],
            'ma_order': row['ma_order']
        }
        cur.execute(insert_query, data)

    conn.commit()
    cur.close()
    conn.close()
    print("✅ golden_cross_ma5_ma20 테이블에 데이터 저장 완료 (3000억 이상 거래대금 기준)")

if __name__ == "__main__":
    insert_golden_cross_data()
