import pandas as pd
from tqdm import tqdm
from shared.connect_postgresql import get_connection

def insert_contrarian_entries():
    conn = get_connection()
    df = pd.read_sql("SELECT * FROM daily_details_stocks ORDER BY ticker, date", conn)

    results = []

    for ticker, group in tqdm(df.groupby("ticker")):
        group = group.sort_values("date").reset_index(drop=True)

        for i in range(1, len(group)):
            row = group.loc[i]
            prev_row = group.loc[i - 1]

            if (
                row["avg_trade_amount_5d"] is not None
                and row["avg_trade_amount_5d"] >= 3_000_000_000
                and row["daily_market_cap"] is not None
                and row["daily_market_cap"] < 1_000_000_000_000
                and row["price_ma_5"] is not None
                and row["price_ma_20"] is not None
                and row["price_ma_5"] < row["price_ma_20"]
                and row["close"] is not None
                and row["price_ma_5"] < row["close"] < row["price_ma_20"]
            ):
                # 추가 정보
                trade_amount_change = None
                if prev_row["trade_amount"] and row["trade_amount"]:
                    if prev_row["trade_amount"] != 0:
                        trade_amount_change = round((row["trade_amount"] - prev_row["trade_amount"]) / prev_row["trade_amount"] * 100, 2)

                bb_upper_break = bool(row["close"] > row["bb_upper_20"]) if row["bb_upper_20"] is not None else None
                vol_ma_break = bool(row["vol_ma_5"] > row["vol_ma_20"]) if row["vol_ma_5"] is not None and row["vol_ma_20"] is not None else None

                results.append((
                    row["ticker"],
                    row["date"],
                    row["low"],
                    row["rsi_14"],
                    bb_upper_break,
                    vol_ma_break,
                    trade_amount_change
                ))

    # insert
    insert_query = """
    INSERT INTO strategy_contrarian_entry (
        ticker, entry_date, low, rsi_14, bb_upper_break, vol_ma_break, trade_amount_change
    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (ticker, entry_date) DO NOTHING;
    """
    cur = conn.cursor()
    cur.executemany(insert_query, results)
    conn.commit()
    cur.close()
    conn.close()
    print("✅ 데이터 저장 완료.")

if __name__ == "__main__":
    insert_contrarian_entries()
