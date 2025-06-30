from shared.connect_postgresql import get_connection

def create_kr_stock_data_table():
    query_stock = """
    CREATE TABLE IF NOT EXISTS kr_stock_data (
        sector_code TEXT,
        sector_name TEXT,
        ticker TEXT,
        name TEXT,
        ticker_numeric TEXT PRIMARY KEY,
        market_cap BIGINT
    );
    """

    query_daily_details_stocks = """
    CREATE TABLE IF NOT EXISTS daily_details_stocks (
        ticker TEXT NOT NULL,
        date DATE NOT NULL,
        open NUMERIC,
        high NUMERIC,
        low NUMERIC,
        close NUMERIC,
        volume BIGINT,
        foreign_limit_ratio NUMERIC,
        trade_amount NUMERIC,
        avg_trade_amount_4d NUMERIC,
        avg_trade_amount_5d NUMERIC,
        daily_market_cap NUMERIC,
        rsi_14 NUMERIC,
        bb_upper_20 NUMERIC,
        bb_lower_20 NUMERIC,
        vol_ma_5 NUMERIC,
        vol_ma_20 NUMERIC,
        price_ma_5 NUMERIC,
        price_ma_20 NUMERIC,
        price_ma_60 NUMERIC,
        price_ma_120 NUMERIC,
        PRIMARY KEY (ticker, date)
    );
    """

    conn = get_connection()
    cur = conn.cursor()

    cur.execute(query_stock)
    cur.execute(query_daily_details_stocks)

    conn.commit()
    cur.close()
    conn.close()

    print("✅ kr_stock_data 테이블 생성 완료")
    print("✅ daily_details_stocks 테이블 생성 완료")

if __name__ == "__main__":
    create_kr_stock_data_table()
