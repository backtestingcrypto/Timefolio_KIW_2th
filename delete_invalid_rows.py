from shared.connect_postgresql import get_connection

def delete_invalid_rows():
    conn = get_connection()
    cur = conn.cursor()

    # 1. market_cap이 NULL인 종목 삭제
    cur.execute("DELETE FROM kr_stock_data WHERE market_cap IS NULL;")
    deleted_null = cur.rowcount

    # 2. ticker_numeric이 950000 이상인 종목 삭제 (DR 등 제외)
    cur.execute("DELETE FROM kr_stock_data WHERE ticker_numeric::INTEGER >= 950000;")
    deleted_dr = cur.rowcount

    conn.commit()
    cur.close()
    conn.close()

    print(f"🗑️ market_cap이 NULL인 종목 {deleted_null}개 삭제 완료")
    print(f"🗑️ DR 등 ticker_numeric ≥ 950000인 종목 {deleted_dr}개 삭제 완료")

if __name__ == "__main__":
    delete_invalid_rows()
