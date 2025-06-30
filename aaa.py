import pandas as pd
from shared.connect_postgresql import get_connection

def analyze_rsi_bins():
    # PostgreSQL 연결
    conn = get_connection()
    
    # 1. 데이터 불러오기
    query = """
        SELECT vol_ma_break, bb_upper_break, rsi_14, next_high_pct, next_open_pct
        FROM strategy_advance_analysis
        WHERE next_high_pct IS NOT NULL AND next_open_pct IS NOT NULL AND rsi_14 IS NOT NULL
    """
    df = pd.read_sql(query, conn)
    conn.close()

    # 2. RSI를 5 단위 구간으로 나누기 (0~5, 5~10, ..., 95~100)
    df['rsi_bin'] = (df['rsi_14'] // 5 * 5).astype(int)

    # 3. 그룹핑 및 평균 계산
    grouped = df.groupby(['vol_ma_break', 'bb_upper_break', 'rsi_bin'], dropna=False).agg(
        count=('next_high_pct', 'count'),
        avg_next_high_pct=('next_high_pct', 'mean'),
        avg_next_open_pct=('next_open_pct', 'mean')
    ).reset_index().sort_values(by=['vol_ma_break', 'bb_upper_break', 'rsi_bin'])

    # 4. 결과 출력
    print("📊 RSI별 평균 next_high_pct 및 next_open_pct (4조건 분할)")
    print(grouped)

if __name__ == "__main__":
    analyze_rsi_bins()
