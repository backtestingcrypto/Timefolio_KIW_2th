import pandas as pd
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine
from shared.connect_postgresql import get_connection

# 1. 환경변수 로드
load_dotenv()

# 2. 엑셀 파일 불러오기
df = pd.read_excel("투자가능종목.xlsx")

df['숫자코드'] = df['숫자코드'].astype(str).str.zfill(6)
# 3. 컬럼명 DB에 맞게 변환
df = df.rename(columns={
    '섹터코드': 'sector_code',
    '섹터명': 'sector_name',
    '종목코드': 'ticker',
    '종목명': 'name',
    '숫자코드': 'ticker_numeric'
})

# 4. get_connection() 기반 SQLAlchemy 엔진 생성
conn = get_connection()
engine = create_engine(f'postgresql+psycopg2://', creator=lambda: conn)

# 5. PostgreSQL에 데이터 저장
df.to_sql("kr_stock_data", engine, if_exists="append", index=False)

print("✅ kr_stock_data 테이블에 데이터 저장 완료")
