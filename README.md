docker run --name pg-kr-stock -e POSTGRES_USER=Rth2608 -e POSTGRES_PASSWORD=Rth2608 -e POSTGRES_DB=kr_stock_data -p 5432:5432 -d postgres:15

# 매수 불가 종목
5일 평균 거래대금 30억 이하

신규상장, 재상장 종목은 상장 후 6영업일부터 거래 가능 (주문 시 자동 체크) 최근 거래일 5일 미만 으로 에러 메세지 뜸

투자주의/경고/위험 종목, 투자주의환기 종목, 관리 종목 등 (주문 시 자동 체크)

# 운용 가이드라인
종목별 편입 한도 15% 이하 (단, 삼성전자는 40%)

코스피 및 코스닥 시장의 GICS 산업군 기준 섹터 비중 2배 이하
단, GICS 산업군 기준 섹터 비중이 5% 이하인 경우, 포트폴리오내 비중은 10%까지 편입 가능
예1) 소재 섹터의 시장 비중이 10%인 경우, 포트폴리오내 소재 섹터 비중은 20%까지 가능
예2) 에너지 섹터의 시장 비중이 2%인 경우, 포트폴리오내 에너지 섹터 비중은 10%까지 가능

시가총액 1조 미만 종목들의 합산 비중 40% 이하
※ 편입 한도는 주문시 자동 체크되며, 가격상승으로 인한 편입한도 초과는 허용



df = stock.get_shorting_balance_top50("20210127") : 조회 일자를 입력받아 공매도 비중이 높은 상위 50개 종목 정보를 반환
df = stock.get_shorting_balance_top50("20210129", market="KOSDAQ") : 코스닥 


df = stock.get_shorting_volume_top50("20210129") : 해당일 코스피 종목의 거래 비중 TOP 50을 DataFrame으로 반환
df = stock.get_shorting_volume_top50("20210129", "KOSDAQ") : 코스닥의 공매도 거래량

df = stock.get_shorting_balance_by_date("20190401", "20190405", "005930") : 시작일/종료일 두 개의 파라미터를 입력받아 해당 기간동안 공매도 잔고 정보를 DataFrame으로 반환

df = stock.get_shorting_investor_value_by_date("20190401", "20190405", "KOSPI") : 시작일/종료일 두 개의 파라미터를 입력받아 해당 기간동안 코스피 종목의 투자자별 공매도 거래대금을 DataFrame으로 반환
df = stock.get_shorting_investor_value_by_date("20190401", "20190405", "KOSDAQ") : 코스닥의 공매도 거래량 또한 조회


df = df = stock.get_shorting_volume_by_date("20210104", "20210108", "005930") : 입력받은 종목에 대해 주어진 기간 동안의 공매도 거래 정보를 반환

df = stock.get_shorting_volume_by_ticker("20210125") : 입력받은 일자의 공매도 거래량 정보를 반환

df = stock.get_exhaustion_rates_of_foreign_investment("20210108", "20210115", "005930") : 일자별 외국인 보유량 및 외국인 한도소진률

df = stock.get_market_cap("20190101", "20190131", "005930")
시가총액     거래량         거래대금   상장주식수

