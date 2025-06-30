from PyQt5.QAxContainer import QAxWidget
from PyQt5.QtWidgets import QApplication
import psycopg2

class Kiwoom:
    def __init__(self):
        self.app = QApplication([])
        self.ocx = QAxWidget("KHOPENAPI.KHOpenAPICtrl.1")
        self.ocx.OnEventConnect.connect(self.on_event_connect)
        self.ocx.OnReceiveRealData.connect(self.on_receive_real_data)
        self.login()

    def login(self):
        self.ocx.dynamicCall("CommConnect()")
        self.app.exec_()

    def on_event_connect(self, err_code):
        if err_code == 0:
            print("로그인 성공")
            # 종목 등록 예시: 삼성전자 (005930)
            self.ocx.dynamicCall("SetRealReg(QString, QString, QString, QString)", "1000", "005930", "10", "0")

    def on_receive_real_data(self, code, real_type, data):
        if real_type == "주식체결":
            price = self.ocx.dynamicCall("GetCommRealData(QString, int)", code, 10)  # 현재가
            volume = self.ocx.dynamicCall("GetCommRealData(QString, int)", code, 15)  # 거래량
            print(f"실시간: {code}, 현재가: {price}, 거래량: {volume}")
            self.save_to_postgresql(code, price, volume)

    def save_to_postgresql(self, code, price, volume):
        conn = psycopg2.connect(
            dbname='your_db', user='your_user', password='your_pw', host='localhost', port='5432'
        )
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO realtime_prices (ticker, price, volume, time) VALUES (%s, %s, %s, NOW())",
            (code, price.strip(), volume.strip())
        )
        conn.commit()
        cur.close()
        conn.close()

if __name__ == "__main__":
    Kiwoom()
