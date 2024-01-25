import logging
from pathlib import Path
from os import path

from pandas import DataFrame
from sqlalchemy.orm import Session
from sqlalchemy import select, asc
from config import retri_table, engine
from tickers import ticker_lists


# Log-File
PROJECT_DIR = Path(__file__).resolve().parent.parent
LOGFILE_PATH = path.join(PROJECT_DIR, "logs") 
LOGFILE = path.join(LOGFILE_PATH, "que_logfile.log")

# logging
logger = logging.getLogger()
logger.setLevel(logging.ERROR)
formatter = logging.Formatter(u"[%(asctime)s] %(message)s", "%Y-%m-%d %H:%M:%S")

file_handler = logging.handlers.TimedRotatingFileHandler(LOGFILE,
                                                         when='midnight',
                                                         interval=1,
                                                         encoding='utf-8')

file_handler.suffix = "log-%Y%m%d"
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)

class QueFunc:
    def __init__(self) -> None:
        self.status_code = 200
        self.return_dict_data = dict(result='Good')

    def check_ticker(self, ticker: str) -> bool:
        
        return_type = True if ticker in ticker_lists else False

        return return_type

    def que_signal(self, ticker: str, limit: str) -> bool:
        
        # Ticker의 USDT 제거
        if ticker.endswith('USDT'):
            ticker = ticker.split('USDT')[0]

        # 존재하는 Ticker 유무 확인
        if not self.check_ticker(ticker):
            self.status_code = 422
            self.return_dict_data = dict(error_code=self.status_code, error_message="Can't find ticker.")

            return False
        
        # Select SQL
        stsql = select(retri_table).where(retri_table.c.ticker == ticker
            ).order_by(asc(retri_table.c.datetime)).limit(limit)
        
        # DB Session > DF 생성
        with Session(engine) as session:
            resp_session = session.execute(stsql)

        new_signal_data = DataFrame(resp_session)
        
        try:
            # 시그널 생성 시 추천 가격 통합
            if len(new_signal_data) > 0:
                new_signal_data['midle'] = new_signal_data['midle'].shift(-1)
                new_signal_data['profit1'] = new_signal_data['profit1'].shift(-1)
                new_signal_data['profit2'] = new_signal_data['profit2'].shift(-1)
                new_signal_data['profit3'] = new_signal_data['profit3'].shift(-1)
                new_signal_data['recom_price'] = new_signal_data['recom_price'].shift(-1)

            # 생성된 시그널 조회
            new_signal_data = new_signal_data[(new_signal_data['new_signal']=='L')|(new_signal_data['new_signal']=='S')]
            new_signal_data = new_signal_data.fillna(0)

            # DF Type 변경
            new_signal_data['midle'] = new_signal_data['midle'].apply(float)
            new_signal_data['profit1'] = new_signal_data['profit1'].apply(float)
            new_signal_data['profit2'] = new_signal_data['profit2'].apply(float)
            new_signal_data['profit3'] = new_signal_data['profit3'].apply(float)

            self.return_dict_data['result'] = []

            # Return Data 생성
            for i in new_signal_data.iterrows():
                data = i[1]
                new_dict = {}
                new_dict['datetime'] = data['datetime']
                new_dict['ticker'] = data['ticker']
                new_dict['entry_price'] = round(data['midle'], 6)
                new_dict['profit_one_percent'] = round(data['profit1'], 6)
                new_dict['profit_two_percent'] = round(data['profit2'], 6)
                new_dict['profit_three_percent'] = round(data['profit3'], 6)
                new_dict['recommend_price'] = list(map(float, data['recom_price'].split('~')))
                new_dict['signal'] = data['new_signal']

                self.return_dict_data['result'].append(new_dict)

        except Exception as e:
            self.status_code = 500
            self.return_dict_data = dict(error_code=self.status_code, error_message="Unknown Error")

            logger.error(f"알 수 없는 오류 정보 수집 : {e}")

            return False

        return True
    
    