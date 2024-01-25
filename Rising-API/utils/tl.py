from pathlib import Path
from os import path
import sys, os
from pandas import DataFrame
from sqlalchemy.orm import Session
from sqlalchemy import select, asc
from config import signal_data, engine
from tickers import ticker_lists



import logging
from pathlib import Path
from os import path

from pandas import DataFrame
from sqlalchemy.orm import Session
from sqlalchemy import select, desc
from config import signal_data, engine
from tickers import ticker_lists




class RSIQueFunc:
    def __init__(self) -> None:
        self.status_code = 200
        self.return_dict_data = dict(result='Good')

    def check_ticker(self, ticker: str) -> bool:
        
        return_type = True if ticker in ticker_lists else False

        return return_type
    
    def signal(self,df):

        def sample(sample_df):
            if  sample_df['basesignal'][-4:-3]=='S':

                return 'S'
            elif sample_df['basesignal'][-4:-3]=='L':

                return 'L'
            else:
                return ''
        
        df['new_signal'] = df[['basesignal']].apply(lambda x : sample(x), axis=1)
        
        return df

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
        stsql = select(signal_data).where(signal_data.c.ticker == ticker
            ).order_by(desc(signal_data.c.datetime)).limit(limit)
        
        # DB Session > DF 생성
        with Session(engine) as session:
            resp_session = session.execute(stsql)

        new_signal_data = DataFrame(resp_session)
        new_signal_data=self.signal(new_signal_data)
        try:
            if len(new_signal_data) > 0: 
                new_signal_data.sort_values('datetime',inplace=True)
                new_signal_data = new_signal_data[(new_signal_data['new_signal']=='L')|(new_signal_data['new_signal']=='S')]
                
            self.return_dict_data['result'] = []

            # Return Data 생성
            for i in new_signal_data.iterrows():
                data = i[1]
                new_dict = {}
                new_dict['datetime'] = str(data['datetime'])
                new_dict['ticker'] = data['ticker']
                new_dict['signal'] = data['new_signal']

                self.return_dict_data['result'].append(new_dict)

        except Exception as e:
            self.status_code = 500
            self.return_dict_data = dict(error_code=self.status_code, error_message="Unknown Error")

            return False

        return True
    
    

