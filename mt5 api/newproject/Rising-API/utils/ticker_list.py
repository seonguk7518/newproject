import logging
from pathlib import Path
from os import path

from pandas import DataFrame
from sqlalchemy.orm import Session
from sqlalchemy import select, desc
from config import ticker, engine




class tickers:

    def __init__(self) -> None:
        self.status_code = 200
        self.return_dict_data = dict(result='Good')

    
    def exchange(self, selet: int)  :
        
        
        
        # Select SQL
        stsql = select(ticker).where(ticker.c.id == selet)
        
        # DB Session > DF 생성
        with Session(engine) as session:
            resp_session = session.execute(stsql)

        ticker_list = DataFrame(resp_session)
        
        try:
            
            new_ticker_list=ticker_list['lists'][0].split(",")
            

            
            self.return_dict_data['result']=new_ticker_list
            

        except Exception as e:
            self.status_code = 500
            self.return_dict_data = dict(error_code=self.status_code, error_message="Unknown Error")

            

            return False

        return True