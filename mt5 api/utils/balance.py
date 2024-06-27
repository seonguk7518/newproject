import MetaTrader5 as mt5
from fastapi import FastAPI
import pandas as pd
import pymysql
import uvicorn
from sqlalchemy import create_engine


class mt5fun:
    def __init__(self):

        self.return_dict_data=dict(results=[])
        self.status_code=200


    def read_data(self,id:int,pas:str,server:str,money:float):
        account = id  # 실제 계좌 번호로 변경
        password = pas  # 실제 비밀번호로 변경
        server = server  # 브로커 서버 이름으로 변경

        # account = 80286327  # 실제 계좌 번호로 변경
        # password = "Asd237525!"  # 실제 비밀번호로 변경
        # server = "FPMarketsSC-Live"  # 브로커 서버 이름으로 변경

        # account = 80310443  # 실제 계좌 번호로 변경
        # password = "Cmcmc9208!"  # 실제 비밀번호로 변경
        # server = "FPMarketsLLC-Live"  # 브로커 서버 이름으로 변경


    # MT5 초기화

        mt5.initialize(login=account,password=password,server=server,timeout=1000)
        
        if not mt5.initialize(timeout=1000):
            print('로그인실패')
            self.return_dict_data['reCode']=1
            

            return False
        #     print("initialize() failed, error code =", mt5.last_error())
        #     quit()

        # print(mt5.terminal_info())


        # 로그인 시도
        authorized = mt5.login(account, password,server)
       
        if not authorized:
            print('b')
            mt5.shutdown()
            text='err'
            
            
            return False
      

        # 계좌 정보 가져오기
        account_info = mt5.account_info()

        # if account_info is None:
        #     print("Failed to get account info, error code =", mt5.last_error())
        #     mt5.shutdown()
        #     quit()
       
       
        print(account_info.balance)

        if int(account_info.balance)>=money:
        
            self.return_dict_data['reCode']=0
            return True
        
        else:
            self.return_dict_data['reCode']=1

            return False