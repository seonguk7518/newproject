import MetaTrader5 as mt5
from fastapi import FastAPI
import pandas as pd
import pymysql
import uvicorn
from sqlalchemy import create_engine
from utils.db_connect import MySQLAdapter
from utils.make_error import MakeErrorType


class mt5fun:
    def __init__(self):

        self.return_dict_data=dict(results=[], reCode=1, message='Server Error')
        self.balance=0
        self.status_code=200


    def read_data(self,id:int,pas:str,server:str,money:float):
        account = id  # 실제 계좌 번호로 변경
        password = pas  # 실제 비밀번호로 변경
        server = server  # 브로커 서버 이름으로 변경
        check = MakeErrorType()
        # account = 80286327  # 실제 계좌 번호로 변경
        # password = "Asd237525!"  # 실제 비밀번호로 변경
        # server = "FPMarketsSC-Live"  # 브로커 서버 이름으로 변경

        # account = 80310443  # 실제 계좌 번호로 변경
        # password = "Cmcmc9208!"  # 실제 비밀번호로 변경
        # server = "FPMarketsLLC-Live"  # 브로커 서버 이름으로 변경


    # MT5 초기화
        mysql=MySQLAdapter()
        mt5.initialize(login=account,password=password,server=server,timeout=2000)
        
        if not mt5.initialize(timeout=2000):
            print('터미널로그인실패')
            self.return_dict_data['reCode']=102
            self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])

            return False
        #     print("initialize() failed, error code =", mt5.last_error())
        #     quit()

        # print(mt5.terminal_info())
        if mysql.find_user(id):
            print('체크')
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
                self.balance=account_info.balance
                self.return_dict_data['reCode']=0
                self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                
                return True
            
            else:
                print('밸런스 부족')
                self.return_dict_data['reCode']=104
                self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])

                return False
        else:
            print('안될체크')
            self.return_dict_data['reCode']=103
            self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])

            return False
    
    
     
            