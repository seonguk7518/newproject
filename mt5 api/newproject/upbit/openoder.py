import logging
import sqlite3
import time
from dataclasses import dataclass
import logging
import logging.handlers
from pyupbit  import Upbit
import pyupbit
import pandas as pd
from enum import Enum, auto
from decimal import Decimal
from sqlalchemy import create_engine
from datetime import datetime
import os
import pymysql.cursors
from pymysql.connections import Connection
import pymysql
from pytz import timezone
import time
from datetime import timedelta
from sqlalchemy.engine.url import URL










class Signal(Enum):
    LONG = auto()
    SHORT = auto()
    NO_SIGNAL = auto()


class Client:

    def __init__(self,access_key,secret_key):

        self.access_key=access_key
        self.secret_key=secret_key
        self.upbit = Upbit(self.access_key, self.secret_key)

    def buy_limit_order(self,symbol, price, quantity):

        self.symbol=symbol
        self.price=price
        self.quantity=quantity

        try:
            
            print(self.upbit.buy_limit_order(self.symbol, self.price, self.quantity))
        except Exception as e:
            print(e,"주문실패")
        

    def  cancel_all_open_orders(self,symbol):

        self.symbol=symbol
        self.uuid=self.upbit.get_order(self.symbol)

        
        for i in self.uuid:
            uid=i['uuid']

            self.upbit.cancel_order(uid)


    def  sell_market_order (self,symbol:str, quantity):

        self.symbol=symbol
        self.quantity=quantity

        self.upbit.sell_market_order(self.symbol, self.quantity)


    def get_klines(self,symbol:str, interval:str, limit:int):
        self.symbol=symbol
        self.interval=interval
        self.limit=limit

        df = pyupbit.get_ohlcv(self.symbol, f"minute{self.interval}",self.limit)


        return df

    def get_existence_ticker(self,symbol):

        self.symbol=symbol
        ticker=pyupbit.get_tickers(f"{self.symbol}")

        if len(ticker) >0:

            return True
        else:
            return False

    def get_open_position_list(self):


        ticker_list=[]
        self.ticker=self.upbit.get_balances()

        for i in self.ticker:
            current_p=float(i['avg_buy_price'])*float(i['balance'])

            if i['currency']!='KRW' and current_p>5000:

                ticker_list.append({"symbol":i['currency'],"positionAmt":i['balance']})

        return ticker_list

    def change_usdt_to_lot_size(self,symbol,money):

        self.symbol=symbol
        self.money=money
        self.current_price=pyupbit.get_current_price(self.symbol)

        

        qut=round((self.money/self.current_price),3)
        

        
        return qut

    def change_price_to_exact_price(self,price):
        
        self.price=price
        price1=pyupbit.get_tick_size(self.price)
        return  price1


    def cancel_order_open_order(self):

        ticker=pyupbit.get_tickers()
        
        for i in ticker:
            
            open_or=self.upbit.get_order(i)

            if len(open_or)>0:
                print(open_or)
            try:
                for open in open_or:

                    aa=(open['created_at'].split(":")[0])
                    aa1=aa.split("T")[1]
                    aa2=aa.split("T")[0]
                    aa3= aa2+' '+aa1

                    past = datetime.strptime(aa3,"%Y-%m-%d %H")
                    past1=(past+timedelta(hours=4)).strftime('%Y-%m-%d %H:00:00')

                    
                    now = (datetime.now(timezone('Asia/Seoul'))).strftime('%Y-%m-%d %H:00:00')

                    if now==past1 and open['state']=='wait':
                        print("4시간후 오픈오던 종료")
                        print(open['market'])
                        uuid = open['uuid']

                        self.upbit.cancel_order(uuid)
            except Exception as e:
                pass
                









@dataclass
class User:
    id: int
    name: str
    access_key: str
    secret_key: str
    money: int
    client: Client = None
    long_coin_num: int = 0

    # self.access_key="eUVDflvMzDePFlelEvBgJHQVGMedb8xERTVEx7TM"
    # self.secret_key="6eXoEGqgw9rXoUgxvQJu610IznfkkxZ92FQveiYZ"

    def __post_init__(self):
        self.client = Client(self.access_key, self.secret_key)

 

    def close_open_or(self):
        try:
            self.client.cancel_order_open_order()
        except Exception as e:
            raise e



@dataclass
class Coin:
    symbol: str


class System:
    def __init__(self):
        self.interval = '60'

        self.max_long_coin_num: int = 5


        self.first_entry_ratios = 1
        self.second_entry_ratios = 0.2
        self.third_entry_ratios = 0.2

        # self.first_split_price_ratios =
        # self.second_split_price_ratios =
        # self.third_split_price_ratios =

        # self.profit_percent = 0.01  # not used
        self.loss_percent = 0.05

        self.admin_client = Client("eUVDflvMzDePFlelEvBgJHQVGMedb8xERTVEx7TM",
                                   "6eXoEGqgw9rXoUgxvQJu610IznfkkxZ92FQveiYZ")

        self.user_list: list = self._get_users()
        
    def _get_users(self) -> list:
        try:

            # DB = {
            #     'drivername': 'mysql',
            #     'host': 'database-1-instance-1.ccjdvtwjk9lh.ap-northeast-2.rds.amazonaws.com',
            #     'port': '3306',
            #     'username': 'staff_new',
            #     'password': 'onebot9608!!',
            #     'database': 'data',
            #     'query': {'charset':'utf8'}
            # }

            # engine = create_engine(URL(**DB))
            engine = create_engine(
            "mysql+pymysql://staff_new:onebot9608!!@database-1-instance-1.ccjdvtwjk9lh.ap-northeast-2.rds.amazonaws.com/data?charset=utf8")
        except pymysql.MySQLError as e:
            print(e)



        conn = engine.connect()
        sql = f"""
        SELECT * FROM user;
        """
        with conn as con:
            result=pd.read_sql(sql,con )



        user_list= [User(id=user[1]['id'],name=user[1]['name'],access_key=user[1]['access_key'],secret_key=user[1]['secret_key'],money=user[1]['money'] )for user in result.iterrows()]

        return user_list




    def detect(self):
        
        print((datetime.now(timezone('Asia/Seoul'))).strftime('%Y-%m-%d %H:00:00'))
        for user in self.user_list:
            print(user.name)
            user.close_open_or()
#        

  
    
        
           
                
def main():
    system = System()
    system.detect()
    
    
if __name__ == "__main__":
    main()



