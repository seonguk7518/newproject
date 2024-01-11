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

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
# ENV LITERAL
LOGFILE_PATH = os.path.join(PROJECT_DIR, "logfile.log")

logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter(u'[%(asctime)s] %(message)s', "%Y-%m-%d %H:%M:%S")

file_handler = logging.handlers.TimedRotatingFileHandler(LOGFILE_PATH,
                                                         when='midnight',
                                                         interval=1,
                                                         encoding='utf-8')
file_handler.suffix = 'log-%Y%m%d'
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)





class DBAdapter:
    '''
    2023/12/15 - pysonic
    DB(mysql) INFO
    - DB name : chart_data
    - table name : signal_data
    db adapter extract LONG/SHORT signal coin list (received datetime(str) argument)
    ex) db_adapter.extract_S_signal_coin_list('2023-12-15 14:00:00')
    '''
    def __init__(self):
        self._get_db_connection_info()

    def _get_connection(self):
        try:
            connection = Connection(host=self.HOST,
                                    user=self.USER,
                                    password=self.PASSWORD,
                                    database=self.DATABASE,
                                    cursorclass=pymysql.cursors.DictCursor)
            connection.ping(False)
        except Exception as e:
            print("connection error", e)
        else:
            return connection

    def _get_db_connection_info(self) -> None:
        self.HOST: str = "52.79.226.104"
        self.USER: str = "datateam"
        self.PASSWORD: str = "datateam1234$$"
        self.DATABASE: str = "chart_data"

    def select_signal(self, now_df_time: str, pre_df_time: str) -> list:
        conn = self._get_connection()

        if conn:
            try:
                with conn.cursor() as cursor:
                    
                    sql = "SELECT * FROM chart_data.signal_data where datetime = %s or datetime = %s order by ticker, datetime desc;"
                    cursor.execute(sql, (now_df_time, pre_df_time))
                    result = cursor.fetchall()
                
                return result
            except Exception as e:  # TODO: Custom Exception
                print("select signal error", e)
                
        return []
    
    def goya_check(self, now_df_time: str, ticker: str) -> bool:
        conn = self._get_connection()

        now_time = (datetime.strptime(now_df_time, '%Y-%m-%d %H:00:00')).strftime('%Y%m%d%H00')

        if conn:
            try:
                with conn.cursor() as cursor:
                    
                    sql = "SELECT * FROM chart_data.b2_ticker_price_1h where datetime = %s and ticker = %s;"
                    cursor.execute(sql, (now_time, ticker))
                    df_result = cursor.fetchall()

                    result = (lambda c, p: 'L' if c > p else 'S')(df_result[0]['close'], df_result[0]['pink'])
                
                return result
            except Exception as e:  # TODO: Custom Exception
                print("goya_check error", e)
                
        return False
            
    def extract_S_signal_coin_list(self, now_df_time: str) -> list:
        
        short_coin_list: list = []

        pre_df_time = (datetime.strptime(now_df_time, '%Y-%m-%d %H:00:00') - timedelta(hours=1)).strftime('%Y-%m-%d %H:00:00')
        
        signal_result = self.select_signal(now_df_time, pre_df_time)
        
        for num, signal in enumerate(signal_result):
            if num % 2 == 0:
                save_signal = signal['basestate'][-8:-7]
                save_sar = signal['sar']
                save_ticker = signal['ticker']
            else:
                if ( save_ticker == signal['ticker'] and (save_signal != signal['basestate'][-8:-7] 
                     or save_sar != signal['sar'] )):
                    if save_signal == save_sar == 'S':
                        check = self.goya_check(now_df_time, save_ticker)
                        if check == 'S':
                            short_coin_list.append(save_ticker)
                    
        return short_coin_list
        
    def extract_L_signal_coin_list(self, now_df_time: str) -> list:
        
        long_coin_list: list = []
        
        pre_df_time = (datetime.strptime(now_df_time, '%Y-%m-%d %H:00:00') - timedelta(hours=1)).strftime('%Y-%m-%d %H:00:00')
        
        signal_result = self.select_signal(now_df_time, pre_df_time)
        
        for num, signal in enumerate(signal_result):
            if num % 2 == 0:
                save_signal = signal['basestate'][-8:-7]
                save_sar = signal['sar']
                save_ticker = signal['ticker']
            else:
                if ( save_ticker == signal['ticker'] and (save_signal != signal['basestate'][-8:-7] 
                     or save_sar != signal['sar'] )):
                    if save_signal == save_sar == 'L':
                        check = self.goya_check(now_df_time, save_ticker)
                        if check == 'L':
                            long_coin_list.append(save_ticker)
                    
        return long_coin_list






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

            print("오픈오더정리",self.upbit.cancel_order(uid))


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

    def set_user_coin_num(self, open_postion_list: list):

        self.long_coin_num = 0


        for position in open_postion_list:
            if float(position['positionAmt']) > 0:
                self.long_coin_num += 1



    def check_is_position_open(self, open_position_list: list, symbol: str) -> str:
        for position in open_position_list:
            if position['symbol'] == symbol.split("-")[1]:
                if float(position['positionAmt']) > 0:
                    return "LONG"

        return "NO_POSITION"


    def get_open_one_position(self, open_position_list: list, symbol: str) -> dict:
        result = []
        for position in open_position_list:
            if position['symbol'] == symbol.split("-")[1]:
                result.append(position)
                break

        return result[0]


    def get_open_position(self) -> list:
        try:
            result = self.client.get_open_position_list()
            return result
        except Exception as e:
            logger.error(f"[{self.id}]{self.name} get open position error {e}")
            raise e



    def open_long_limit_order(self, symbol: str, price: float, quantity: float):
        try:
            self.client.buy_limit_order(symbol, price, quantity)
        except Exception as e:
            logger.error(f"[{self.id}]{self.name} open long error {e}")
            raise e



    # def open_long_tp_sl_order(self, symbol: str, tp_price: float, sl_price: float):
    #     try:
    #         self.client.open_sl_order(symbol, sl_price)
    #         # self.client.open_tp_order(symbol, tp_price)  # only SL
    #     except Exception as e:
    #         logger.error(f"[{self.id}]{self.name} OPEN LONG TP/SL ERROR {e}")
    #         raise e



    def cancel_all_open_order(self, symbol: str):
        try:
            self.client.cancel_all_open_orders(symbol)
        except Exception as e:
            logger.error(f"[{self.id}]{self.name} cancel all open order error {e}")
            raise e


    def close_long_market_order(self, symbol: str, quantity: float):
        try:
            self.client.sell_market_order(symbol, quantity)
        except Exception as e:
            logger.error(f"[{self.id}]{self.name} close long error {e}")
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

        return [user_list[-1]]



    # utils TODO: divide other moudle later
    def _check_candle_up_down(self, candle: pd.Series) -> str:
        '''
        ENUM : UP / DOWN
        '''
        if candle['close'] > candle['open']:
            return "UP"
        elif candle['close'] < candle['open']:
            return "DOWN"

    def get_UP_trend_candle(self, df: pd.DataFrame, index: int=-2):

        first_trend_candle = df.iloc[index]

        for i in range(3):
            if self._check_candle_up_down(df.iloc[index-i]) == 'UP':
                first_trend_candle = df.iloc[index-i]
            else:
                break

        first_candle = first_trend_candle
        last_candle = df.iloc[index]

        return first_candle, last_candle


    # def get_DOWN_trend_candle(self, df: pd.DataFrame, index: int=-2):

    #     first_trend_candle = df.iloc[index]

    #     for i in range(3):
    #         if self._check_candle_up_down(df.iloc[index-i]) == 'DOWN':
    #             first_trend_candle = df.iloc[index-i]
    #         else:
    #             break

    #     first_candle = first_trend_candle
    #     last_candle = df.iloc[index]

    #     return first_candle, last_candle

    def get_split_price_from_up_candle(self, first_candle: pd.Series, last_candle: pd.Series) -> tuple[float, float, float]:
        mid = first_candle['open'] + ((last_candle['close'] - first_candle['open']) * 0.5)
        low =  first_candle['open'] + ((last_candle['close'] - first_candle['open']) * 0.2)


        return low, mid, last_candle['close']


    # def get_split_price_from_down_candle(self, first_candle: pd.Series, last_candle: pd.Series) -> tuple[float, float, float]:
    #     mid = first_candle['open'] - ((first_candle['open'] - last_candle['close']) * 0.5)
    #     low =  first_candle['open'] - ((first_candle['open'] - last_candle['close']) * 0.7)

    #     # return first_candle['open'], low, mid
    #     return low, mid, last_candle['close']



    def get_long_sl_price(self, stand_price: float) -> float:
        return float(Decimal(f'{stand_price}') * (Decimal(f'{1}') - Decimal(f'{self.loss_percent}')))





    # logic
    def load_df(self, coin: Coin) -> pd.DataFrame:
        try:
            df = self.admin_client.get_klines(coin.symbol, self.interval, limit=10)
            return df
        except Exception as e:
            logger.error(f"get df error {e}")
            raise e

    def detect(self):
        now = (datetime.now(timezone('Asia/Seoul'))-timedelta(hours=1)).strftime('%Y-%m-%d %H:00:00')
        logger.info(now)
        db = DBAdapter()
        long_coin_list = [Coin("KRW-"+symbol) for symbol in db.extract_L_signal_coin_list(now)]
        # short_coin_list = [Coin("KRW-"+symbol) for symbol in db.extract_S_signal_coin_list(now)]

        short_coin_list=[Coin("KRW-ORBS")]
        # long_coin_list=[Coin("KRW-XRP")]

        logger.info(f"{long_coin_list} LONG  시그널")
        logger.info(f"{short_coin_list} SHORT 시그널")
        # short_coin_list=[Coin("KRW-ORBS")]
        print("숏시그널",short_coin_list)
        print("롱시그널",long_coin_list)

        for coin in long_coin_list:
            
            
            try:
                bul=self.admin_client.get_existence_ticker(coin.symbol)

                if bul ==True:
                    df = self.load_df(coin)
                    print(df)

                    self.long_signal(coin, df)
                    time.sleep(0.3)
            except Exception as e:
                logger.error(e)

                continue

        for coin in short_coin_list:
            try:
            
                bul=self.admin_client.get_existence_ticker(coin.symbol)

                if bul ==True:
                    self.short_signal(coin)
                    time.sleep(0.3)

            except Exception as e:
                print(e)

#        for coin in short_coin_list:
#            logger.info(f"SHORT SIGNAL - {coin.symbol}")

#            try:
#                bul=self.admin_client.get_existence_ticker(coin.symbol)
#                if bul ==True:
#                    df = self.load_df(coin)
#            except Exception as e:
#                logger.error(e)
#                continue


            # self.short_signal(coin, df)
            # time.sleep(0.3)

    def long_signal(self, coin: Coin, df: pd.DataFrame):

        
        print("롱",coin.symbol)
        first_candle, last_candle = self.get_UP_trend_candle(df)
        open, low, mid = self.get_split_price_from_up_candle(first_candle, last_candle)

        print("첫번째캔들",first_candle,"마지막캔들", last_candle)
        print("open",open,"low", low,"mid", mid)
        print("유저리스트",self.user_list)
        for user in self.user_list:
            try:
                print(user.name)

                logger.info(f"{user.id}{user.name} {coin.symbol} LONG 오픈 시작")
                position_list = user.get_open_position()
                position_result = user.check_is_position_open(position_list, coin.symbol)

                print("포지션리스트",position_list,"포지션",position_result)
                logger.info(f"포지션리스트: {position_list} 포지션: {position_result} ")
                
                if position_result == "LONG":
                    logger.info(f"[{user.id}]{user.name} LONG 포지션 이미 존재하므로 진입X")
                    continue

                user.set_user_coin_num(position_list)

                

                print("포지션갯수",user.long_coin_num)
                if user.long_coin_num < self.max_long_coin_num:
                    print("진입전 오픈오더 정리",coin.symbol)
                    user.cancel_all_open_order(coin.symbol)
                    quantity1 = self.admin_client.change_usdt_to_lot_size(coin.symbol, int(user.money)*self.first_entry_ratios)
                    quantity2 = self.admin_client.change_usdt_to_lot_size(coin.symbol, int(user.money)*self.second_entry_ratios)
                    quantity3 = self.admin_client.change_usdt_to_lot_size(coin.symbol, int(user.money)*self.third_entry_ratios)
                    print("수량1",quantity1,"수량2",quantity2,"수량3",quantity3)

                    logger.info(f"수량1: {quantity1} 수량2: {quantity2} 수량3: {quantity3} ")

                    price1 =  self.admin_client.change_price_to_exact_price( mid)
                    price2 =  self.admin_client.change_price_to_exact_price( low)
                    price3 =  self.admin_client.change_price_to_exact_price( open)
                    
                    print("mid가격",price1 ,"low가격",price2 ,"open가격",price3 )

                    logger.info(f"mid가격: {price1} low가격: {price2} open가격: {price3} ")
                    user.open_long_limit_order(coin.symbol, price1, quantity1)
                    # user.open_long_limit_order(coin.symbol, price2, quantity2)
                    # user.open_long_limit_order(coin.symbol, price3, quantity3)
            except Exception as e:
                print(e)   

    def short_signal(self, coin: Coin):
        

        for user in self.user_list:
            try:
                position_list = user.get_open_position()
                position_result = user.check_is_position_open(position_list, coin.symbol)
                logger.info(f"open오더정리 {user.id} {user.name} {coin.symbol} ")
                user.cancel_all_open_order(coin.symbol)
                if position_result == "LONG":

                    logger.info(f"LONG 정리 {user.id} {user.name} {coin.symbol} ")
                    position = user.get_open_one_position(position_list, coin.symbol)
                    user.close_long_market_order(coin.symbol, abs(float(position['positionAmt'])))  
            except Exception as e:
                print(e)

    def start(self):
        logger.info("************************************************")
        logger.info("탐지 시작")
        logger.info(f"max long coin num : {self.max_long_coin_num}")
        
        # logger.info(f"profit percent : {self.profit_percent*100}%")
        logger.info(f"유저 목록")
        for user in self.user_list:
            logger.info(f"{[user.id]} {user.name}")
        logger.info("************************************************")
        self.detect()    
                
def main():
    system = System()
    system.start()
    
    
if __name__ == "__main__":
    main()

