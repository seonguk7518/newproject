import pandas as pd
import numpy as np
import ccxt
import datetime
import time
import requests
from pytz import timezone

import pymysql

binance = ccxt.binanceusdm()
coins_trd = binance.fetch_tickers()
host = "database-1-instance-1.ccjdvtwjk9lh.ap-northeast-2.rds.amazonaws.com"
port=3306
name="staff_new"
password="onebot9608!!"
db_name="data"

def filter_usdt_key(coin_dict) :
    usdt_key_list = []
    for key in coin_dict.keys() :
        try:  
            currency = key.split(":")[1]
            if currency == 'USDT' :
                usdt_key_list.append(key)
        except Exception as e:
            print(e)
    return usdt_key_list

def load_coin_data(symbol, time_m) :   #타입 변환
    dt_utc = datetime.datetime.now() + datetime.timedelta(hours=-24)  #KST -> UTC

    dt_utc = dt_utc.strftime('%Y-%m-%d %H:%M:%S')  #datetime -> str
    timestamp = time.mktime(datetime.datetime.strptime(dt_utc, '%Y-%m-%d %H:%M:%S').timetuple())*1000  #stamp형태로 변환

    BTC_data = binance.fetch_ohlcv(symbol, time_m, since = round(timestamp))  #코인명, 코인시세 간격 입력
    df_BTC = pd.DataFrame(BTC_data, columns = ['datetime','Open','High','Low','Close','Volume'])
    
    df_BTC['datetime'] = pd.to_datetime(df_BTC['datetime'], unit='ms')
    
    for i in range(len(df_BTC)) :
        df_BTC.loc[i,'datetime'] = df_BTC.loc[i,'datetime'] + datetime.timedelta(hours=9)  #UTC -> KST 
           
    df_BTC['datetime']=df_BTC['datetime'].apply(lambda x:x.strftime('%Y%m%d%H%M'))
    # df_BTC.set_index('datetime', inplace=True)

    return df_BTC

def generate_price_dict(usdt_keys) : 
    data_list = []
    for usdt_key in usdt_keys :
        try : 
            coin_data = load_coin_data(usdt_key,'5m').iloc[-2]
            ticker =  usdt_key.split(":")[0].split("/")[0]
            data_dict = {}
            data_dict['ticker'] = ticker
            data_dict['datetime'] = coin_data['datetime']
            data_dict['open'] = float(coin_data['Open'])
            data_dict['high'] = float(coin_data['High'])
            data_dict['low'] = float(coin_data['Low'])
            data_dict['close'] = float(coin_data['Close'])
            data_dict['volume'] = float(coin_data['Volume'])
            data_list.append(data_dict)
        except Exception as e:
            print(e)
    return data_list


def insert_data_to_db(price_data_list, conn) : 
    with conn.cursor() as cur : 
        for price_data in price_data_list : 
            ticker= price_data['ticker']
            datetime_str = price_data['datetime']
            open = price_data['open']
            high = price_data['high']
            low = price_data['low']
            volume = price_data['volume']
            close = price_data['close']
            values_String = f"'{datetime_str}','{ticker}',{open},{high},{low},{close},{volume}"
            sql = f"Insert into ticker_price_score_5m (datetime,ticker,open,high,low,close,volume) values ({values_String})"
            cur.execute(sql)
        conn.commit()



usdt_key_list = filter_usdt_key(coins_trd)
price_data_dict = generate_price_dict(usdt_key_list)
print(price_data_dict)

try :
    conn = pymysql.connect(host=host,port=port, user=name, passwd=password, db=db_name, connect_timeout=5)
except pymysql.MySQLError as e:
    print(e)
    


insert_data_to_db(price_data_dict,conn)

