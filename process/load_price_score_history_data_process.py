import pandas as pd
import numpy as np
import ccxt
import datetime as dt
import time
import requests
from pytz import timezone

import pymysql

from sqlalchemy import create_engine, Table, Column, Integer, String, Float, BigInteger
from sqlalchemy.ext.declarative import declarative_base

binance = ccxt.binanceusdm()
coins_trd = binance.fetch_tickers()

def filter_usdt_key(coin_dict) :
    usdt_key_list = []
    for key in coin_dict.keys() : 
        currency = key.split(":")[1]
        if currency == 'USDT' :
            usdt_key_list.append(key)
    return usdt_key_list


def load_coin_data(symbol, time_m, date) : 
    dateSet = []
    dateSet.append(date)
    date = dt.datetime.strptime(date, '%Y-%m-%d %H:%M:%S')  #타입 변환
    dt_utc = date + dt.timedelta(hours=-9)  #KST -> UTC

    dt_utc = dt_utc.strftime('%Y-%m-%d %H:%M:%S')  #datetime -> str
    timestamp = time.mktime(dt.datetime.strptime(dt_utc, '%Y-%m-%d %H:%M:%S').timetuple())*1000  #stamp형태로 변환

    BTC_data = binance.fetch_ohlcv(symbol, time_m, since = round(timestamp))  #코인명, 코인시세 간격 입력
    df_BTC = pd.DataFrame(BTC_data, columns = ['datetime','Open','High','Low','Close','Volume'])
    
    df_BTC['datetime'] = pd.to_datetime(df_BTC['datetime'], unit='ms')
    
    for i in range(len(df_BTC)) :
        df_BTC.loc[i,'datetime'] = df_BTC.loc[i,'datetime'] + dt.timedelta(hours=9)  #UTC -> KST    

    # df_BTC.set_index('datetime', inplace=True)

    return df_BTC

usdt_key_list = filter_usdt_key(coins_trd)

date_range = pd.date_range(start="20211231",end=dt.datetime.now().strftime("%Y%m%d"),freq='15d')
date_range = [date.strftime("%Y-%m-%d %H:%M:%S") for date in date_range]

host = "database-1-instance-1.ccjdvtwjk9lh.ap-northeast-2.rds.amazonaws.com"
port=3306
engine = create_engine(f"mysql+mysqldb://admin:onebot7085!!@{host}:{port}/data",echo=True)


for usdt_key in usdt_key_list :
    ticker = usdt_key.split("/")[0]
    print(ticker)
    df_list = []
    for date in date_range : 
        df= load_coin_data(usdt_key,'1h',date)
        df_list.append(df)
    ticker_price_data = pd.concat(df_list)
    ticker_price_data['datetime'] = ticker_price_data['datetime'].apply(lambda x:x.strftime("%Y%m%d%H"))
    ticker_price_data['ticker'] = ticker
    ticker_price_data.rename(columns={'datetime':"datetime","ticker":"ticker",
                                      "High":"high","Open":"open",
                                      "Low":"low",
                                      "Close":"close",
                                      "Volume":"volume"
                                      },inplace=True)
    ticker_price_data.drop_duplicates(subset="datetime")
    # ticker_price_data = ticker_price_data['']
    ticker_price_data.to_sql("ticker_price", con=engine,if_exists='append',index=False)
# print(df)