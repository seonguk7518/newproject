import pandas as pd
import pandas_ta as ta
from aiohttp import ClientSession
from datetime import timedelta
from requests import get
import datetime    
import pandas as pd
import numpy as np
import ccxt
import datetime
import time
import requests
from pytz import timezone
import asyncio
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

ticker_list=filter_usdt_key(coins_trd)


dt_utc = datetime.datetime.now() + datetime.timedelta(weeks=-2) 
# dt_utc =dt_utc  + datetime.timedelta(days=-2)
dt_utc  =  dt_utc = dt_utc.strftime('%Y-%m-%d %H:%M:%S') 
timestamp = time.mktime(datetime.datetime.strptime(dt_utc, '%Y-%m-%d %H:%M:%S').timetuple())*1000 


async def get_aiohttp( symbol,date,loop):
    async with ClientSession(loop=loop) as session:
        async with session.get(f'https://fapi.binance.com/fapi/v1/klines?' + \
                f'symbol={symbol}USDT&interval=1w&startTime={round(date)}') as response:
            
            resp = await response.json()

        return resp
loop = asyncio.get_event_loop()

price_list=[]

for ticker in ticker_list:   
    new_dict={} 
    ticker=ticker.split("/")[0]
    try:
            
            resp=loop.run_until_complete(get_aiohttp(ticker,timestamp,loop))
            columns = [
                'datetime', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close time', 'Quote asset volume', 
                'Number of trades', 'Taker buy base asset volume', 'Taker buy quote asset volume', 'Ignore'
            ]

            # DataFrame 생성
            new_df = pd.DataFrame(data=resp, columns=columns)
            new_df = new_df.astype({'Open' : float, 'High' : float, 'Low' : float, 'Close' : float})
            new_df['datetime'] = pd.to_datetime(new_df['datetime'], unit='ms') + timedelta(hours=9)
            new_df['ticker']=ticker
            new_df['datetime']=new_df['datetime'].apply(lambda x:x.strftime('%Y%m%d'))
            # new_df.set_index(keys=['datetime'], inplace=True)
            
            if len(new_df)>=2:
                new_dict['datetime']=new_df['datetime'].iloc[0]
                new_dict['ticker']=new_df['ticker'].iloc[0]
                new_dict['open']=new_df['Open'].iloc[0]
                new_dict['high']=new_df['High'].iloc[0]
                new_dict['low']=new_df['Low'].iloc[0]
                new_dict['close']=new_df['Close'].iloc[0]
                new_dict['volume']=new_df['Volume'].iloc[0]
                price_list.append(new_dict)
    except Exception as e :
        print(e) 
        pass


def insert_data_to_db(price_data_list, conn) : 
    with conn.cursor() as cur : 
        for price_data in price_data_list : 
            ticker= price_data['ticker']
            datetime_str = price_data['datetime']
            open = price_data['open']
            high = price_data['high']
            low = price_data['low']
            close = price_data['close']
            volume = price_data['volume']
         
            
            values_String = f"'{datetime_str}','{ticker}',{open},{high},{low},{close},{volume}"
            sql = f"Insert into b_ticker_price_1w (datetime,ticker,open,high,low,close,volume) values ({values_String})"
            cur.execute(sql)
        conn.commit()

try:
    conn = pymysql.connect(host=host,port=port, user=name, passwd=password, db=db_name, connect_timeout=5)
except pymysql.MySQLError as e:
    print(e)
print(price_list)
print(len(price_list))
insert_data_to_db(price_list,conn)

