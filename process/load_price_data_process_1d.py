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
name="admin"
password="onebot7085!!"
db_name="data"

try:
    conn = pymysql.connect(host=host,port=port, user=name, passwd=password, db=db_name, connect_timeout=5)
except pymysql.MySQLError as e:
    print(e)

def get_10_days_ago_date(days=6) :
    base_line_date = (datetime.datetime.now() - datetime.timedelta(days=days)).strftime("%Y%m%d%H")
    return base_line_date

def get_previus_data(base_line_date, con) :
    sql = f"""
    select * from data.ticker_price_score where datetime >={base_line_date}
    """
    previus_data = pd.read_sql(sql,con=con)
    return previus_data

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
            sql = f"Insert into ticker_price_score_1d (datetime,ticker,open,high,low,close,volume) values ({values_String})"
            cur.execute(sql)
        conn.commit()

base_line_date = get_10_days_ago_date(days=2)
previus_data = get_previus_data(base_line_date,conn)

date= (datetime.datetime.now(timezone('Asia/Seoul'))-datetime.timedelta(days=1)).strftime("%Y%m%d")

previus_data['date'] = previus_data['datetime'].apply(lambda x:x[:8])
previus_data = previus_data[previus_data['date']==date]
data_list = [
]
for group in previus_data.groupby(["date",'ticker']) : 
    try :
        high_price = group[1]['high'].max()
        low_price = group[1]['low'].min()
        open_price = group[1].iloc[0]['real_open']
        close_price = group[1].iloc[-1]['real_open']
        volume = group[1]['volume'].sum()
        data = {
            'datetime':group[0][0],
            'ticker':group[0][1],
            'open':open_price,
            'high':high_price,
            'low':low_price,
            'close':close_price,
            'volume':volume
        }
        data_list.append(data)
    except Exception as e :
        pass

insert_data_to_db(data_list,conn)
