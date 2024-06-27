################## marketdata #############

import pandas as pd
import numpy as np
import ccxt
import datetime
from datetime import timedelta
import time
import requests
from pytz import timezone
import requests
import pymysql



def Coinbase_Premuim_Index(df,from_data,to_data,limit_data):
    headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/market-data/coinbase-premium-index?window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
        df['coinbase_premium_gap']=a1['coinbase_premium_gap']
        df['coinbase_premium_index']=a1['coinbase_premium_index']
    return df


def Capitalization(df,from_data,to_data,limit_data):
    headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/market-data/capitalization?window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
        df['market_cap']=a1['market_cap']
        df['realized_cap']=a1['realized_cap']
        df['average_cap']=a1['average_cap']
        df['delta_cap']=a1['delta_cap']
        df['thermo_cap']=a1['thermo_cap']
    return df

def Liquidations(df,from_data,to_data):
    headers = {'Authorization': 'Bearer ' +'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/market-data/liquidations?window=day&from={from_data}&to={to_data}&exchange=deribit"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
        df['long_liquidations']=a1['long_liquidations']
        df['short_liquidations']=a1['short_liquidations']
        df['long_liquidations_usd']=a1['long_liquidations_usd']
        df['short_liquidations_usd']=a1['short_liquidations_usd']
    
    return df

def Taker_Buy_Sell_Stats(df,from_data,to_data):
    headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/market-data/taker-buy-sell-stats?window=day&from={from_data}&to={to_data}&exchange=bitmex"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
        df['taker_buy_volume']=a1['taker_buy_volume']
        df['taker_sell_volume']=a1['taker_sell_volume']
        df['taker_buy_ratio']=a1['taker_buy_ratio']
        df['taker_sell_ratio']=a1['taker_sell_ratio']
        df['taker_buy_sell_ratio']=a1['taker_buy_sell_ratio']
    return df

def Funding_Rate(df,from_data,to_data):
    headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/market-data/funding-rates?window=day&from={from_data}&to={to_data}&exchange=bitmex"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
        df['funding_rates']=a1['funding_rates']
    return df

def Open_Interest(df,from_data,to_data):
    headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/market-data/open-interest?window=day&from={from_data}&to={to_data}&exchange=bitmex"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
        df['open_interest']=a1['open_interest']
    return df

def Price_OHLCV(df,from_data,to_data,limit_data):
    headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/market-data/price-ohlcv?window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
        df['open']=a1['open']
        df['high']=a1['high']
        df['low']=a1['low']
        df['close']=a1['close']
        df['volume']=a1['volume']
    return df



def get_days_ago_date(days=6) :
    base_line_date = (datetime.datetime.now() - timedelta(days=days))
    to_data=base_line_date
    from_data=to_data - timedelta(days=1)
    to_data=to_data.strftime("%Y%m%d")
    from_data=to_data
    return from_data,to_data

def insert_data_to_db(index_data_list, conn) :
    with conn.cursor() as cur : 
        for index_data in index_data_list : 
         
            datetime_str = index_data['datetime']
            value = index_data['value']
            parameter = index_data['parameter']
            values_String = f"'{datetime_str}','{value}','{parameter}'"
            sql = f"Insert into onchain_data_daily (datetime,value,parameter) values ({values_String})"
            cur.execute(sql)
        conn.commit()

host = "database-1-instance-1.ccjdvtwjk9lh.ap-northeast-2.rds.amazonaws.com"
port=3306
name="admin"
password="onebot7085!!"
db_name="onchain_data"
conn = pymysql.connect(host=host,port=port, user=name, passwd=password, db=db_name, connect_timeout=5)

from_data,to_data=get_days_ago_date(1)
limit_data=1


try:
    data=pd.DataFrame()
    data['date']=[from_data]
    data=Coinbase_Premuim_Index(data,from_data,to_data,limit_data)
    data=Capitalization(data,from_data,to_data,limit_data)
    data=Liquidations(data,from_data,to_data)
    data=Funding_Rate(data,from_data,to_data)
    data=Taker_Buy_Sell_Stats(data,from_data,to_data)
    data=Open_Interest(data,from_data,to_data)
    data=Price_OHLCV(data,from_data,to_data,limit_data)
   

    data_list=[]

    for i in data.columns:

        if i != 'date' :
            data_list.append(
                    {'datetime':data['date'][0],
                
                    'value':data[f'{i}'][0],
                    'parameter':f'{i}'
                    })

    
except Exception as e :
        print(e)


if len(data_list)==0:
    
    pass
else:
    insert_data_to_db(data_list,conn)


