#################### funddata #####################

# BTC Fund Data
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

def Digital_Asset_Holdings(df,from_data,to_data,limit_data):
    headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/fund-data/digital-asset-holdings?symbol=gbtc&window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:

        return df
    else:
        
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
        df['digital_asset_holdings']=a1['digital_asset_holdings']
    
    return df

def Market_Premium(df,from_data,to_data,limit_data):
    headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/fund-data/market-premium?symbol=gbtc&window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
  
        df['market_premium']=a1['market_premium']
    return df


def Market_Volume(df,from_data,to_data,limit_data):
    headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/fund-data/market-volume?symbol=gbtc&window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
   
        df['market_volume']=a1['volume']
    return df

def Market_Price(df,from_data,to_data,limit_data):
    headers = {'Authorization': 'Bearer ' +' U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/fund-data/market-price-usd?symbol=gbtc&window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)

        df['price_usd_open']=a1['price_usd_open']
        df['price_usd_high']=a1['price_usd_high']
        df['price_usd_low']=a1['price_usd_low']
        df['price_usd_close']=a1['price_usd_close']
        df['price_usd_adj_close']=a1['price_usd_adj_close']
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
    data=Digital_Asset_Holdings(data,from_data,to_data,limit_data)
    data=Market_Premium(data,from_data,to_data,limit_data)
    data=Market_Volume(data,from_data,to_data,limit_data)
    data=Market_Price(data,from_data,to_data,limit_data)


 

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