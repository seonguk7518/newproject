###################### marketindicator ###########################


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


########################################## market indicator #######################

def market_indicator_EstimatedLeverageRatio(df,from_data,to_data,limit_data):
    headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/market-indicator/estimated-leverage-ratio?exchange=binance&window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
        df['estimated_leverage_ratio']=a1['estimated_leverage_ratio']
    
    return df

def market_indicator_StablecoinSupplyRatio(df,from_data,to_data,limit_data):
    headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/market-indicator/stablecoin-supply-ratio?window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
        df['stablecoin_supply_ratio']=a1['stablecoin_supply_ratio']
    return df


def market_indicator_MVRV(df,from_data,to_data,limit_data):
    headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/market-indicator/mvrv?window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
        df['mvrv']=a1['mvrv']
    return df

def market_indicator_SpentOutputProfitRatio(df,from_data,to_data,limit_data):
    headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/market-indicator/sopr?window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
        df['sopr']=a1['sopr']
        df['a_sopr']=a1['a_sopr']
        df['sth_sopr']=a1['sth_sopr']
        df['lth_sopr']=a1['lth_sopr']
    return df

def market_indicator_SOPRRatio(df,from_data,to_data,limit_data):
    headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/market-indicator/sopr-ratio?window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
        df['sopr_ratio']=a1['sopr_ratio']
    return df

def market_indicator_RealizedPrice(df,from_data,to_data,limit_data):
    headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/market-indicator/realized-price?window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
        df['realized_price']=a1['realized_price']
    return df

def market_indicator_UTxORealizedPriceAgeDistribution(df,from_data,to_data,limit_data):
    headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/market-indicator/utxo-realized-price-age-distribution?window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
        df['UTxOAge_range_0d_1d']=a1['range_0d_1d']
        df['UTxOAge_range_1d_1w']=a1['range_1d_1w']
        df['UTxOAge_range_1w_1m']=a1['range_1w_1m']
        df['UTxOAge_range_1m_3m']=a1['range_1m_3m']
        df['UTxOAge_range_3m_6m']=a1['range_3m_6m']
        df['UTxOAge_range_6m_12m']=a1['range_6m_12m']
        df['UTxOAge_range_12m_18m']=a1['range_12m_18m']
        df['UTxOAge_range_18m_2y']=a1['range_18m_2y']
        df['UTxOAge_range_2y_3y']=a1['range_2y_3y']
        df['UTxOAge_range_3y_5y']=a1['range_3y_5y']
        df['UTxOAge_range_5y_7y']=a1['range_5y_7y']
        df['UTxOAge_range_7y_10y']=a1['range_7y_10y']
        df['UTxOAge_range_10y_inf']=a1['range_10y_inf']
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
    data= market_indicator_EstimatedLeverageRatio(data,from_data,to_data,limit_data)
    data= market_indicator_StablecoinSupplyRatio(data,from_data,to_data,limit_data)
    data= market_indicator_MVRV(data,from_data,to_data,limit_data)
    data= market_indicator_SpentOutputProfitRatio(data,from_data,to_data,limit_data)
    data= market_indicator_SOPRRatio(data,from_data,to_data,limit_data)
    data= market_indicator_RealizedPrice(data,from_data,to_data,limit_data)
    data= market_indicator_UTxORealizedPriceAgeDistribution(data,from_data,to_data,limit_data)
    

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
