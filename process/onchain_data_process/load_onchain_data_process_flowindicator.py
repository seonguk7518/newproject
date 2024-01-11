#########################flowindicator ############

import pandas as pd
import numpy as np
import ccxt

import time
import requests
from pytz import timezone
import datetime
from datetime import timedelta
import pymysql

def flow_indicator_MPI(df,from_data,to_data,limit_data):

    headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/flow-indicator/mpi?exchange=binance&window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
        df['mpi']=a1['mpi']
        return df

def flow_indicator_ExchangeShutdownIndex(df,from_data,to_data,limit_data):

    headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/flow-indicator/exchange-shutdown-index?exchange=binance&window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
        df['is_shutdown']=a1['is_shutdown']
    return df

def flow_indicator_ExchangeWhaleRatio(df,from_data,to_data,limit_data):

    headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/flow-indicator/exchange-whale-ratio?exchange=binance&window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
        df['exchange_whale_ratio']= a1['exchange_whale_ratio']
    return df

def flow_indicator_FundFlowRatio(df,from_data,to_data,limit_data):

    headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/flow-indicator/fund-flow-ratio?exchange=binance&window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
    
        df['fund_flow_ratio']=a1['fund_flow_ratio']
    
    return df

def flow_indicator_StablecoinsRatio(df,from_data,to_data,limit_data):

    headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/flow-indicator/stablecoins-ratio?exchange=binance&window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
        df['stablecoins_ratio']=a1['stablecoins_ratio']
        df['stablecoins_ratio_usd']=a1['stablecoins_ratio_usd']
    return df




def flow_indicator_ccd(df,from_data,to_data,limit_data):

    headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/flow-indicator/exchange-inflow-cdd?exchange=binance&window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
    
        df['inflow_cdd']=a1['inflow_cdd']
    return df



def flow_indicator_ExchangeSupplyRatio(df,from_data,to_data,limit_data):

    headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/flow-indicator/exchange-supply-ratio?exchange=binance&window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
    
        df['exchange_supply_ratio']=a1['exchange_supply_ratio']
    return df

def flow_indicator_MinerSupplyRatio(df,from_data,to_data,limit_data):

    headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/flow-indicator/miner-supply-ratio?miner=f2pool&window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
    
        df['miner_supply_ratio']=a1['miner_supply_ratio']
    return df

def flow_indicator_BankSupplyRatio(df,from_data,to_data,limit_data):

    headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/flow-indicator/bank-supply-ratio?bank=binance_pegged&window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
   
        df['bank_supply_ratio']=a1['bank_supply_ratio']
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
            values_String = f"'{datetime_str}',{value},'{parameter}'"
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
    data=flow_indicator_MPI(data,from_data,to_data,limit_data)
    data=flow_indicator_ExchangeShutdownIndex(data,from_data,to_data,limit_data)
    data=flow_indicator_ExchangeWhaleRatio(data,from_data,to_data,limit_data)
    data=flow_indicator_FundFlowRatio(data,from_data,to_data,limit_data)
    data=flow_indicator_StablecoinsRatio(data,from_data,to_data,limit_data)
    data=flow_indicator_ccd(data,from_data,to_data,limit_data)
    data=flow_indicator_ExchangeSupplyRatio(data,from_data,to_data,limit_data)
    data=flow_indicator_MinerSupplyRatio(data,from_data,to_data,limit_data)
    data=flow_indicator_BankSupplyRatio(data,from_data,to_data,limit_data)
   

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

