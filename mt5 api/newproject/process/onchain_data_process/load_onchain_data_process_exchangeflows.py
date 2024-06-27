import pandas as pd
import numpy as np
import ccxt

import time
import requests
from pytz import timezone
import datetime
from datetime import timedelta
import pymysql

########################################## exchange flow #######################




def exchange_flows_reserve(df,from_data,to_data,limit_data):

    headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/exchange-flows/reserve?exchange=binance&window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)

        df['exchange_reserve']=a1['reserve']
        df['exchange_reserve_usd']=a1['reserve_usd']
        
        return df
    
def exchange_flows_netflow(df,from_data,to_data,limit_data):

    headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/exchange-flows/netflow?exchange=binance&window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
        df['exchange_netflow_total']=a1['netflow_total']
    return df

def exchange_flows_inflow(df,from_data,to_data,limit_data):

    headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/exchange-flows/inflow?exchange=binance&window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        btc_inflow_data=pd.DataFrame(reserve_data['data'])
        df['exchange_inflow_total']=btc_inflow_data['inflow_total']
        df['exchange_inflow_top10']=btc_inflow_data['inflow_top10']
        df['exchange_inflow_mean']=btc_inflow_data['inflow_mean']
        df['exchange_inflow_mean_ma7']=btc_inflow_data['inflow_mean_ma7']
    
    return df

def exchange_flows_outflow(df,from_data,to_data,limit_data):

    headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/exchange-flows/outflow?exchange=binance&window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        btc_outflow_data=pd.DataFrame(reserve_data['data'])
        df['exchange_outflow_total']=btc_outflow_data['outflow_total']
        df['exchange_outflow_top10']=btc_outflow_data['outflow_top10']
        df['exchange_outflow_mean']=btc_outflow_data['outflow_mean']
        df['exchange_outflow_mean_ma7']=btc_outflow_data['outflow_mean_ma7']

    return df

def exchange_flows_TransactionsCount(df,from_data,to_data,limit_data):

    headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/exchange-flows/transactions-count?exchange=binance&window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        btc_TransactionsCount_data=pd.DataFrame(reserve_data['data'])
        df['exchange_transactions_count_inflow']=btc_TransactionsCount_data['transactions_count_inflow']
        df['exchange_transactions_count_outflow']=btc_TransactionsCount_data['transactions_count_outflow']
    
    return df

def exchange_flows_AddressesCount(df,from_data,to_data,limit_data):

    headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/exchange-flows/addresses-count?exchange=binance&window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        btc_AddressesCount_data=pd.DataFrame(reserve_data['data'])
        df['exchange_addresses_count_inflow']=btc_AddressesCount_data['addresses_count_inflow']
        df['exchange_addresses_count_outflow']=btc_AddressesCount_data['addresses_count_outflow']

    return df

def exchange_flows_InHouseFlow(df,from_data,to_data,limit_data):

    headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/exchange-flows/in-house-flow?exchange=binance&window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        btc_InHouseFlow_data=pd.DataFrame(reserve_data['data'])
        df['exchange_flow_total']=btc_InHouseFlow_data['flow_total']
        df['exchange_transactions_count_flow']=btc_InHouseFlow_data['transactions_count_flow']
        df['exchange_flow_mean']=btc_InHouseFlow_data['flow_mean']
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
    data=exchange_flows_reserve(data,from_data,to_data,limit_data)
    data=exchange_flows_netflow(data,from_data,to_data,limit_data)
    data=exchange_flows_inflow(data,from_data,to_data,limit_data)
    data=exchange_flows_outflow(data,from_data,to_data,limit_data)
    data=exchange_flows_TransactionsCount(data,from_data,to_data,limit_data)
    data=exchange_flows_AddressesCount(data,from_data,to_data,limit_data)
    data=exchange_flows_InHouseFlow(data,from_data,to_data,limit_data)
    

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





