# BTC Bank Flows
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

def Addresses_Count(df,from_data,to_data,limit_data):
    headers = {'Authorization': 'Bearer ' + ' U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/bank-flows/addresses-count?bank=blockfi&window=day&from={from_data}&to={to_data}&limit={limit_data}"
    
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
        df['bank_addresses_count_inflow']=a1['addresses_count_inflow']
        df['bank_addresses_count_outflow']=a1['addresses_count_outflow']
   
    return df

def Transactions_Count(df,from_data,to_data,limit_data):
    headers = {'Authorization': 'Bearer ' + ' U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/bank-flows/transactions-count?bank=blockfi&window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
        df['bank_transactions_count_inflow']=a1['transactions_count_inflow']
        df['bank_transactions_count_outflow']=a1['transactions_count_outflow']
    return df

def Outflow(df,from_data,to_data,limit_data):
    headers = {'Authorization': 'Bearer ' + ' U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/bank-flows/outflow?bank=blockfi&window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
        df['bank_outflow_total']=a1['outflow_total']
        df['bank_outflow_top10']=a1['outflow_top10']
        df['bank_outflow_mean']=a1['outflow_mean']
        df['bank_outflow_mean_ma7']=a1['outflow_mean_ma7']
    return df


def Inflow(df,from_data,to_data,limit_data):
    headers = {'Authorization': 'Bearer ' + ' U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/bank-flows/inflow?bank=blockfi&window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
        df['bank_inflow_total']=a1['inflow_total']
        df['bank_inflow_top10']=a1['inflow_top10']
        df['bank_inflow_mean']=a1['inflow_mean']
        df['bank_inflow_mean_ma7']=a1['inflow_mean_ma7']
    
    return df

def Netflow(df,from_data,to_data,limit_data):
    headers = {'Authorization': 'Bearer ' + ' U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/bank-flows/netflow?bank=blockfi&window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
        df['bank_netflow_total']=a1['netflow_total']
    return df


def Reserve(df,from_data,to_data,limit_data):
    headers = {'Authorization': 'Bearer ' + ' U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/bank-flows/reserve?bank=blockfi&window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
        df['bank_reserve']=a1['reserve']
        df['bank_reserve_usd']=a1['reserve_usd']
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
    data=Addresses_Count(data,from_data,to_data,limit_data)
    data=Transactions_Count(data,from_data,to_data,limit_data)
    data=Outflow(data,from_data,to_data,limit_data)
    data=Inflow(data,from_data,to_data,limit_data)
    data=Netflow(data,from_data,to_data,limit_data)
    data=Reserve(data,from_data,to_data,limit_data)
   

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

