######################### minerflows ######################
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



###################################### miner flows ############################################

def miner_flows_Reserve(df,from_data,to_data,limit_data):
    headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/miner-flows/reserve?miner=f2pool&window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
        df['miner_reserve']=a1['reserve']
        df['miner_reserve_usd']=a1['reserve_usd']
   
    return df

def miner_flows_Netflow(df,from_data,to_data,limit_data):
    headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/miner-flows/netflow?miner=f2pool&window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
        df['miner_netflow_total']=a1['netflow_total']
    return df

def miner_flows_Inflow(df,from_data,to_data,limit_data):
    headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/miner-flows/inflow?miner=f2pool&window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
        df['miner_inflow_total']=a1['inflow_total']
        df['miner_inflow_top10']=a1['inflow_top10']
        df['miner_inflow_mean']=a1['inflow_mean']
        df['miner_inflow_mean_ma7']=a1['inflow_mean_ma7']
    return df


def miner_flows_Outflow(df,from_data,to_data,limit_data):
    headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/miner-flows/outflow?miner=f2pool&window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
        df['miner_outflow_total']=a1['outflow_total']
        df['miner_outflow_top10']=a1['outflow_top10']
        df['miner_outflow_mean']=a1['outflow_mean']
        df['miner_outflow_mean_ma7']=a1['outflow_mean_ma7']
    return df

def miner_flows_TransactionsCount(df,from_data,to_data,limit_data):
    headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/miner-flows/transactions-count?miner=f2pool&window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
        df['miner_transactions_count_inflow']= a1['transactions_count_inflow']
        df['miner_transactions_count_outflow']= a1['transactions_count_outflow']
    return df

def miner_flows_AddressesCount(df,from_data,to_data,limit_data):
    headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/miner-flows/addresses-count?miner=f2pool&window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
         return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
        df['miner_addresses_count_inflow']=a1['addresses_count_inflow']
        df['miner_addresses_count_outflow']=a1['addresses_count_outflow']
    return df

def miner_flows_InHouseFlow(df,from_data,to_data,limit_data):
    headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/miner-flows/in-house-flow?miner=f2pool&window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
         return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
        df['miner_flow_total']=a1['flow_total']
        df['miner_transactions_count_flow']=a1['transactions_count_flow']
        df['miner_flow_mean']=a1['flow_mean']
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
    data=miner_flows_Reserve(data,from_data,to_data,limit_data)
    data=miner_flows_Netflow(data,from_data,to_data,limit_data)
    data=miner_flows_Inflow(data,from_data,to_data,limit_data)
    data=miner_flows_Outflow(data,from_data,to_data,limit_data)
    data=miner_flows_TransactionsCount(data,from_data,to_data,limit_data)
    data=miner_flows_AddressesCount(data,from_data,to_data,limit_data)
    data=miner_flows_InHouseFlow(data,from_data,to_data,limit_data)
    

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