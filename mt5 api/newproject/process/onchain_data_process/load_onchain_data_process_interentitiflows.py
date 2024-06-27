# BTC Inter Entity Flows

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

 
def Exchange_to_Bank(df,from_data,to_data,limit_data):
    
    headers = {'Authorization': 'Bearer ' + ' U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/inter-entity-flows/exchange-to-bank?from_exchange=binance&to_bank=blockfi&window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
       
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
        
        df['exchange_to_bank_flow_total']=a1['flow_total']
        df['exchange_to_bank_transactions_count_flow']=a1['transactions_count_flow']
        df['exchange_to_bank_flow_mean']=a1['flow_mean']
        
    return df


def Miner_to_Miner(df,from_data,to_data,limit_data):
    headers = {'Authorization': 'Bearer ' + ' U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/inter-entity-flows/miner-to-miner?from_miner=f2pool&to_miner=antpool&window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
        df['mtm_flow_total']=a1['flow_total']
        df['mtm_transactions_count_flow']=a1['transactions_count_flow']
        df['mtm_flow_mean']=a1['flow_mean']
    return df

def Exchange_to_Miner(df,from_data,to_data,limit_data):
    headers = {'Authorization': 'Bearer ' + ' U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/inter-entity-flows/exchange-to-miner?from_exchange=binance&to_miner=f2pool&window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
   
        df['etn_flow_total']=a1['flow_total']
        df['etm_transactions_count_flow']=a1['transactions_count_flow']
        df['etm_flow_mean']=a1['flow_mean']
    return df




def Exchange_to_Exchange(df,from_data,to_data,limit_data):
    headers = {'Authorization': 'Bearer ' + ' U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/inter-entity-flows/exchange-to-exchange?from_exchange=binance&to_exchange=bithumb&window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
    
        df['ete_flow_total']=a1['flow_total']
        df['ete_transactions_count_flow']=a1['transactions_count_flow']
        df['ete_flow_mean']=a1['flow_mean']
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
    data=Exchange_to_Bank(data,from_data,to_data,limit_data)
    data=Miner_to_Miner(data,from_data,to_data,limit_data)
    data=Exchange_to_Miner(data,from_data,to_data,limit_data)
    data=Exchange_to_Exchange(data,from_data,to_data,limit_data)
  

    # data['date'] = datetime.datetime.strptime(data['date'][0],'%Y-%m-%d').strftime("%Y%m%d")

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