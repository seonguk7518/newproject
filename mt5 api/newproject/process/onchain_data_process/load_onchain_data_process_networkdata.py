###################### net workdata ######################

#  BTC Network Data
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


def load_Hashrate(df,from_data,to_data,limit_data):
    headers = {'Authorization': 'Bearer ' +'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/network-data/hashrate?window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
        df['hashrate']=a1['hashrate']
    return df



def load_Difficulty(df,from_data,to_data,limit_data):
    headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/network-data/difficulty?window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
        df['difficulty']=a1['difficulty']
    return df

def load_Blockreward(df,from_data,to_data,limit_data):
    headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/network-data/blockreward?window=day&=from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
        df['blockreward']=a1['blockreward']
    return df

def load_FeesTransaction(df,from_data,to_data,limit_data):
    headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/network-data/fees-transaction?window=day&=from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()   
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
        df['fees_transaction_mean']=a1['fees_transaction_mean']
        df['fees_transaction_mean_usd']=a1['fees_transaction_mean_usd']
        df['fees_transaction_median']=a1['fees_transaction_median']
        df['fees_transaction_median_usd']=a1['fees_transaction_median_usd']
    return df



def load_Fees(df,from_data,to_data,limit_data):
    headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/network-data/fees?window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
        df['fees_block_mean']=a1['fees_block_mean']
        df['fees_block_mean_usd']=a1['fees_block_mean_usd']
        df['fees_total']=a1['fees_total']
        df['fees_total_usd']=a1['fees_total_usd']
        df['fees_reward_percent']=a1['fees_reward_percent']
    
    return df


def UTXOCount(df,from_data,to_data,limit_data):
    headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/network-data/utxo-count?window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
        df['utxo_count']=a1['utxo_count']
    return df

def BlockInterval(df,from_data,to_data,limit_data):
    headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/network-data/block-interval?window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result'] 
    if reserve_data.get('data')==[]:
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
        df['block_interval']=a1['block_interval']
    return df    


def BlockCount(df,from_data,to_data,limit_data):
    headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/network-data/block-count?window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
        df['block_count']=a1['block_count']
    return df

def BlockBytes(df,from_data,to_data,limit_data):
    headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url =f"https://api.cryptoquant.com/v1/btc/network-data/block-bytes?window=day&from{from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
        df['block_bytes']=a1['block_bytes']
    return df

def TokensTransferred(df,from_data,to_data,limit_data):
    headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/network-data/tokens-transferred?window=day&from{from_data}&to={to_data}&limit={limit_data}"   
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
        df['tokens_transferred_total']=a1['tokens_transferred_total']
        df['tokens_transferred_mean']=a1['tokens_transferred_mean']
        df['tokens_transferred_median']=a1['tokens_transferred_median']
    return df

def TransactionsCount(df,from_data,to_data,limit_data):
  
    headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/network-data/transactions-count?window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
        df['transactions_count_total']=a1['transactions_count_total']
        df['transactions_count_mean']=a1['transactions_count_mean']
    return df

def Velocity(df,from_data,to_data,limit_data):
    headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/network-data/velocity?window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
        df['velocity_supply_total']=a1['velocity_supply_total']
    return df

def Supply(df,from_data,to_data,limit_data):
    headers = {'Authorization': 'Bearer ' +'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}   
    url = f"https://api.cryptoquant.com/v1/btc/network-data/supply?window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
        df['net_supply_total']=a1['supply_total']
        df['net_supply_new']=a1['supply_new']
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
    data=load_Hashrate(data,from_data,to_data,limit_data)
    data=load_Difficulty(data,from_data,to_data,limit_data)
    data=load_Blockreward(data,from_data,to_data,limit_data)
    data=load_FeesTransaction(data,from_data,to_data,limit_data)
    data=load_Fees(data,from_data,to_data,limit_data)
    data=UTXOCount(data,from_data,to_data,limit_data)
    data=BlockInterval(data,from_data,to_data,limit_data)
    data=BlockCount(data,from_data,to_data,limit_data)
    data=BlockBytes(data,from_data,to_data,limit_data)
    data=TokensTransferred(data,from_data,to_data,limit_data)
    data=TransactionsCount(data,from_data,to_data,limit_data)
    data=Velocity(data,from_data,to_data,limit_data)
    data=Supply(data,from_data,to_data,limit_data)
  

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