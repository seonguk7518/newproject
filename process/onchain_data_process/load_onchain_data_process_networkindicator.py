################################# networkindicator #######################


######################################  Network Indicator ############################################

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


def network_indicator_StocktoFlow(df,from_data,to_data,limit_data):
    headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/network-indicator/stock-to-flow?window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
        df['stock_to_flow']=a1['stock_to_flow']
        df['stock_to_flow_reversion']=a1['stock_to_flow_reversion']
    return df

def network_indicator_NVT(df,from_data,to_data,limit_data):
    headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/network-indicator/nvt?window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
        df['nvt']=a1['nvt']
    return df

def network_indicator_NVTGoldenCross(df,from_data,to_data,limit_data):
    headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/network-indicator/nvt-golden-cross?window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
        df['nvt_golden_cross']=a1['nvt_golden_cross']
    return df

def network_indicator_NVM(df,from_data,to_data,limit_data):
    headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/network-indicator/nvm?window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
        df['nvm']=a1['nvm']
    return df

def network_indicator_PuellMultiple(df,from_data,to_data,limit_data):
    headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/network-indicator/puell-multiple?window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
        df['puell_multiple']=a1['puell_multiple']
    return df

def network_indicator_cdd(df,from_data,to_data,limit_data):
    headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/network-indicator/cdd?window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
        df['cdd']=a1['cdd']
        df['sa_cdd']=a1['sa_cdd']
        df['average_sa_cdd']=a1['average_sa_cdd']
        df['binary_cdd']=a1['binary_cdd']
    return df

def network_indicator_MeanCoinAge(df,from_data,to_data,limit_data):
    headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/network-indicator/mca?window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
        df['mca']=a1['mca']
        df['mcda']=a1['mcda']
    return df

def network_indicator_SumCoinAge(df,from_data,to_data,limit_data):
    headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/network-indicator/sca?window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
        df['sca']=a1['sca']
        df['scda']=a1['scda']
    return df

# def network_indicator_SumCoinAgeDistribution(from_data,to_data,limit_data):
#     headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
#     url = f"https://api.cryptoquant.com/v1/btc/network-indicator/sca-distribution?window=day&from={from_data}&to={to_data}&limit={limit_data}"
#     data=requests.get(url, headers=headers).json()
#     reserve_data=data['result']
#     btc_SumCoinAgeDistribution_data=pd.DataFrame(reserve_data['data'])
#     return btc_SumCoinAgeDistribution_data

def network_indicator_nupl(df,from_data,to_data,limit_data):
    headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/network-indicator/nupl?window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
        df['nupl']=a1['nupl']
        df['nup']=a1['nup']
        df['nul']=a1['nul']
    return df

def network_indicator_pnl_utxo(df,from_data,to_data,limit_data):
    headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/network-indicator/pnl-utxo?window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
        df['utxo_profit_percent']=a1['profit_percent']
        df['utxo_loss_percent']=a1['loss_percent']
        df['utxo_profit_amount']=a1['profit_amount']
        df['utxo_loss_amount']=a1['loss_amount']
    return df

def network_indicator_pnl_supply(df,from_data,to_data,limit_data):
    headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/network-indicator/pnl-supply?window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
        df['supply_profit_percent']=a1['profit_percent']
        df['supply_loss_percent']=a1['loss_percent']
        df['supply_profit_amount']=a1['profit_amount']
        df['supply_loss_amount']=a1['loss_amount']
    return df

def network_indicator_Dormancy(df,from_data,to_data,limit_data):
    headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
    url = f"https://api.cryptoquant.com/v1/btc/network-indicator/dormancy?window=day&from={from_data}&to={to_data}&limit={limit_data}"
    data=requests.get(url, headers=headers).json()
    reserve_data=data['result']
    if reserve_data.get('data')==[]:
        return df
    else:
        aa=reserve_data['data']
        a1=pd.DataFrame(aa)
        df['average_dormancy']=a1['average_dormancy']
        df['sa_average_dormancy']=a1['sa_average_dormancy']
    return df

# def network_indicator_UTxOAgeDistribution(from_data,to_data,limit_data):
#     headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
#     url = f"https://api.cryptoquant.com/v1/btc/network-indicator/utxo-age-distribution?window=day&from={from_data}&to={to_data}&limit={limit_data}"
#     data=requests.get(url, headers=headers).json()
#     reserve_data=data['result']
#     btc_UTxOAgeDistribution_data=pd.DataFrame(reserve_data['data'])
#     return btc_UTxOAgeDistribution_data

# def network_indicator_UTxORealizedAgeDistribution(from_data,to_data,limit_data):
#     headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
#     url = f"https://api.cryptoquant.com/v1/btc/network-indicator/utxo-realized-age-distribution?window=day&from={from_data}&to={to_data}&limit={limit_data}"
#     data=requests.get(url, headers=headers).json()
#     reserve_data=data['result']
#     btc_UTxORealizedAgeDistribution_data=pd.DataFrame(reserve_data['data'])
#     return btc_UTxORealizedAgeDistribution_data

# def network_indicator_UTxOCountAgeDistribution(from_data,to_data,limit_data):
#     headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
#     url = f"https://api.cryptoquant.com/v1/btc/network-indicator/utxo-count-age-distribution?window=day&from={from_data}&to={to_data}&limit={limit_data}"
#     data=requests.get(url, headers=headers).json()
#     reserve_data=data['result']
#     btc_UTxOCountAgeDistribution_data=pd.DataFrame(reserve_data['data'])
#     return btc_UTxOCountAgeDistribution_data

# def network_indicator_SpentOutputAgeDistribution(from_data,to_data,limit_data):
#     headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
#     url = f"https://api.cryptoquant.com/v1/btc/network-indicator/spent-output-age-distribution?window=day&from={from_data}&to={to_data}&limit={limit_data}"
#     data=requests.get(url, headers=headers).json()
#     reserve_data=data['result']
#     btc_SpentOutputAgeDistribution_data=pd.DataFrame(reserve_data['data'])
#     return btc_SpentOutputAgeDistribution_data

# def network_indicator_UTxOSupplyDistribution(from_data,to_data,limit_data):
#     headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
#     url = f"https://api.cryptoquant.com/v1/btc/network-indicator/utxo-supply-distribution?window=day&from={from_data}&to={to_data}&limit={limit_data}"
#     data=requests.get(url, headers=headers).json()
#     reserve_data=data['result']
#     btc_UTxOSupplyDistribution_data=pd.DataFrame(reserve_data['data'])
#     return btc_UTxOSupplyDistribution_data

# def network_indicator_UTxORealizedSupplyDistribution(from_data,to_data,limit_data):
#     headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
#     url = f"https://api.cryptoquant.com/v1/btc/network-indicator/utxo-realized-supply-distribution?window=day&from={from_data}&to={to_data}&limit={limit_data}"
#     data=requests.get(url, headers=headers).json()
#     reserve_data=data['result']
#     btc_UTxORealizedSupplyDistribution_data=pd.DataFrame(reserve_data['data'])
#     return btc_UTxORealizedSupplyDistribution_data

# def network_indicator_UTxOCountSupplyDistribution(from_data,to_data,limit_data):
#     headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
#     url = f"https://api.cryptoquant.com/v1/btc/network-indicator/utxo-count-supply-distribution?window=day&from={from_data}&to={to_data}&limit={limit_data}"
#     data=requests.get(url, headers=headers).json()
#     reserve_data=data['result']
#     btc_UTxOCountSupplyDistribution_data=pd.DataFrame(reserve_data['data'])
#     return btc_UTxOCountSupplyDistribution_data

# def network_indicator_SpentOutputSupplyDistribution(from_data,to_data,limit_data):
#     headers = {'Authorization': 'Bearer ' + 'U5E9G1Xu56wrIdxrq0nPrmSPByqTMCyGuaWxqoEm'}
#     url = f"https://api.cryptoquant.com/v1/btc/network-indicator/spent-output-supply-distribution?window=day&from={from_data}&to={to_data}&limit={limit_data}"
#     data=requests.get(url, headers=headers).json()
#     reserve_data=data['result']
#     btc_SpentOutputSupplyDistribution_data=pd.DataFrame(reserve_data['data'])
#     return btc_SpentOutputSupplyDistribution_data

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
    data=network_indicator_StocktoFlow(data,from_data,to_data,limit_data)
    data=network_indicator_NVT(data,from_data,to_data,limit_data)
    data=network_indicator_NVTGoldenCross(data,from_data,to_data,limit_data)
    data=network_indicator_NVM(data,from_data,to_data,limit_data)
    data=network_indicator_PuellMultiple(data,from_data,to_data,limit_data)
    data=network_indicator_cdd(data,from_data,to_data,limit_data)
    data=network_indicator_MeanCoinAge(data,from_data,to_data,limit_data)
    data=network_indicator_SumCoinAge(data,from_data,to_data,limit_data)
    data=network_indicator_nupl(data,from_data,to_data,limit_data)
    data=network_indicator_pnl_utxo(data,from_data,to_data,limit_data)
    data=network_indicator_pnl_supply(data,from_data,to_data,limit_data)
    data=network_indicator_Dormancy(data,from_data,to_data,limit_data)
  

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