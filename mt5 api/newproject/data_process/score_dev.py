import pandas as pd
import numpy as np
import ccxt
import time
from pytz import timezone
import requests
import pymysql
from datetime import timedelta
import json
import asyncio
import datetime
from pytz import timezone
import telegram
import nest_asyncio
nest_asyncio.apply()



host = "database-1-instance-1.ccjdvtwjk9lh.ap-northeast-2.rds.amazonaws.com"
port=3306
name="staff_new"
password="onebot9608!!"
db_name="data"



def filter_usdt_key(coin_dict) :
    usdt_key_list = []
    for key in coin_dict.keys() :
        try:  
            currency = key.split(":")[1]
            if currency == 'USDT' :
                usdt_key_list.append(key)
        except Exception as e:
            print(e)
    return usdt_key_list

def get_score_dev(ticker):
    try:
        newTickerName = ticker + 'USDT'
        new_dict={'date':[],'score':[],'ticker':[]}
        date= datetime.datetime.now(timezone('Asia/Seoul'))
        mktime = int(time.mktime(date.timetuple()))
        data_df = {
        }


        requestData = requests.get(f'https://won.korbot.com/page/predict_chart_api.php?kind={newTickerName}&ob_number=2&query_mktime={mktime}')
        if requestData.status_code == 200 : 
                requestData = requestData.json()
                
                for data in requestData.keys() :
                        if data != 'info' :
                                scoreData = requestData[data]
                                new_dict['date'].append(scoreData['date'])
                                new_dict['score'].append(scoreData['score'])
                                new_dict['ticker'].append(ticker)
        df_newdict=pd.DataFrame(new_dict)
        dev=df_newdict['score'][1]-df_newdict['score'][0]
        percentage=round((df_newdict['score'][1]-(df_newdict['score'][0]))/(np.abs(df_newdict['score'][0]))*100,1)
        data_dict={}
        data_dict['datetime']=[df_newdict['date'][1]]
        data_dict['score_dev']=[dev]
        data_dict['score_percentage']=percentage
        data_dict['ticker']=[ticker]
    except Exception as e :
        print(e) 
        pass
    return pd.DataFrame(data_dict)


def insert_data_to_db(index_data_list, conn) :
    with conn.cursor() as cur : 
        for index_data in index_data_list : 
            ticker= index_data['ticker']
            datetime_str = index_data['datetime']
            value = index_data['score_dev']
            percentage=index_data['score_percentage']
            values_String = f"'{datetime_str}','{ticker}',{value},{percentage}"
            sql = f"Insert into score_dev (datetime,ticker,score_dev,score_percentage) values ({values_String})"
            cur.execute(sql)
        conn.commit()


binance = ccxt.binanceusdm()
coins_trd = binance.fetch_tickers()

ticker_list=filter_usdt_key(coins_trd)
score_list=[]

for usdt_key in ticker_list :
    try:
        ticker =  usdt_key.split(":")[0].split("/")[0]
        b=get_score_dev(ticker)
        print(b)
        score_list.append(b)
    except Exception as e :
            print(e) 
            pass
ab=pd.concat(score_list)
date= datetime.datetime.now(timezone('Asia/Seoul')).strftime("%Y%m%d%H")


new_data_list=[]
for i in ab.iterrows():
    data=i[1]
    new_dict={}
    new_dict['datetime']=date
    new_dict['score_dev']=round(float(data['score_dev']),1)
    new_dict['score_percentage']=data['score_percentage']
    new_dict['ticker']=data['ticker']
    new_data_list.append(new_dict)

try:
    conn = pymysql.connect(host=host,port=port, user=name, passwd=password, db=db_name, connect_timeout=5)
except pymysql.MySQLError as e:
    print(e)

insert_data_to_db(new_data_list,conn)
print(len(ab))


