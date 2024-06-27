from apscheduler.schedulers.background import BlockingScheduler
from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Optional
import pandas as pd
import pymysql
import uvicorn
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
app = FastAPI()
import datetime
import pandas as pd
import numpy as np
import ccxt
import time
import requests
from pytz import timezone
import requests
import pymysql
from datetime import timedelta
import requests
import json
import asyncio
import datetime
from datetime import timedelta
from pytz import timezone
import telegram
import time
import schedule
import time
import nest_asyncio
nest_asyncio.apply()
        
data= datetime.datetime.now(timezone('Asia/Seoul')).strftime("%Y%m%d%H")   



async def main(text): 
    text=text
    token = "5802939242:AAFNFee5qZSJWrWxqTgfnzhBl_EOtTpAGj0" 
    chat_id = '-1001617082963'
    bot = telegram.Bot(token = token)
    await bot.send_message(chat_id,text) 

try:
    engine = create_engine(
    "mysql+pymysql://staff_new:onebot9608!!@database-1-instance-1.ccjdvtwjk9lh.ap-northeast-2.rds.amazonaws.com/data",
        encoding="utf8",
    )
except pymysql.MySQLError as e:
    print(e)


try:
    conn = engine.connect()
    sql = f"""
    SELECT * FROM b_ticker_price_1h;
    """
    with conn as con:
        tickerdata=pd.read_sql(sql,con )
        
except Exception as e :
    pass


try:
    conn1 = engine.connect()
    sql = f"""
        SELECT * FROM b_ticker_price_1w ;
        """
    with conn1 as con1:
        w1data=pd.read_sql(sql,con1 )
except Exception as e :
    pass

ticker_list=tickerdata['ticker'].unique()
# ticker_list=['BTC','DOGE']
long_data_list=[]
short_data_list=[]
w1long_data_list=[]
w1short_data_list=[]

for ticker in ticker_list:
    tidata=tickerdata[tickerdata['ticker']==ticker]
    w1tidata=w1data[w1data['ticker']==ticker]
    w1end=w1tidata.iloc[-1]
    w1_high=w1end['high']
    w1_low=w1end['low']


    high=max(tidata['high'][:-1])
    low=min(tidata['low'][:-1])
    end_data=tidata.iloc[-1]
    end_data2=tidata.iloc[-2]

    end_data3=tidata.iloc[-48:-1]
    high2=max(end_data3['high'])
    low2=min(end_data3['low'])

    end_data_high=end_data['high']
    end_data_low=end_data['low']

    end_data_high2=end_data2['high']
    end_data_low2=end_data2['low']

    print(  high,low,end_data_high, end_data_low ,w1_high,w1_low)

    if ((end_data_high >= high) and (end_data_high >= high2)) :
        long_data_list.append({"ticker":end_data['ticker'],'open':end_data['open'],'high':end_data['high'],'low':end_data['low'],'close':end_data['close']})

        
    if ((end_data_low <= low) and (end_data_low <= low2)):
        short_data_list.append({"ticker":end_data['ticker'],'open':end_data['open'],'high':end_data['high'],'low':end_data['low'],'close':end_data['close']})
        
    if ((end_data_high >= w1_high) and (end_data_high >= high2))  :
        w1long_data_list.append({"ticker":end_data['ticker'],'open':end_data['open'],'high':end_data['high'],'low':end_data['low'],'close':end_data['close']})
    
    if ((end_data_low <= w1_low) and (end_data_low <= low2)):
        w1short_data_list.append({"ticker":end_data['ticker'],'open':end_data['open'],'high':end_data['high'],'low':end_data['low'],'close':end_data['close']})

print(end_data)

if len(long_data_list) >0:
    text=f"""[고점 돌파  [1H] - {data} ]\n\n"""
    for j in long_data_list:
        
        text+=f"- #{j['ticker']}:  High: {j['high']} \n"
    if len(text) !=0:
    
        asyncio.run(main(text))


if len(short_data_list) >0:
    text=f"""[저점 돌파 [1H]- {data} ]\n\n"""
    for ii in short_data_list:
    
        text+=f"- #{ii['ticker']}:  Low: {ii['low']}  \n"
    if len(text) !=0:
   

        asyncio.run(main(text))

lcount=0
scount=0

if len(w1long_data_list) >0:

    text=f"""[1주일전 대비 고점돌파  [1H] - {data} ]\n\n"""
    for j1 in w1long_data_list:
        lcount =lcount +1
        text+=f"- #{j1['ticker']}:  High: {j1['high']} \n"
   
        if lcount >= 50 :
            if len(text) !=0:
                
                
                asyncio.run(main(text))
                text=''
                text=f"""[1주일전 대비 고점돌파  [1H] - {data} ]\n\n"""
                lcount=0
    asyncio.run(main(text))

if len(w1short_data_list) >0:

    text='\n'+f"""[1주일전 대비 저점돌파 [1H]- {data} ]\n\n"""
    for ii1 in w1short_data_list:
        scount =scount +1
        text+=f"- #{ii1['ticker']}:  Low: {ii1['low']}  \n"
        if scount >= 50 :
            if len(text) !=0:
              
                
                asyncio.run(main(text))
                text=''
                text='\n'+f"""[1주일전 대비 저점돌파 [1H]- {data} ]\n\n"""
                scount=0
    asyncio.run(main(text))
