from fastapi import FastAPI
import pandas as pd
import pymysql
import uvicorn
from sqlalchemy import create_engine
from aiohttp import ClientSession
from enum import Enum
from datetime import timedelta
from pytz import timezone
from datetime import datetime
import nest_asyncio
from pandas import DataFrame
import requests
import time
nest_asyncio.apply()




engine = create_engine(
    "mysql+pymysql://datateam:datateam1234$$@52.79.226.104/chart_data",
        encoding="utf8",
    )
pre_time = (datetime.now(timezone('Asia/Seoul'))-timedelta(hours=2)).strftime('%Y-%m-%d %H:00:00')
now_time=(datetime.now(timezone('Asia/Seoul'))-timedelta(hours=1)).strftime('%Y-%m-%d %H:00:00')
data_time=(datetime.now(timezone('Asia/Seoul'))-timedelta(hours=1)).strftime('%Y%m%d%H00')
try:
    conn = engine.connect()
    sql = f"""
    SELECT * FROM signal_data where datetime='{pre_time}' or datetime='{now_time}'  order by ticker, datetime desc;
    """
    with conn as con:

        new_signal_data=pd.read_sql(sql,con )
except Exception as e:
        print(e)
        pass


try:
    conn = engine.connect()
    sql = f"""
    select  * from  b2_ticker_price_1h btph  where datetime='{data_time}';
    """
    with conn as con:

        pink=pd.read_sql(sql,con )
except Exception as e:
    print(e)
    pass


ticker_list=new_signal_data['ticker'].unique()  
# ticker_list=['BTC']
for ticker in ticker_list:

    dt_utc = datetime.now() + timedelta(hours=-24)
    dt_utc = dt_utc.strftime('%Y-%m-%d %H:%M:%S')
    timestamp = time.mktime(datetime.strptime(dt_utc, '%Y-%m-%d %H:%M:%S').timetuple())*1000
   

    try:
        
        resp_1h=requests.get(f'https://fapi.binance.com/fapi/v1/klines?' + \
                f'symbol={ticker}USDT&interval=1h&startTime={round(timestamp)}&limit=200').json()
            
            

    
        result=dict(
            ticker=ticker, resp_1h=resp_1h
        )
#        print(result)
    except:
        resp_1h = dict(result='해당 데이터를 수집할 수 없습니다.')
    

    columns = [
                        'datetime', 'open', 'high', 'low', 'close', 'volume', 
                        'Close time', 'Quote asset volume', 'Number of trades', 
                        'Taker buy base asset volume', 'Taker buy quote asset volume', 'Ignore'
                    ]
    ticker=result['ticker']
    resp_1h= result['resp_1h']


    # DataFrame 생성
    df1 = DataFrame(data=resp_1h, columns=columns) # 1시간 봉


    df_1h = df1.astype({
                'open' : float, 'high' : float, 'low' : float, 'close' : float, 'volume' : float
            })



    df_1h['datetime'] = pd.to_datetime(df_1h['datetime'], unit='ms')
    df_1h['ticker']=ticker
    for i in range(len(df_1h)) :
        df_1h.loc[i,'datetime'] = df_1h.loc[i,'datetime'] + timedelta(hours=9)  

   

    new_state=''
    new_signal=''
    new_rsi_signal=''

    new_signal_data1=new_signal_data[new_signal_data['ticker']==ticker]
    new_pink=pink[pink['ticker']==ticker]

    new_signal_data1.reset_index(inplace=True)
    new_pink.reset_index(inplace=True)
  
    
    if len(new_signal_data1)>=2:
        try:
            print("전시간봉 업데이트")

            now_state=new_signal_data1['basestate'][0][-8:-7]
            pre_state=new_signal_data1['basestate'][1][-8:-7]
            now_rsi=new_signal_data1['sar'][0]
            pre_rsi=new_signal_data1['sar'][1]

            if ((pre_state != now_state)  or (now_rsi != pre_rsi)):
                # 숏
                if now_state == now_rsi =='S':

                    if new_pink['close'][0] < new_pink['pink'][0]:

                        new_signal='S'


                elif now_state == now_rsi =='L':

                    if new_pink['close'][0] > new_pink['pink'][0]:
                        
                        new_signal='L'
        
            conn = pymysql.connect(
                    host='52.79.226.104',
                    user='datateam',
                    password='datateam1234$$',
                    db='chart_data',
                    charset='utf8',
                    port=3306
                    )

            with conn.cursor() as cur:


                sql = ''' UPDATE retri set open = %s,close= %s ,rsi = %s ,rsi_signal= %s,  basesignal= %s, basestate =%s, new_state =%s , new_signal = %s
                where `datetime` = %s and ticker = %s
                '''
                data=(
                    df_1h['open'].iloc[-2],
                    df_1h['close'].iloc[-2],
                    new_signal_data1['sar'][0],
                    new_rsi_signal,
                    new_signal_data1['basesignal'][0],
                    new_signal_data1['basestate'][0],
                    new_state,
                    new_signal,
                    now_time,
                    ticker)
                cur.execute(sql,data)

            conn.commit()  
            print("전시간봉 업데이트",data) 
        except Exception as e:
            print(e)
            pass 

