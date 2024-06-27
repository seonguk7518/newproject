from fastapi import FastAPI
import pandas as pd
import uvicorn
import pandas as pd
import numpy as np
import ccxt
import datetime
import requests
import json
import datetime
from datetime import timedelta
from pytz import timezone
import time
app = FastAPI()


@app.get("/onebot")
async def read_data():
    binance = ccxt.binanceusdm()
    coins_trd = binance.fetch_tickers()

    usdt_key_list = []
    for key in coins_trd .keys() :
        try:  
            currency = key.split(":")[1]
            if currency == 'USDT' :
                usdt_key_list.append(key)
        except Exception as e:
            print(e)

    score_list=[]

    for usdt_key in usdt_key_list :
        try:
            ticker =  usdt_key.split(":")[0].split("/")[0]
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
            data_dict={}
            data_dict['datetime']=[df_newdict['date'][1]]
            data_dict['score_dev']=[dev]
            data_dict['ticker']=[ticker]

            b=pd.DataFrame(data_dict)
            score_list.append(b)
        except Exception as e :
            print(e) 
            pass

    ab=pd.concat(score_list)
    up50=ab[ab['score_dev']>=50]
    down50=ab[ab['score_dev']<=-50]
    onbotdata=pd.concat([up50,down50],axis=0)

    data_list=[]
    for i in onbotdata.iterrows():
        data=i[1]
        new_dict={}
        new_dict['datetime']=data['datetime']
        new_dict['score_dev']=data['score_dev']
        new_dict['ticker']=data['ticker']
        data_list.append(new_dict)
        
    return  data_list

if __name__ == "__main__":
    uvicorn.run(app="onbotdev:app", host="0.0.0.0", port=8000,reload=True)