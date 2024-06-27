
import pandas as pd
import numpy as np
import ccxt
import datetime
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


def get_liqi():
    try:
        url = "https://open-api.coinglass.com/public/v2/liquidation_top?time_type=h1"

        headers = {
            "accept": "application/json",
            "coinglassSecret": "7c8af43420a5462b943e8737b935fed5"
        }

        response = requests.get(url, headers=headers)


        a=response.json()
        aa=a['data']
        aa=pd.DataFrame(aa)
        aa['to']=aa['totalVolUsd']/1000
        aa['long_k']=aa['longVolUsd']/1000
        aa['short_k']=aa['shortVolUsd']/1000
        data= datetime.datetime.now(timezone('Asia/Seoul')).strftime("%Y%m%d%H")


        url1 = "https://open-api.coinglass.com/public/v2/liquidation_ex?time_type=h1&symbol=all"

        headers1 = {
            "accept": "application/json",
            "coinglassSecret": "7c8af43420a5462b943e8737b935fed5"
        }

        response1 = requests.get(url1, headers=headers1)
        a1=response1.json()
        aa1=a1['data']
        aa1=pd.DataFrame(aa1)
        aa1['to']=aa1['totalVolUsd']/1000
        aa1['long_k']=aa1['longVolUsd']/1000
        aa1['short_k']=aa1['shortVolUsd']/1000

        aaa1=aa1[aa1['exchangeName']=='Binance']

        new_dict={'datatime':[],'long':[],'short':[],'total':[],'ticker':[]}
        for i in aa.iterrows():
            data1=i[1]

            if data1['to']>50:
                new_dict['datatime'].append(data)
                new_dict['long'].append(data1['long_k'])
                new_dict['short'].append(data1['short_k'])
                new_dict['total'].append(data1['to'])
                new_dict['ticker'].append(data1['symbol'])
        new_dict1=pd.DataFrame(new_dict)

        text=f"""[롱/숏 청산량  [1H] - {data} ]\n\n"""

        print(new_dict1)

        for j in new_dict1.iterrows():
            j1=j[1]
            text+=f"- #{j1['ticker']}:    Long: ${round(j1['long'],2)}K     Short: ${round(j1['short'],2)}K     Total: ${round(j1['total'],2)}K  \n"

        text+='\n'+f"""[바이낸스 롱/숏 청산량 [1H]- {data} ]\n\n"""

        for ii in aaa1.iterrows():
            iii1=ii[1]
            text+= f"Long: ${round(iii1['long_k'],2)}K     Short: ${round(iii1['short_k'],2)}K     Total: ${round(iii1['to'],2)}K  \n"


        async def main(text): 
            text=text
            token = "5802939242:AAFNFee5qZSJWrWxqTgfnzhBl_EOtTpAGj0" 
            chat_id = '6111013114'
            bot = telegram.Bot(token = token)
            await bot.send_message(chat_id,text)
        asyncio.run(main(text))
    except Exception as e:
        print(e)
    
get_liqi()
