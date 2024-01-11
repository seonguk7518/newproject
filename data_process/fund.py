import requests
import json
import asyncio
import datetime
from datetime import timedelta
from pytz import timezone
import telegram
import time
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
try:
    url = 'https://fapi.binance.com/fapi/v1/premiumIndex'
    response = requests.get(url)
    response_text = response.text
    response_dict = json.loads(response_text)

    bb=[]
    for i in response_dict:
        if i['symbol'].endswith('USDT') :
            if float(i['lastFundingRate'])> 0.0005 or float(i['lastFundingRate'])< -0.0005:
                        bb.append({'symbol':i['symbol'],
                                    'Funding_Rate':round(float(i['lastFundingRate'])*100,3)
                                })
                        
    if len(bb)>0:
        date= datetime.datetime.now(timezone('Asia/Seoul')).strftime("%Y%m%d%H")

        text =f"""[Funding_Rate - {date} ]\n\n"""
        for i in bb:
            text+=f"- {i['symbol']} : {i['Funding_Rate']} \n"
            
        async def main(text): 
            text=text
            token = "5802939242:AAFNFee5qZSJWrWxqTgfnzhBl_EOtTpAGj0" 
            chat_id = '6111013114'
            bot = telegram.Bot(token = token)
            await bot.send_message(chat_id,text)
    else :
        date= datetime.datetime.now(timezone('Asia/Seoul')).strftime("%Y%m%d%H")
        async def main(text): 
            text=text
            token = "5802939242:AAFNFee5qZSJWrWxqTgfnzhBl_EOtTpAGj0" 
            chat_id = '6111013114'
            bot = telegram.Bot(token = token)
            await bot.send_message(chat_id,text)
            
        text =f"""[Funding_Rate - {date} ]\n\n"""
        text+=f"              -no ticker"
except Exception as e :
        print(e) 
        pass
asyncio.run(main(text))

