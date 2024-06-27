import pandas as pd
import numpy as np
import ccxt
import datetime
import time
import requests
from pytz import timezone

import pymysql

binance = ccxt.binanceusdm()
coins_trd = binance.fetch_tickers()
host = "database-1-instance-1.ccjdvtwjk9lh.ap-northeast-2.rds.amazonaws.com"
port=3306
name="admin"
password="onebot7085!!"
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

def load_coin_data(symbol, time_m) :   #타입 변환
    dt_utc = datetime.datetime.now() + datetime.timedelta(hours=-2)  #KST -> UTC

    dt_utc = dt_utc.strftime('%Y-%m-%d %H:%M:%S')  #datetime -> str
    timestamp = time.mktime(datetime.datetime.strptime(dt_utc, '%Y-%m-%d %H:%M:%S').timetuple())*1000  #stamp형태로 변환

    BTC_data = binance.fetch_ohlcv(symbol, time_m, since = round(timestamp))  #코인명, 코인시세 간격 입력
    df_BTC = pd.DataFrame(BTC_data, columns = ['datetime','Open','High','Low','Close','Volume'])
    
    df_BTC['datetime'] = pd.to_datetime(df_BTC['datetime'], unit='ms')
    
    for i in range(len(df_BTC)) :
        df_BTC.loc[i,'datetime'] = df_BTC.loc[i,'datetime'] + datetime.timedelta(hours=9)  #UTC -> KST    

    # df_BTC.set_index('datetime', inplace=True)

    return df_BTC

def generate_price_dict(coin_dict, usdt_keys ) : 
    data_list = []
    for usdt_key in usdt_keys :
        try : 
            coin_data = load_coin_data(usdt_key,'1h').iloc[-2]
            ticker =  usdt_key.split(":")[0].split("/")[0]
            open_price = coin_dict[usdt_key]['info']['lastPrice']
            real_open_price = coin_data['Close']
            volume = coin_data['Volume']
            high_price = coin_data['High']
            low_price = coin_data['Low']
            data_dict = {}
            data_dict['ticker'] = ticker
            data_dict['real_open'] = float(coin_data['Close'])
            data_dict['open'] = float(open_price)
            data_dict['high'] = float(high_price)
            data_dict['low'] = float(low_price)
            data_dict['volume'] = float(volume)
            data_dict['real_open'] = float(real_open_price)
            data_list.append(data_dict)
        except Exception as e:
            print(e)
    return data_list

def get_onebot_score_data(ticker,ob_number=1) :
    date= datetime.datetime.now(timezone('Asia/Seoul'))
    mktime = int(time.mktime(date.timetuple()))
    data_df = {
    }
    newTickerName = ticker + 'USDT'
    
    requestData = requests.get(f'https://won.korbot.com/page/predict_chart_api.php?kind={newTickerName}&ob_number={ob_number}&query_mktime={mktime}')
    if requestData.status_code == 200 : 
            requestData = requestData.json()
            try : 
                for data in requestData.keys() :
                    if data != 'info' :
                        scoreData = requestData[data]
                        new_date = datetime.datetime.strptime(scoreData['date'],'%Y-%m-%d %H:%M:%S').strftime("%Y%m%d%H")
                        data_df['datetime'] = new_date
                        data_df['score'] = scoreData['score']
                        data_df['ticker'] = ticker
            except :
                data_df = {}
    return data_df

def merge_onebot_score_data(price_data_dict) : 
    new_price_data = []
    for price_data in price_data_dict :
        ticker = price_data['ticker']
        onebotScoreData = get_onebot_score_data(ticker)
        if onebotScoreData :
            price_data['score'] = onebotScoreData['score']
            price_data['datetime'] = onebotScoreData['datetime']
            new_price_data.append(price_data)
    return new_price_data

def insert_data_to_db(price_data_list, conn) : 
    with conn.cursor() as cur : 
        for price_data in price_data_list : 
            ticker= price_data['ticker']
            datetime_str = price_data['datetime']
            open = price_data['open']
            high = price_data['high']
            low = price_data['low']
            volume = price_data['volume']
            score = price_data['score']
            real_open = price_data['real_open']
            values_String = f"'{datetime_str}','{ticker}',{open},{high},{low},{volume},{score},{real_open}"
            sql = f"Insert into ticker_price_score (datetime,ticker,open,high,low,volume,score,real_open) values ({values_String})"
            cur.execute(sql)
        conn.commit()
    
usdt_key_list = filter_usdt_key(coins_trd)
price_data_dict = generate_price_dict(coins_trd,usdt_key_list)
price_data_dict = merge_onebot_score_data(price_data_dict)
# print(price_data_dict)

try:
    conn = pymysql.connect(host=host,port=port, user=name, passwd=password, db=db_name, connect_timeout=5)
except pymysql.MySQLError as e:
    print(e)

insert_data_to_db(price_data_dict,conn)
