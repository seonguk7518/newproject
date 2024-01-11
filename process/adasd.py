import pandas as pd
import numpy as np
import ccxt
import datetime
import time
import requests
from pytz import timezone

import pymysql

import warnings
import talib

warnings.filterwarnings("ignore")

binance = ccxt.binanceusdm()
coins_trd = binance.fetch_tickers()
host = "database-1-instance-1.ccjdvtwjk9lh.ap-northeast-2.rds.amazonaws.com"
port=3306
name="admin"
password="onebot7085!!"
db_name="data"

try:
    conn = pymysql.connect(host=host,port=port, user=name, passwd=password, db=db_name, connect_timeout=5)
except pymysql.MySQLError as e:
    print(e)

def get_10_days_ago_date(days=6) :
    base_line_date = (datetime.datetime.now() - datetime.timedelta(days=days)).strftime("%Y%m%d%H")
    return base_line_date

def get_previus_data(base_line_date, con) :
    sql = f"""select * from data.ticker_price_score where datetime >='{base_line_date}'"""
    previus_data = pd.read_sql(sql,con=con)
    return previus_data

def get_derivative_df(df) : 
    df = df.copy()
    df['score-derivative'] = (df['score'].diff(1))
    df['derivative'] = (df['open'].diff(1)/df['open'].shift(1)*100)
    df['score-signal'] = df['score-derivative']*df['derivative']
    # df[f'{ticker}-score-signal-2'] = df[f'{ticker}-score-signal'].apply(lambda x : 1 if x<-10 else 0)
    return df

def get_rsi(df,window=10) : 
    temp_df = df.copy()
    temp_df[f'derivative'] = (temp_df[f'open'].diff(1)/temp_df[f'open'].shift(1)*100)
    temp_df['U'] = np.where(temp_df['derivative'] > 0,temp_df['derivative'],0 ) 
    temp_df['D'] = np.where(temp_df['derivative'] < 0,-temp_df['derivative'],0 )
    AU = temp_df['U'].ewm(com=window-1,min_periods=window).mean()
    AD = temp_df['D'].abs().ewm(com=window-1,min_periods=window).mean()
    temp_df['AU'] = AU 
    temp_df['AD'] = AD
    RS = temp_df['AU']/temp_df['AD']
    RSI = RS/(1+RS)*100
    # df['RSI_distance'] = (df['RSI']-0.5).abs()
    return RSI

def get_volinger_band(df, window=48) : 
    df['ma'] = df[f'open'].rolling(window=window).mean()
    df['upper_volinger'] = df['ma'] + df[f'open'].rolling(window=window).std()*2
    df['lower_volinger'] = df['ma'] - df[f'open'].rolling(window=window).std()*2
    df['upper_volinger_distance'] = (df['upper_volinger'] - df[f'open'])/(df['upper_volinger'] - df['lower_volinger'])
    df['lower_volinger_distance'] = (df[f'open']-df['lower_volinger']) / (df['upper_volinger']-df['lower_volinger'])
    df['volinger_distance'] = (df['lower_volinger_distance'])
    return df

def get_momentum_df(df) : 
    df['momentum'] = df['RSI'] * df['volinger_distance']
    return df

def get_macd(df, fast_window=12,slow_window=26, signal_window=9) :
    df["MACD_short"]=df[f'open'].rolling(fast_window).mean()
    df["MACD_long"]=df[f'open'].rolling(slow_window).mean()
    df["MACD"]=df.apply(lambda x: (x["MACD_short"]-x["MACD_long"]), axis=1)
    df['MACD-index'] = np.abs(df['MACD_short'] / df['MACD_long']-1)
    df['MACD-cross'] = df['MACD']*df['MACD'].shift(1)
    df['MACD-cross-signal'] = df['MACD-cross'].apply(lambda x: 1 if x<0 else 0)
    return df

def get_obv(df,window=10,) : 
    obv = []
    obv.append(0)
    for i in range(1,len(df[f'open'])) :
        if df['open'].iloc[i] > df['open'].iloc[i-1] :
            obv.append(obv[-1]+df['volume'].iloc[i])
        elif df['open'].iloc[i] < df['open'].iloc[i-1] :
            obv.append(obv[-1]-df['volume'].iloc[i])
        else : 
            obv.append(obv[-1])
    df['obv'] = obv
    df['obv-ema'] = df['obv'].ewm(com=window).mean()
    df['obv-signal'] = df['obv'] - df['obv-ema']
    return df

def get_ema_df(df, window=12) : 
    df[f'ema-{window}'] = df[f'open'].ewm(com=window).mean()
    df[f'ema-distance-{window}'] = (df[f'open'] - df[f'ema-{window}'])/(df[f'open'])
    return df

def get_support_resistance_df(df,window) : 
    temp_df = df.copy()
    temp_df['max']=temp_df[f'high'].rolling(window=window).max()
    temp_df['min']=temp_df[f'low'].rolling(window=window).min()
    temp_df['distin']=temp_df['max']-temp_df['min']
    temp_df['open-low']=temp_df[f'open']-temp_df['min']
    df[f'support_resistance_index_{window}']=(temp_df['open-low']/temp_df['distin'])*100    
    return df

def get_stochastic(df, n=14,m=5,t=5):
    #n일중 최고가
    df["ndays_high"] =df[f'high'].rolling(window=n, min_periods=1).max()
    #n일중 최저가
    df["ndays_low"]= df[f'low'].rolling(window=n , min_periods=1).min() 
    # Fast%K 계산
    df["fast_k"] =((df[f'open']-df["ndays_low"])/(df["ndays_high"]-df["ndays_low"])) *100

    # Fast%D (=Slow%K) 계산

    df["slow_k"] = df["fast_k"].ewm(span=m).mean()
    # Slow%D 계산

    df["slow_d"] = df["slow_k"].ewm(span=t).mean()

    # slow%d와 slow%k의 교차점 구하기 위한 함수
    df['slow'] =df.apply(lambda x: (x["slow_k"]-x["slow_d"]),axis=1)
    df['slow-signal'] = df['slow'].rolling(n).mean()
    df["slow-signal-division"] = df['slow']/df['slow-signal']
    
    df["slow-cross"] = df['slow'] * df['slow'].shift(1)
    df['slow-cross-signal'] = df['slow-cross'].apply(lambda x:1 if x<0 else 0)

    return df

def get_rescaling_df(df,column,max,min) :
    column_key = column+"-scaler"
    df[column_key] = (df[column] - min)/(max-min)
    return df

def get_zscore_df(df,column, window=100) :
    column_key = column+"-zscore"
    df[column_key] = (df[column] - df[column].rolling(window).mean())/df[column].rolling(window).std()
    return df

def get_sar_df(df,acceleration=0.02, maximum=0.2) : 
    def get_sar_ls(sample_df) : 
        if (sample_df['SAR_cross_signal'] == 1 ) and (sample_df['SAR_signal'] == 1)  :
            return 1
        elif (sample_df['SAR_cross_signal'] == 1 ) and (sample_df['SAR_signal']== -1 ) : 
            return -1
        else : 
            return 0
        
    df['SAR'] = talib.SAR(df['high'], df['low'], acceleration=acceleration, maximum=maximum)
    df['SAR_signal']=np.where(df['real_open']>df['SAR'],1,-1)
    SAR_cross=df['SAR_signal'].shift(1)*df['SAR_signal']
    df['SAR_cross_signal']=np.where(SAR_cross<0,1,0)
    df['SAR_ls'] = df[['SAR_cross_signal','SAR_signal']].apply(lambda x:get_sar_ls(x),axis=1)
    return df

def insert_data_to_db(index_data_list, conn) :
    with conn.cursor() as cur : 
        for index_data in index_data_list : 
            ticker= index_data['ticker']
            datetime_str = index_data['datetime']
            value = index_data['value']
            parameter = index_data['parameter']
            values_String = f"'{datetime_str}','{ticker}',{value},'{parameter}'"
            sql = f"Insert into index_history (datetime,ticker,value,parameter) values ({values_String})"
            cur.execute(sql)
        conn.commit()
