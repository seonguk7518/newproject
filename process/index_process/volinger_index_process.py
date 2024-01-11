import pandas as pd
import numpy as np
import pymysql
from sqlalchemy import create_engine, Table, Column, Integer, String, Float, BigInteger
import warnings
warnings.filterwarnings("ignore")


host = "database-1-instance-1.ccjdvtwjk9lh.ap-northeast-2.rds.amazonaws.com"
port=3306

db = pymysql.connect(
    host=host,
    port=port,
    user="admin",
    passwd="onebot7085!!",
    db='data',
    charset="utf8"
)

engine = create_engine(f"mysql+mysqldb://admin:onebot7085!!@{host}:{port}/data",echo=True)

def get_ticker_list(connect) :
    sql = "select distinct ticker from data.ticker_price"
    data = pd.read_sql(sql, con=connect)['ticker']
    return data

def get_ticker_data(ticker, connect) : 
    sql = f"select * from data.ticker_price where ticker='{ticker}'"
    data=  pd.read_sql(sql, con=connect)
    data.drop_duplicates(subset=['datetime','ticker'],inplace=True)
    return data

def get_derivative(df) : 
    df['derivative'] = df['open'].diff(1)/df['open'].shift(1)*100
    return df

def get_rsi(df,window=10,column = 'derivative') : 
    df['U'] = np.where(df[column] > 0,df[column],0 ) 
    df['D'] = np.where(df[column] < 0,-df[column],0 ) 
    AU = df['U'].ewm(com=window-1,min_periods=window).mean()
    AD = df['D'].abs().ewm(com=window-1,min_periods=window).mean()
    df['AU'] = AU 
    df['AD'] = AD
    RS = df['AU']/df['AD']
    df['rsi'] = RS/(1+RS)*100
    # df['RSI_distance'] = (df['RSI']-0.5).abs()
    return df

def get_volinger_band(df, window=48) : 
    df['ma'] = df[f'open'].rolling(window=window).mean()
    df['upper_volinger'] = df['ma'] + df[f'open'].rolling(window=window).std()*2
    df['lower_volinger'] = df['ma'] - df[f'open'].rolling(window=window).std()*2
    df['upper_volinger_distance'] = (df['upper_volinger'] - df[f'open'])/(df['upper_volinger'] - df['lower_volinger'])
    df['lower_volinger_distance'] = (df[f'open']-df['lower_volinger']) / (df['upper_volinger']-df['lower_volinger'])
    df['volinger_distance'] = (df['lower_volinger_distance'])
    return df

if __name__ == "__main__" :
    import argparse
    parser = argparse.ArgumentParser(description='Argparse Tutorial')
    parser.add_argument('--window', type=int,   default=48)
    args = parser.parse_args()
    window = args.window
    
    ticker_list = get_ticker_list(db)

    index_list = [('volinger_distance','volinger-window-48'),('rsi','rsi-window-10')]
    
    for ticker in ticker_list : 
        ticker_data = get_ticker_data(ticker,db)
        ticker_data = get_derivative(ticker_data)
        ticker_data = get_volinger_band(ticker_data,window)
        insert_data = ticker_data[['datetime','ticker']]
        insert_data['value'] = ticker_data['volinger_distance']
        insert_data['parameter'] = f'volinger-window-{window}'
        insert_data.dropna(inplace=True)
        insert_data.to_sql("index_history",con=engine,if_exists='append',index=False)