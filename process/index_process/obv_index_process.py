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

if __name__ == "__main__" :
    import argparse
    parser = argparse.ArgumentParser(description='Argparse Tutorial')
    parser.add_argument('--window', type=int,   default=10)
    args = parser.parse_args()
    window = args.window
    
    ticker_list = get_ticker_list(db)

    index_list = [('obv',f'obv-window-{window}'),('obv-signal',f'obv-signal-window-{window}')]
    
    for ticker in ticker_list : 
        ticker_data = get_ticker_data(ticker,db)
        ticker_data = get_obv(ticker_data,window=window)
        for index in index_list : 
            insert_data = ticker_data[['datetime','ticker']]
            insert_data['value'] = ticker_data[index[0]]
            insert_data['parameter'] = index[1]
            insert_data.dropna(inplace=True)
            insert_data.to_sql("index_history",con=engine,if_exists='append',index=False)