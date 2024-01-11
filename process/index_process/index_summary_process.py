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

def get_index_list(connect) : 
    sql = "select distinct parameter from data.index_history"
    data = pd.read_sql(sql, con=connect)['parameter']
    return data

index_list = get_index_list(db)
for index in index_list :
    sql = f"select * from data.index_history where parameter='{index}'" 
    data = pd.read_sql(sql, db)
    
    break