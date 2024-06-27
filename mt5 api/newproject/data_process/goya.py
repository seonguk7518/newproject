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
        
        
try:
    engine = create_engine(
    "mysql+pymysql://staff_new:onebot9608!!@database-1-instance-1.ccjdvtwjk9lh.ap-northeast-2.rds.amazonaws.com/data",
        encoding="utf8",
    )
except pymysql.MySQLError as e:
    print(e)

limit=2
try:
    conn = engine.connect()
    sql = f"""
    SELECT * FROM onebot ORDER BY `datetime`  DESC LIMIT {limit};
    """
    with conn as con:
        onbotdata=pd.read_sql(sql,con )
except Exception as e :
    
    pass
new_list=[]
new_dict={}
date= (datetime.datetime.now(timezone('Asia/Seoul'))).strftime("%Y%m%d%H")
ombot_ch=round(float(onbotdata['onebot'].diff(-1)[0]),1)
new_dict['datetime']=date
new_dict['onebotdev']=ombot_ch
new_list.append(new_dict)


host = "database-1-instance-1.ccjdvtwjk9lh.ap-northeast-2.rds.amazonaws.com"
port=3306
name="staff_new"
password="onebot9608!!"
db_name="data"


try:
    conn1 = pymysql.connect(host=host,port=port, user=name, passwd=password, db=db_name, connect_timeout=5)
except pymysql.MySQLError as e:
    print(e)


def insert_data_to_db(price_data_list, conn) : 
    with conn.cursor() as cur :
        for price_data in price_data_list : 
            
            datetime_str = price_data['datetime']
            onbot = price_data['onebotdev']
            values_String = f"'{datetime_str}',{onbot}"
            sql = f"Insert into onebot_ch (datetime,onebotdev) values ({values_String})"
            cur.execute(sql)
    conn.commit()

insert_data_to_db(new_list,conn1)

