from fastapi import FastAPI
import pandas as pd
import pymysql
import uvicorn
from sqlalchemy import create_engine
from enum import Enum

app = FastAPI()

class IntervalModel(str, Enum):
    n25='d1'
    n50 = 'd2'
    n75='d3'
    n100 ='d4' 
    n125 = 'd5'
    n150 = 'd6'
    n200= 'w1'
    n400 = 'w2'
    n600= 'w3'
    n4 = 'h4'
    n8 = 'h8'
    n12 = 'h12'
    n16 = 'h16'
    n20 = 'h20'




@app.get("/리트리 선물차트")

async def read_data(ticker:str,limit:IntervalModel):
    
    limit=limit
    limit=IntervalModel(limit).name.split("n")[1]
    
    if ticker.endswith("USDT"):
   
        ticker=ticker.split('USDT')[0]
    else:
        ticker=ticker

    
    try:
        engine = create_engine("mysql+pymysql://datateam:datateam1234$$@52.79.226.104/chart_data",encoding="utf8")
        conn = engine.connect()
        sql = f"""
        SELECT * FROM retri where ticker ='{ticker}' ORDER BY `datetime`  DESC LIMIT {limit};
        """
        with conn as con:

            new_signal_data=pd.read_sql(sql,con )
            if len(new_signal_data)>0:
                new_signal_data.sort_values('datetime',inplace=True)
                new_signal_data['midle']=new_signal_data['midle'].shift(-1)
                new_signal_data['profit1']=new_signal_data['profit1'].shift(-1)
                new_signal_data['profit2']=new_signal_data['profit2'].shift(-1)
                new_signal_data['profit3']=new_signal_data['profit3'].shift(-1)
                new_signal_data['recom_price']=new_signal_data['recom_price'].shift(-1)
   
                new_signal_data=new_signal_data[(new_signal_data['new_signal']=='L')|(new_signal_data['new_signal']=='S')]

        new_signal_data['midle']=new_signal_data['midle'].apply(float)
        new_signal_data['profit1']=new_signal_data['profit1'].apply(float)
        new_signal_data['profit2']=new_signal_data['profit2'].apply(float)
        new_signal_data['profit3']=new_signal_data['profit3'].apply(float)

        data_list=[]
        for i in new_signal_data.iterrows():
            data=i[1]
            new_dict={}
            new_dict['datetime']=data['datetime']
            new_dict['ticker']=data['ticker']
            new_dict['진입가']=round(data['midle'],6)
            new_dict['1%익절가']=round(data['profit1'],6)
            new_dict['2%익절가']=round(data['profit2'],6)
            new_dict['3%익절가']=round(data['profit3'],6)
            new_dict['추천가']=data['recom_price']
            new_dict['signal']=data['new_signal']
            data_list.append(new_dict)
    except Exception as e :
        
        pass
    return  data_list


if __name__ == "__main__":
    uvicorn.run(app="recomprice_api:app", host="0.0.0.0", port=8000,reload=True)