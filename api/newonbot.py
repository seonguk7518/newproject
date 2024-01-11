from fastapi import FastAPI
import pandas as pd
import pymysql
import uvicorn
from sqlalchemy import create_engine
app = FastAPI()

@app.get("/onebot")
async def read_data(limit:int,b:int):
    engine = create_engine(
    "mysql+pymysql://staff_new:onebot9608!!@database-1-instance-1.ccjdvtwjk9lh.ap-northeast-2.rds.amazonaws.com/data",
        encoding="utf8",
    )
    try:
        conn = engine.connect()
        sql = f"""
        SELECT * FROM onebot ORDER BY `datetime`  DESC LIMIT {limit};
        """
        with conn as con:
            onbotdata=pd.read_sql(sql,con )
        
        
        
        data_list=[]
        for i in onbotdata.iterrows():
            data=i[1]
            new_dict={}
            new_dict['datetime']=data['datetime']
            new_dict['onebot ']=data['onebot']
            data_list.append(new_dict)
    except Exception as e :
        
        pass
    return  data_list


if __name__ == "__main__":
    uvicorn.run(app="api:app", host="0.0.0.0", port=8000,reload=True)