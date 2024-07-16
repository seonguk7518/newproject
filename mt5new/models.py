
from pydantic import BaseModel
from enum import Enum
from enum import IntEnum

class positionModel(IntEnum):
    all = 0
    buy = 1
    sell = 2
   
class interModel(IntEnum):
    defall = 0
    day = 1
    week = 2
    month=3

class strtusModel(IntEnum):
    defall = 0
    Waiting=1
    run = 2
    puase = 3
    
class checkModel(IntEnum):
    t = 0
    f=1
    d= 3
    


class UserMdel(BaseModel):
    fx_id:int
    fx_pas:str
    br_account:str
    br_accountpas:str
    server:str
    retri_id:str
    amount:float
    deposit:float
    
class approvemodel(BaseModel):
    id:int
    selet:int


class trademodel(BaseModel):
    fx_id:int
    from_data:str=''
    to_data:str=''
    select:int
    
class connetmodel(BaseModel):
    retri_id:str=''
    from_data:str=''
    to_data:str=''
    select:int
    trade:int
    
class managertrademodel(BaseModel):
    retri_id:str=''
    from_data:str=''
    to_data:str=''
    select:int
    trade:int=0    


    