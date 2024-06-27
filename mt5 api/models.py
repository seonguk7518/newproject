
from pydantic import BaseModel





class UserMdel(BaseModel):
    id:int
    pas:str
    account:str
    accountpas:str
    server:str
    retri:str
    amount:float
    deposit:float

class approvemodel(BaseModel):
    id:int
    selet:int
   