from fastapi import APIRouter
from starlette.responses import JSONResponse
from models import *
from utils.balance import mt5fun
from utils.db_connect import MySQLAdapter
from utils.telegram_message import error_message


router= APIRouter()

@router.post('/mt5-balance', summary='Api MT5 ', tags=['MT5 USER'])
async def api_chance(user: UserMdel):

    mysql=MySQLAdapter()
    try:
        res=mt5fun()
        if res.read_data(user.id,user.pas,user.server,user.amount):
            mysql.save_user_log(UserMdel(id=user.id,pas=user.pas,account=user.account,accountpas=user.accountpas,

            server=user.server,
            retri=user.retri,
            amount=user.amount,
            deposit=user.deposit
          ))
            
        else:
            res.status_code=423

        

    except Exception as e:
        print(e)
        

    return JSONResponse(res.return_dict_data, status_code=res.status_code)



@router.get('/mt5-approve', summary='Api MT5 ', tags=['MT5 MANAGER '])
async def api_approve():

    mysql=MySQLAdapter()
    
    try:    
        if mysql.get_approve():
            mysql.status_code=200
        
        else:
            mysql.status_code=423

         
        
    except Exception as e:
        print(e)
        

    return JSONResponse(mysql.return_dict_data, status_code=mysql.status_code)

@router.post('/mt5-select', summary='Api MT5 ', tags=['MT5 MANAGER '])
async def api_select(appro:approvemodel):

    mysql=MySQLAdapter()
    
    try:    
        if appro.selet==1:
            
            if mysql.get_udate(appro.id):
                text=f"""[MT5 회원승인]\n"""
                text+=f"""회원아이디:{appro.id}"""
                await error_message(text)
            
        
        else:
            mysql.get_delet(appro.id)
           


    except Exception as e:
        print(e)
        

    return JSONResponse(mysql.return_dict_data, status_code=mysql.status_code)


@router.get('/mt5-memberlist', summary='Api MT5 ', tags=['MT5 MANAGER '])
async def api_select(selet:int):

    mysql=MySQLAdapter()
    
    try:    
        mysql.get_link_list(selet)


    except Exception as e:
        print(e)
        

    return JSONResponse(mysql.return_dict_data, status_code=mysql.status_code)