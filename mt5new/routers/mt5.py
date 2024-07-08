from fastapi import APIRouter
from starlette.responses import JSONResponse
from models import *
from utils.balance import mt5fun
from utils.db_connect import MySQLAdapter
from utils.telegram_message import error_message,error_message2


router= APIRouter()

@router.post('/mt5-user-balance', summary='Api MT5 ', tags=['MT5 USER'])
async def api_chance(user: UserMdel):
    
    """
    ## Request Body 정보
    ### if_id: MT5 로그인 아이디
    ### fx_pas: MT5 로그인 패스워드
    ### br_account : 브로커서버 계좌번호
    ### br_accountpas : 브로커서버 계좌 비밀번호
    ### retri_id: 리트리 id
    ### amount: 초기금액 ex) 10000,20000,30000
    ### deposit: 보증금 초기금액 10% 고정
    
    
    ## retCode 정보
    ### OK: 0
  
   
    """
    

    mysql=MySQLAdapter()
    try:
        res=mt5fun()
        if res.read_data(user.fx_id,user.fx_pas,user.server,user.amount):
            mysql.save_user_log(UserMdel(fx_id=user.fx_id,fx_pas=user.fx_pas,br_account=user.br_account,br_accountpas=user.br_accountpas,

            server=user.server,
            retri_id=user.retri_id,
            amount=user.amount,
            deposit=user.deposit
          ),res.balance)
            
        else:
            res.status_code=423

        

    except Exception as e:
        print(e)
        

    return JSONResponse(res.return_dict_data, status_code=res.status_code)


@router.post('/mt5-user-bot-management', summary='Api MT5 LINK MANAGEMENT', tags=['MT5 USER'])
async def uaaap(fx_id:int,select:int):
    
    """
    ## Request Body 정보
    ### fx_id : fx_id 회원 번호
    ### select: 2(봇연동),3(봇중지)
   
    
    
    ## retCode 정보
    ### OK: 0
  
   
    """

    mysql=MySQLAdapter()
    try:
       if mysql.get_user_stop(fx_id,select):
           
            if mysql.status==3:
            
                text=f"""[MT5 거래중지 요청]\n"""
                text+=f"""회원아이디:{fx_id}"""
                await error_message2(text)
                
            elif mysql.status==2:
                
                data=mysql.approve_user(fx_id)
                text=f"""[MT5 회원승인]\n"""
                text+=f"""회원아이디:{fx_id}\n회원패스워드:{data['mt_pas']}\n서버:{data['mt_sercer']}\n계좌번호:{data['account']}\n계좌비밀번호:{data['accountpas']}\n초기금액:{data['initial_amount']}\n보증금:{data['deposit']}\nmt밸런스:{data['money']}"""
                await error_message(text)
        

    except Exception as e:
        print(e)
        

    return JSONResponse(mysql.return_dict_data, status_code=mysql.status_code)



@router.get('/mt5-user-position-list', summary='Api USER MT5 POSITION ', tags=['MT5 USER'])
async def api_select(fx_id:int):

    
    """
    ## Request Body 정보
    ### fx_id : fx_id 회원 번호
   
   
    
    
    ## retCode 정보
    ### OK: 0
  
   
    """
    mysql=MySQLAdapter()
    
    try:    
        mysql.position_list(fx_id)
            


    except Exception as e:
        print(e)
        

    return JSONResponse(mysql.return_dict_data, status_code=mysql.status_code)

# @router.post('/mt5-user-trade-history', summary='Api MT5 TRADE HISTORY', tags=['MT5 USER'])
# async def trade_list(trade:trademodel):

#     mysql=MySQLAdapter()
#     try:
#        mysql.trade_list(trade.fx_id,trade.from_data,trade.to_data,trade.select)
           
            

        

#     except Exception as e:
#         print(e)
        

#     return JSONResponse(mysql.return_dict_data, status_code=mysql.status_code)

@router.get('/mt5-user-trade-history', summary='Api MT5 TRADE HISTORY', tags=['MT5 USER'])
async def trade_list(fx_id:str,select:interModel,from_data:str='',to_data:str=''):

    
    """
    ## Request Body 정보
    ### fx_id : fx_id 회원 번호
    ### select:기간선택 1(오늘),2(이번주),3(이번달)
    ### from_data : 시작날짜 ex) 2024-07-01
   ### to_data : 끝날짜 ex) 2024-07-08
    
    
    ## retCode 정보
    ### OK: 0
  
   
    """
    mysql=MySQLAdapter()
    try:
       mysql.trade_list(fx_id,from_data,to_data,select)
           
            

        

    except Exception as e:
        print(e)
        

    return JSONResponse(mysql.return_dict_data, status_code=mysql.status_code)



@router.get('/mt5-approve', summary='Api MT5 APPROVE ', tags=['MT5 MANAGER '], deprecated=True)
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



@router.post('/mt5-select', summary='Api MT5 SELECT ', tags=['MT5 MANAGER '], deprecated=True)
async def api_select(appro:approvemodel):

    mysql=MySQLAdapter()
    
    try:    
        if appro.selet==1:
            
            if mysql.get_udate(appro.id):
                data=mysql.approve_user(appro.id)
                text=f"""[MT5 회원승인]\n"""
                text+=f"""회원아이디:{appro.id}\n회원패스워드:{data['mt_pas']}\n서버:{data['mt_sercer']}\n계좌번호:{data['account']}\n계좌비밀번호:{data['accountpas']}\n초기금액:{data['initial_amount']}\n보증금:{data['deposit']}\nmt밸런스:{data['money']}"""
                await error_message(text)
            
        
        else:
            mysql.get_delet(appro.id)
           


    except Exception as e:
        print(e)
        

    return JSONResponse(mysql.return_dict_data, status_code=mysql.status_code)

@router.post('/mt5-manager-bot-management', summary='Api MT5 LINK MANAGEMENT', tags=['MT5 MANAGER '])
async def uaaap(retri_id:str,select:int):

    mysql=MySQLAdapter()
    try:
       if mysql.get_manager_stop(retri_id,select):
           
            if mysql.status==3:
                
                data=mysql.new_approve_user(retri_id)
                text=f"""[MT5 거래중지 요청]\n"""
                text+=f"""회원아이디:{data['mt_id']}"""
                await error_message2(text)
                
            elif mysql.status==2:
                
                data=mysql.new_approve_user(retri_id)
                text=f"""[MT5 회원승인]\n"""
                text+=f"""회원아이디:{data['mt_id']}\n회원패스워드:{data['mt_pas']}\n서버:{data['mt_sercer']}\n계좌번호:{data['account']}\n계좌비밀번호:{data['accountpas']}\n초기금액:{data['initial_amount']}\n보증금:{data['deposit']}\nmt밸런스:{data['money']}"""
                await error_message(text)
        

    except Exception as e:
        print(e)
        

    return JSONResponse(mysql.return_dict_data, status_code=mysql.status_code)


@router.get('/mt5-manager-memberlist', summary='Api MT5 MENBER ', tags=['MT5 MANAGER '])
async def api_select(select:int):

    mysql=MySQLAdapter()
    
    try:    
        mysql.get_link_list(select)


    except Exception as e:
        print(e)
        

    return JSONResponse(mysql.return_dict_data, status_code=mysql.status_code)


@router.get('/mt5-manager-position-list', summary='Api MT5 POSITION ', tags=['MT5 MANAGER '])
async def api_select(retri_id:str):

    mysql=MySQLAdapter()
    
    try:    
        mysql.manager_position_list(retri_id)
            


    except Exception as e:
        print(e)
        

    return JSONResponse(mysql.return_dict_data, status_code=mysql.status_code)

# @router.post('/mt5-manager-connet', summary='Api MT5 CONNET', tags=['MT5 MANAGER '])
# async def connet_list(trade:connetmodel):

#     mysql=MySQLAdapter()
#     try:
       
#        mysql.connet_list(trade.from_data, trade.to_data, trade.select, trade.retri_id,trade.trade)
           
            

        

#     except Exception as e:
#         print(e)
        

#     return JSONResponse(mysql.return_dict_data, status_code=mysql.status_code)

@router.get('/mt5-manager-connet', summary='Api MT5 CONNET', tags=['MT5 MANAGER '])
async def connet_list( select:interModel,trade:strtusModel,retri_id:str='',from_data:str='',to_data:str=''):

    mysql=MySQLAdapter()
    try:
       
       mysql.connet_list(from_data,to_data, select,retri_id,trade)
           
            

        

    except Exception as e:
        print(e)
        

    return JSONResponse(mysql.return_dict_data, status_code=mysql.status_code)


@router.get('/mt5-manager-connet-count', summary='Api MT5 CONNET COUNT ', tags=['MT5 MANAGER '])
async def api_connet_count():

    mysql=MySQLAdapter()
    
    try:    
        mysql.connet_count()


    except Exception as e:
        print(e)
        

    return JSONResponse(mysql.return_dict_data, status_code=mysql.status_code)


# @router.post('/mt5-manager-trade-history', summary='Api MT5 TRADE HISTORY', tags=['MT5 MANAGER '])
# async def trade_list(trade:managertrademodel):

#     mysql=MySQLAdapter()
#     try:
#        mysql.manager_trade_list(trade.retri_id,trade.from_data,trade.to_data,trade.select,trade.trade)
           
            

        

#     except Exception as e:
#         print(e)
        

#     return JSONResponse(mysql.return_dict_data, status_code=mysql.status_code)


@router.get('/mt5-manager-trade-history', summary='Api MT5 TRADE HISTORY', tags=['MT5 MANAGER '])
async def trade_list(retri_id:str,select:interModel,trade:positionModel,from_data:str='',to_data:str='',):

    mysql=MySQLAdapter()
    try:
       
        mysql.manager_trade_list(retri_id,from_data,to_data,select,trade)
           
            

        

    except Exception as e:
        print(e)
        

    return JSONResponse(mysql.return_dict_data, status_code=mysql.status_code)


@router.post('/mt5-manager-check-management', summary='Api MT5 CHECK MANAGEMENT', tags=['MT5 MANAGER '])
async def uaaap(retri_id:str):

    mysql=MySQLAdapter()
    try:
       mysql.check_list(retri_id)
           
           
                
             

    except Exception as e:
        print(e)
        

    return JSONResponse(mysql.return_dict_data, status_code=mysql.status_code)