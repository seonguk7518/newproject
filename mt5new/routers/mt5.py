from fastapi import APIRouter
from starlette.responses import JSONResponse
from models import *
from utils.balance import mt5fun
from utils.db_connect import MySQLAdapter
from utils.telegram_message import error_message,error_message2
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import mysql.connector
# import logging
from decimal import Decimal
from datetime import datetime
import MetaTrader5 as mt5
import boto3
from fastapi import APIRouter, HTTPException,Query
from sqlalchemy import select,func
from sqlalchemy.orm import Session
from config import engine,monthly_money_table, main_table, balance_table, daily_money_table
import pandas as pd
from pydantic import BaseModel
from datetime import datetime, timedelta
from utils.make_error import MakeErrorType

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
async def uaaap(fx_id:int,select:int,retri_id:str):
    
    """
    ## Request Body 정보
    ### fx_id : fx_id 회원 번호
    ### select: 2(봇연동),3(봇중지)
   
    
    
    ## retCode 정보
    ### OK: 0
  
   
    """

    mysql=MySQLAdapter()
    try:
        
        if mysql.new_find_user(retri_id)==False:
            
            if mysql.get_user_stop(fx_id,select):
                
                    if mysql.status==3:
                    
                        text=f"""[MT5 거래중지 요청]\n"""
                        text+=f"""회원아이디:{fx_id}"""
                        await error_message2(text)
                        
                        mysql.save_bot_log(botmanagemodel(fx_id=fx_id,retri_id=retri_id,status=3,check_status=1))
                    elif mysql.status==2:
                        
                        data=mysql.approve_user(fx_id)
                        text=f"""[MT5 회원승인]\n"""
                        text+=f"""회원아이디:{fx_id}\n회원패스워드:{data['mt_pas']}\n서버:{data['mt_sercer']}\n계좌번호:{data['account']}\n계좌비밀번호:{data['accountpas']}\n초기금액:{data['initial_amount']}\n보증금:{data['deposit']}\nmt밸런스:{data['money']}"""
                        await error_message(text)
                        mysql.save_bot_log(botmanagemodel(fx_id=fx_id,retri_id=retri_id,status=2,check_status=1))
        else:
            
            check = MakeErrorType()
            mysql.return_dict_data['reCode']=100
            mysql.return_dict_data['message'] = check.error(mysql.return_dict_data['reCode'])
            mysql.status_code=423
            

    except Exception as e:
        print(e)
        

    return JSONResponse(mysql.return_dict_data, status_code=mysql.status_code)


@router.get('/mt5-user-botmanage-list', summary='Api MT5 BOTMANAGE LIST ', tags=['MT5 USER'])
async def api_select(fx_id:str,from_data:str,to_data:str,page:int=1,size:int=5):

    mysql=MySQLAdapter()
    
    try:    
        mysql.user_botmanage_list(fx_id,from_data,to_data,page,size)
            


    except Exception as e:
        print(e)
        

    return JSONResponse(mysql.return_dict_data, status_code=mysql.status_code)



@router.get('/mt5-user-dashboard', summary='Api USER MT5 DASHBOARD ', tags=['MT5 USER'])
async def api_select(fx_id:int):

    
    """
    ## Request Body 정보
    ### fx_id : fx_id 회원 번호
   
   
    
    
    ## retCode 정보
    ### OK: 0
  
   
    """
    mysql=MySQLAdapter()
    
    try:    
        mysql.dashboard_list(fx_id)
            


    except Exception as e:
        print(e)
        

    return JSONResponse(mysql.return_dict_data, status_code=mysql.status_code)


@router.get('/mt5-user-position-list', summary='Api USER MT5 POSITION ', tags=['MT5 USER'])
async def api_select(fx_id:int,position_id:str=''):

    
    """
    ## Request Body 정보
    ### fx_id : fx_id 회원 번호
   
   
    
    
    ## retCode 정보
    ### OK: 0
  
   
    """
    mysql=MySQLAdapter()
    
    try:    
        mysql.position_list(fx_id,position_id)
            


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
async def trade_list(fx_id:str,select:interModel,from_data:str='',to_data:str='',page:int=1,size:int=5):

    
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
       mysql.trade_list(fx_id,from_data,to_data,select,page,size)
           
            

        

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
                print(data)
                text=f"""[MT5 회원승인]\n"""
                text+=f"""회원아이디:{appro.id}\n회원패스워드:{data['mt_pas']}\n서버:{data['mt_sercer']}\n계좌번호:{data['account']}\n계좌비밀번호:{data['accountpas']}\n초기금액:{data['initial_amount']}\n보증금:{data['deposit']}\nmt밸런스:{data['money']}"""
                await error_message(text)
                mysql.save_bot_log(botmanagemodel(fx_id=appro.id,retri_id=data['retri_id'],status=2,check_status=2))
        
        else:
            mysql.get_delet(appro.id)
           


    except Exception as e:
        print(e)
        

    return JSONResponse(mysql.return_dict_data, status_code=mysql.status_code)

@router.post('/mt5-manager-bot-management', summary='Api MT5 LINK MANAGEMENT', tags=['MT5 MANAGER '])
async def uaaap(fx_id:int,retri_id:str,select:int,manage_id:str):

    mysql=MySQLAdapter()
    try:
        if mysql.find_user(fx_id)==False:
            
            if mysql.get_manager_stop(retri_id,select):
                
                    if mysql.status==3:
                        
                        data=mysql.new_approve_user(retri_id)
                        text=f"""[MT5 거래중지 요청]\n"""
                        text+=f"""회원아이디:{data['mt_id']}"""
                        await error_message2(text)
                        mysql.save_bot_log(botmanagemodel(fx_id=fx_id,retri_id=retri_id,manage_id=manage_id,status=3,check_status=2))
                    elif mysql.status==2:
                        
                        data=mysql.new_approve_user(retri_id)
                        text=f"""[MT5 회원승인]\n"""
                        text+=f"""회원아이디:{data['mt_id']}\n회원패스워드:{data['mt_pas']}\n서버:{data['mt_sercer']}\n계좌번호:{data['account']}\n계좌비밀번호:{data['accountpas']}\n초기금액:{data['initial_amount']}\n보증금:{data['deposit']}\nmt밸런스:{data['money']}"""
                        await error_message(text)
                        mysql.save_bot_log(botmanagemodel(fx_id=fx_id,retri_id=retri_id,manage_id=manage_id,status=2,check_status=2))
        else:
            check = MakeErrorType()
            mysql.return_dict_data['reCode']=100
            mysql.return_dict_data['message'] = check.error(mysql.return_dict_data['reCode'])
            mysql.status_code=423
            
            
            
            
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
async def api_select(retri_id:str,page:int=1,size:int=5):

    mysql=MySQLAdapter()
    
    try:    
        mysql.manager_position_list(retri_id,page,size)
            


    except Exception as e:
        print(e)
        

    return JSONResponse(mysql.return_dict_data, status_code=mysql.status_code)


@router.get('/mt5-manager-position-count', summary='Api MT5 POSITON COUNT', tags=['MT5 MANAGER '])
async def trade_count_list():

    mysql=MySQLAdapter()
    try:
       
        mysql.get_position_count()
           
            

        

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
async def connet_list( select:interModel,trade:strtusModel,check_status:checkModel,retri_id:str='',from_data:str='',to_data:str='',page:int=1,size:int=5):

    mysql=MySQLAdapter()
    try:
       
       mysql.connet_list(from_data,to_data, select,retri_id,trade,page,size,check_status)
           
            

        

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
async def trade_list(retri_id:str,select:interModel,trade:positionModel,from_data:str='',to_data:str='',page:int=1,size:int=5):

    mysql=MySQLAdapter()
    try:
       
        mysql.manager_trade_list(retri_id,from_data,to_data,select,trade,page,size)
           
            

        

    except Exception as e:
        print(e)
        

    return JSONResponse(mysql.return_dict_data, status_code=mysql.status_code)


@router.get('/mt5-manager-trade-count', summary='Api MT5 TRADE COUNT', tags=['MT5 MANAGER '])
async def trade_count_list():

    mysql=MySQLAdapter()
    try:
       
        mysql.get_trade_count()
           
            

        

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


@router.get('/mt5-manager-botmanage-list', summary='Api MT5 BOTMANAGE ', tags=['MT5 MANAGER '])
async def api_select(retri_id:str,from_data:str,to_data:str,select:int,page:int=1,size:int=5):

    mysql=MySQLAdapter()
    
    try:    
        mysql.manager_botmanage_list(retri_id,from_data,to_data,select,page,size)
            


    except Exception as e:
        print(e)
        

    return JSONResponse(mysql.return_dict_data, status_code=mysql.status_code)




@router.get('/mt5-manager-maindashboard', summary='Api MT5 MAINDASHBOARD ', tags=['MT5 MANAGER '])
async def api_select():

    mysql=MySQLAdapter()
    
    try:    
        mysql.manage_maindash()
            


    except Exception as e:
        print(e)
        

    return JSONResponse(mysql.return_dict_data, status_code=mysql.status_code)

#--------------------------------------------------------------------------------------

DB_HOST = 'ec2-3-36-61-114.ap-northeast-2.compute.amazonaws.com'
DB_NAME = 'goya_fx'
DB_USER = 'fxuser'
DB_PASSWORD = 'As!135790'

AWS_KMS_KEY_ID = '068e6299-cddb-43e6-9e90-22c37f4a4958'
KMS_REGION = 'ap-northeast-2'
KMS_CLIENT = boto3.client('kms', region_name=KMS_REGION)  # 지역 지정

def decrypt_data(encrypted_data: bytes) -> str:
    response = KMS_CLIENT.decrypt(KeyId=AWS_KMS_KEY_ID, CiphertextBlob=encrypted_data)
    decrypted_data = response["Plaintext"].decode()
    return decrypted_data

# 데이터베이스 연결 함수
def get_db_connection():
    try:
        conn = mysql.connector.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=3306  # MySQL 포트
        )
        return conn
    except mysql.connector.Error as err:
        # logging.error(f'Error connecting to database: {err}')
        return None

# Pydantic 모델
class BalanceCheckRequest(BaseModel):
    fx_id: int
    retri_id: str
    initial_amount: float
    deposit: float
    datetime: datetime

class MonthlySummaryRequest(BaseModel):
    retri_id: str

class MonthlySummaryResponse(BaseModel):
    retri_id: str
    monthly_amount: float
    profit: float
    datetime: datetime

@router.post('/mt5-retri-monthly_summary', summary='Api MT5 MONTHLY_SUMMARY', tags=['MT5 RETRI '])
async def get_monthly_summary(request: MonthlySummaryRequest):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")

    cur = conn.cursor()
    return_dict_data=dict(results=[], reCode=1, message='Server Error')
    # retri_id로 MonthlyMoney 테이블에서 조회
    cur.execute("SELECT monthly_amount, profit, datetime FROM MonthlyMoney WHERE retri_id = %s", (request.retri_id,))
    row = cur.fetchone()
    print(row)
    if not row:
        status_code=423
        return_dict_data['reCode']=1
        return_dict_data['message']='not connet'
        cur.close()
        conn.close()
        return JSONResponse(return_dict_data, status_code=status_code) # retri_id가 없을 경우 1 반환

    monthly_amount = float(row[0])
    profit = float(row[1])
    datetime_value = row[2]

    cur.close()
    conn.close()
    new_dict={}
    new_dict['retri_id']=request.retri_id
    new_dict['monthly_amount']=monthly_amount
    new_dict['profit']=profit
    new_dict['datetime']=str(datetime_value)
    return_dict_data['results'].append(new_dict)
    return_dict_data['reCode']=0
    return_dict_data['message']='OK'

    status_code=200
    
    return JSONResponse(return_dict_data, status_code=status_code)


@router.post('/mt5-retri-check_balance', summary='Api MT5 CHECK BALANCE', tags=['MT5 RETRI '])
async def check_balance(request: BalanceCheckRequest):
    # MetaTrader5 초기화
    return_dict_data=dict(results=[], reCode=1, message='Server Error')
    def initialize_mt5(account, password, server):
        if not mt5.initialize(login=account, password=password, server=server):
            # logging.error(f"MetaTrader5 initialize() failed for account {account}")
            return False
        return True

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")

    cur = conn.cursor()

    # 필요한 계좌 정보 가져오기
    cur.execute("SELECT PASSWORD, server FROM Main WHERE fx_id = %s", (request.fx_id,))
    row = cur.fetchone()
    if not row:
        return_dict_data['reCode']=1
        return_dict_data['message']='error'
        status_code=423
        
        
        return JSONResponse(return_dict_data, status_code=status_code)

    encrypted_password = row[0]
    server = row[1]

    password = decrypt_data(encrypted_password)  # 비밀번호 복호화

    if initialize_mt5(request.fx_id, password, server):
        account_info = mt5.account_info()
        if account_info is None:
            # logging.error(f"Failed to get account info for fx_id {request.fx_id}, error code = {mt5.last_error()}")
            mt5.shutdown()
            return 1  # 실패하면 1 반환

        mt5_balance = Decimal(account_info.balance)
        mt5.shutdown()

        if mt5_balance >= Decimal(request.initial_amount):
            # balance 테이블에 데이터 삽입
            cur.execute("""
                INSERT INTO balance (fx_id, retri_id, initial_amount, mt5_balance, deposit, datetime) 
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (request.fx_id, request.retri_id, Decimal(request.initial_amount), mt5_balance, Decimal(request.deposit), request.datetime))
            conn.commit()
            cur.close()
            conn.close()
            new_dict={}
            
            new_dict['mt5_balance']=float(mt5_balance)
            return_dict_data['results'].append(new_dict)
            return_dict_data['reCode']=0
            return_dict_data['message']='OK'
            status_code=200
            return JSONResponse(return_dict_data, status_code=status_code)
        else:
            cur.close()
            conn.close()
            return_dict_data['reCode']=1
            return_dict_data['message']='error'
            status_code=423
            return JSONResponse(return_dict_data, status_code=status_code)  # mt5_balance가 initial_amount보다 작으면 1 반환
    else:
        return_dict_data['reCode']=1
        return_dict_data['message']='error'
        status_code=423
        return JSONResponse(return_dict_data, status_code=status_code)  # MetaTrader5 초기화 실패하면 1 반환



@router.get('/mt5-user-deposit', summary='Api MT5 DEPOSIT', tags=['MT5 USER'])
async def get_deposit_info(
    fx_id: int,
    page: int = Query(1, ge=1),
    size: int = Query(10, ge=1)
):  # fx_id 타입을 int로 변경, 페이지와 사이즈 파라미터 추가
    return_dict_data = {"results": [], "reCode": 1, "message": "Server Error"}
    session = Session(engine)
    
    try:
        # 현재 시간
        now = datetime.now()

        # Main 테이블에서 초기 보증금 가져오기
        main_query = select([main_table.c.deposit, main_table.c.datetime, main_table.c.check]).where(main_table.c.fx_id == fx_id)
        main_result = session.execute(main_query).fetchone()
        if main_result:
            initial_deposit = main_result.deposit
            initial_datetime = main_result.datetime
            initial_check = main_result.check
        else:
            return_dict_data['reCode'] = 2
            return_dict_data['message'] = 'FX ID is incorrect'
            return JSONResponse(return_dict_data, status_code=423)

        # MonthlyMoney 테이블에서 데이터 가져오기
        monthly_money_query = select([monthly_money_table.c.deposit, monthly_money_table.c.datetime]).where(monthly_money_table.c.fx_id == fx_id)
        monthly_money_result = session.execute(monthly_money_query).fetchone()
        
        if monthly_money_result:
            monthly_deposit = monthly_money_result.deposit
            monthly_datetime = monthly_money_result.datetime
        else:
            monthly_deposit = initial_deposit
            monthly_datetime = initial_datetime

        # Balance 테이블에서 추가 보증금 가져오기
        offset = (page - 1) * size
        balance_query = select([balance_table.c.deposit, balance_table.c.datetime, balance_table.c.check]).where(
            (balance_table.c.fx_id == fx_id) & (balance_table.c.datetime > monthly_datetime)
        ).order_by(balance_table.c.datetime).limit(size).offset(offset)
        balance_results = session.execute(balance_query).fetchall()

        # Balance 테이블에서 전체 카운트 가져오기
        total_count_query = select([func.count()]).where(
            (balance_table.c.fx_id == fx_id) & (balance_table.c.datetime > monthly_datetime)
        )
        total_count = session.execute(total_count_query).scalar()

        # 트랜잭션 정보 생성
        transactions = [
            {"type": "initial", "datetime": initial_datetime.isoformat(), "amount": float(initial_deposit), "difference": 0, "check": initial_check},
            {"type": "monthly", "datetime": monthly_datetime.isoformat(), "amount": float(monthly_deposit), "difference": float(monthly_deposit - initial_deposit), "check": initial_check}
        ]

        previous_deposit = monthly_deposit
        previous_check = initial_check

        for balance in balance_results:
            additional_deposit = balance.deposit
            additional_datetime = balance.datetime
            additional_check = balance.check
            
            # Check 값이 변경되면 새로운 초기 보증금으로 간주하되 difference는 이전 초기 보증금과의 차이로 설정
            if additional_check != previous_check:
                difference = additional_deposit - previous_deposit
                initial_deposit = additional_deposit
                initial_datetime = additional_datetime
            else:
                difference = additional_deposit - previous_deposit  # 이전 보증금과의 차이 계산

            transactions.append({"type": "balance", "datetime": additional_datetime.isoformat(), "amount": float(additional_deposit), "difference": float(difference), "check": additional_check})
            previous_deposit = additional_deposit
            previous_check = additional_check

        # 마지막 보증금을 기준으로 현재 보증금 설정
        current_deposit = previous_deposit

        # 필요한 보증금 계산
        required_deposit = current_deposit - initial_deposit

        # 총 페이지 수 계산
        total_pages = (total_count + size - 1) // size

        return_dict_data = {
            "results": [{
                "current_deposit": float(current_deposit),
                "initial_deposit": float(initial_deposit),
                "required_deposit": float(required_deposit),
                "transactions": transactions,
                "timestamp": now.isoformat()
            }],
            "total_pages": total_pages,
            "total_count": total_count,
            "reCode": 0,
            "message": "Success"
        }
        return JSONResponse(return_dict_data, status_code=200)

    except Exception as e:
        return_dict_data['message'] = str(e)
        return JSONResponse(return_dict_data, status_code=500)
    
    finally:
        session.close()

        
class CalendarRequest(BaseModel):
    retri_id: str
    year: int
    month: int
    
    
@router.get('/mt5-user-calendar', summary='Api MT5 CALENDAR', tags=['MT5 USER'])
async def get_calendar(fx_id: int, year: int, month: int):  # fx_id 타입을 int로 변경
    return_dict_data = {"results": [], "reCode": 1, "message": "Server Error"}
    session = Session(engine)

    try:
        # 주어진 연도와 월에 해당하는 시작 날짜와 끝 날짜 계산
        start_date = datetime(year, month, 1)
        end_date = (start_date + timedelta(days=32)).replace(day=1) - timedelta(days=1)

        # DailyMoney 테이블에서 데이터 가져오기
        query = select([
            daily_money_table.c.datetime,
            daily_money_table.c.daily_amount
        ]).where(
            (daily_money_table.c.fx_id == fx_id) &
            (daily_money_table.c.datetime.between(start_date, end_date))
        )
        results = session.execute(query).fetchall()

        if not results:
            return_dict_data['reCode'] = 2
            return_dict_data['message'] = 'No data found'
            return JSONResponse(return_dict_data, status_code=423)

        # 데이터프레임 생성
        df = pd.DataFrame(results, columns=['datetime', 'daily_amount'])
        df['datetime'] = pd.to_datetime(df['datetime'])
        df.set_index('datetime', inplace=True)

        # 일별 수익금 계산
        daily_amount = df['daily_amount'].resample('D').sum()

        # 결과 날짜를 하루 전으로 조정
        daily_amount.index = daily_amount.index - pd.Timedelta(days=1)

        # 인덱스를 문자열로 변환
        daily_amount.index = daily_amount.index.strftime('%Y-%m-%d')

        # 결과를 JSON 형식으로 반환
        result = daily_amount.to_dict()

        return_dict_data = {
            "results": result,
            "reCode": 0,
            "message": "Success"
        }
        return JSONResponse(return_dict_data, status_code=200)

    except Exception as e:
        return_dict_data['message'] = str(e)
        return JSONResponse(return_dict_data, status_code=500)
    
    finally:
        session.close()



class PnLRequest(BaseModel):
    retri_id: str
    start_date: datetime
    end_date: datetime

@router.get('/mt5-user-pnl', summary='Api MT5 PNL', tags=['MT5 USER'])
async def get_pnl(fx_id: int, year_month: str):  # year_month를 str로 변경
    return_dict_data = {"results": [], "reCode": 1, "message": "Server Error"}
    session = Session(engine)

    try:
        # year_month 문자열을 datetime 객체로 변환
        start_date = datetime.strptime(year_month, "%Y%m")
        end_date = (start_date + timedelta(days=32)).replace(day=1) - timedelta(days=1)

        # DailyMoney 테이블에서 데이터 가져오기
        query = select([
            daily_money_table.c.datetime,
            daily_money_table.c.daily_amount,
            daily_money_table.c.profit,
            daily_money_table.c.month,
            daily_money_table.c.week
        ]).where(
            (daily_money_table.c.fx_id == fx_id) &
            (daily_money_table.c.datetime.between(start_date, end_date))
        )
        results = session.execute(query).fetchall()

        if not results:
            return_dict_data['reCode'] = 2
            return_dict_data['message'] = 'No data found'
            return JSONResponse(return_dict_data, status_code=423)

        # 데이터프레임 생성
        df = pd.DataFrame(results, columns=['datetime', 'daily_amount', 'profit', 'month', 'week'])
        df['datetime'] = pd.to_datetime(df['datetime'])
        df.set_index('datetime', inplace=True)

        # 일간 수익률 계산
        daily_pnl = df['profit'].resample('D').sum()
        daily_pnl.index = daily_pnl.index.strftime('%Y%m%d')  # 인덱스를 문자열로 변환

        # 주간 수익률 계산
        weekly_pnl = df['profit'].resample('W').sum()
        weekly_pnl.index = weekly_pnl.index.strftime('%Y%m%d')  # 인덱스를 문자열로 변환

        # 월간 수익률 계산
        monthly_pnl = df['profit'].resample('M').sum()
        monthly_pnl.index = monthly_pnl.index.strftime('%Y%m%d')  # 인덱스를 문자열로 변환

        # 마지막 값을 추출
        last_day_pnl = daily_pnl.iloc[-1] if not daily_pnl.empty else 0
        last_week_pnl = weekly_pnl.iloc[-1] if not weekly_pnl.empty else 0
        last_month_pnl = monthly_pnl.iloc[-1] if not monthly_pnl.empty else 0

        # 월간 총 수익 계산
        monthly_total = df['daily_amount'].sum()

        # 현재 날짜와 같은 달의 check 값 가져오기
        check = None

        # 먼저 Balance 테이블에서 check 값을 가져옴
        balance_query = select([balance_table.c.check]).where(
            (balance_table.c.fx_id == fx_id) & (balance_table.c.datetime.between(start_date, end_date))
        )
        balance_result = session.execute(balance_query).fetchone()
        if balance_result:
            check = balance_result.check
        else:
            # Balance 테이블에 값이 없으면 Main 테이블에서 check 값을 가져옴
            main_query = select([main_table.c.check]).where(
                (main_table.c.fx_id == fx_id) & (main_table.c.datetime.between(start_date, end_date))
            )
            main_result = session.execute(main_query).fetchone()
            if main_result:
                check = main_result.check

        # 결과를 JSON 형식으로 반환
        result = {
            "day": last_day_pnl,
            "week": last_week_pnl,
            "month": last_month_pnl,
            "monthly_total": monthly_total,
            "check": check
        }

        return_dict_data = {
            "results": result,
            "reCode": 0,
            "message": "Success"
        }
        return JSONResponse(return_dict_data, status_code=200)

    except Exception as e:
        return_dict_data['message'] = str(e)
        return JSONResponse(return_dict_data, status_code=500)
    
    finally:
        session.close()