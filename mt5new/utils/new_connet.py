from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import mysql.connector
# import logging
from decimal import Decimal
from datetime import datetime
import MetaTrader5 as mt5
import boto3

app = FastAPI()

# 로깅 설정
# logging.basicConfig(filename='fastapi_log.txt', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# MySQL 데이터베이스 설정
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

@app.post("/check_balance")
def check_balance(request: BalanceCheckRequest):
    # MetaTrader5 초기화
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
        raise HTTPException(status_code=404, detail="Account not found")

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
            return {"status": "success", "mt5_balance": mt5_balance}
        else:
            cur.close()
            conn.close()
            return 1  # mt5_balance가 initial_amount보다 작으면 1 반환
    else:
        return 1  # MetaTrader5 초기화 실패하면 1 반환

@app.post("/monthly_summary", response_model=MonthlySummaryResponse)
def get_monthly_summary(request: MonthlySummaryRequest):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")

    cur = conn.cursor()

    # retri_id로 MonthlyMoney 테이블에서 조회
    cur.execute("SELECT monthly_amount, profit, datetime FROM MonthlyMoney WHERE retri_id = %s", (request.retri_id,))
    row = cur.fetchone()

    if not row:
        cur.close()
        conn.close()
        return JSONResponse(content=1)  # retri_id가 없을 경우 1 반환

    monthly_amount = float(row[0])
    profit = float(row[1])
    datetime_value = row[2]

    cur.close()
    conn.close()

    return MonthlySummaryResponse(retri_id=request.retri_id, monthly_amount=monthly_amount, profit=profit, datetime=datetime_value)