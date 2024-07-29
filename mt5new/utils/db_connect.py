
import os
import sys
import pymysql.cursors
from pymysql.connections import Connection
from starlette.config import Config
from datetime import datetime,timedelta
from pytz import timezone
from boto3 import client
from base64 import b64decode
import pandas as pd
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from utils.make_error import MakeErrorType
from base64 import b64decode
from models import *
from decimal import Decimal
import math
import numpy as np
import warnings
warnings.filterwarnings('ignore')


# 환경 변수 파일 관리
config = Config(".env")
# AWS_KMS_KEY_ID = config.get('AWS_KMS_KEY_ID')
AWS_KMS_KEY_ID = config.get('AWS_KMS_KEY_ID')

class MySQLAdapter:
    def __init__(self) -> None:

        self.KMS_CLIENT= client("kms", region_name='ap-northeast-2')
        self.exchange_id = 3
        self.now = datetime.now(timezone('Asia/Seoul'))
        self.return_dict_data=dict(results=[], reCode=1, message='Server Error')
        self.status_code=200
        self.status=0
        
    # DB Connection 확인
    def _get_connection(self):
        try:
            # print(config.get('USER'))
            # print(config.get('HOST'))
            # print(config.get('PASSWORD'))
            # print(config.get('DBNAME'))
            connection = Connection(host=config.get('HOST'),
                                    user=config.get('USER'),
                                    password=config.get('PASSWORD'),
                                    database=config.get('DBNAME'),
                                    cursorclass=pymysql.cursors.DictCursor)
            connection.ping(False)
            
        except Exception as e:
            print(e)
        else:
            return connection

    def get_update(self,user_id):
        conn = self._get_connection()


        try:
            if conn:
                with conn.cursor() as cursor:
                    sql = "SELECT  * FROM  Main ;" 
                        
                    cursor.execute(sql)

                    
                    data=cursor.fetchall()
                    print(data)
                return True

        except Exception as e:
            print(e)

        return False


    # 사용자 등록 확인
    def find_user(self, user_num: str) -> int:
        return_num = 0
        conn = self._get_connection()

        try:
            if conn:
                with conn.cursor() as cursor:
                    sql = f"SELECT * FROM Main WHERE fx_id={user_num}"
                    cursor.execute(sql)
                    result = cursor.fetchall()
                    cursor.close()
                    if len(result)>0:
                        
                        return False
                    
                    
                    else: 
                        
                        return True
                    
                        
        except Exception as e:
            pass
    
    # 사용자 등록 확인
    def new_find_user(self, user_num: str) -> int:
        return_num = 0
        conn = self._get_connection()

        try:
            if conn:
                with conn.cursor() as cursor:
                    sql = f"SELECT * FROM  Main m WHERE  retri_id ='{user_num}';"
                    cursor.execute(sql)
                    result = cursor.fetchall()
                    cursor.close()
                    # print(result)
                    if len(result)>0:
                        
                        return False
                    
                    
                    else: 
                        
                        return True
                    
                        
        except Exception as e:
            pass
    #         # DB 연결 Error인 경우 TXT 저장
    #         # message = f"Bybit DB Find User Error : {user_num} / {e}\n"
    #         # with open(f"logs/db_error.log", "a") as f:
    #         #     f.write(message)

       

    
        
    

    # def save_error_log(self, error: ErrorModel) -> bool:
    #     conn = self._get_connection()
        
    #     try:
    #         if conn:
    #             with conn.cursor() as cursor:
    #                 sql = "INSERT INTO error_log (error_code, message, user_num, datetime, exchange_id) VALUES " + \
    #                     f"('{error.error_code}','{error.message}','{error.user_num}','{self.now.strftime('%Y%m%d%H%M%S')}',{error.exchange_id})"
    #                 cursor.execute(sql)

    #                 conn.commit()
    #                 cursor.close()
                
    #             return True

    #     except Exception as e:
    #         print(e)

    #     return False
    def get_link_list(self,user_num):
        conn = self._get_connection()
        check = MakeErrorType()
        new_list=[]
        if conn:
            with conn.cursor() as cursor:
                
                if user_num==4:
                    sql = f"SELECT * FROM  Main m WHERE  check_status=1;"
                else:
                    sql = f"SELECT * FROM  Main m WHERE status ={user_num};"
                cursor.execute(sql)
                result=cursor.fetchall()
                
                if len(result)>0:
                    for i in result:
                        new_dict={}
                        new_dict['fx_id']=i['fx_id']
                        new_dict['retri_id']=i['retri_id']
                        new_list.append(new_dict)
            print(new_list)
            self.return_dict_data=dict(results=new_list)
            self.return_dict_data['reCode']=0
            self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
            self.status_code=200

            return True
        else:
            self.return_dict_data['reCode']=1
            self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
            self.status_code=423
            return False
        

    def get_delet(self,user_num):
        conn = self._get_connection()
        if conn:
            with conn.cursor() as cursor:
                sql = f"DELETE FROM Main WHERE fx_id ={user_num};"
                cursor.execute(sql)
                conn.commit()
                cursor.close()
            self.return_dict_data['reCode']=0
            self.status_code=200
            return True
        else:
            self.return_dict_data['reCode']=1
            self.status_code=423
            return False
    
    def get_udate(self,user_num):
        check = MakeErrorType()
        conn = self._get_connection()
        if conn:
            with conn.cursor() as cursor:
                sql = f"UPDATE Main set status=2 WHERE fx_id ={user_num};"
                cursor.execute(sql)
                conn.commit()
                cursor.close()


            self.return_dict_data['reCode']=0
            self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
            self.status_code=200
            return True
        else:
            self.return_dict_data['reCode']=1
            self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
            self.status_code=423
            return False




    def get_approve(self) -> bool:
        conn = self._get_connection()
        check = MakeErrorType()
        try:
            if conn:
               
                
                with conn.cursor() as cursor:
                    user_sql = f"SELECT * FROM `Main` WHERE status='1'" 

                    cursor.execute(user_sql)

                    result=cursor.fetchall()
                    result=pd.DataFrame(result)
                    # print(result)
                    new_list=[]
                    
                    
                    for i in result.iterrows():
                        new_dict={}
                        df_data=i[1]
                        # id=result[0]['fx_id']
                        # initial_amount=result[0]['initial_amount']
                        # deposit=result[0]['deposit']
                        new_dict['fx_id']=int(df_data['fx_id'])
                        new_dict['initial_amount']=str(df_data['initial_amount'])
                        new_dict['deposit']=str(df_data['deposit'])
                        new_list.append(new_dict)
                    self.return_dict_data['reCode']=0
                    self.return_dict_data['results']=new_list
                self.return_dict_data['reCode']=0  
                self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                return True
            else:
                self.return_dict_data['reCode']=1
                self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                return False
        except Exception as e:
        
            print(e)

    


    # Save Trade Log
    def save_user_log(self, trade: UserMdel,money) -> bool:
        conn = self._get_connection()
        
        aaa=datetime.strftime(self.now,"%Y-%m-%d %H:%M:%S")
        aaa1=datetime.strptime(aaa,"%Y-%m-%d %H:%M:%S")
        print(aaa1)
        pas=self.encrypt_data(trade.fx_pas)
        account=self.encrypt_data(trade.br_account)
        accountpas=self.encrypt_data(trade.br_accountpas)
        
        try:
            if conn:
                with conn.cursor() as cursor:
                    sql = "INSERT INTO Main (fx_id,password,account,accountpas,server,retri_id,initial_amount,deposit,money,datetime,status) VALUES " + \
                    f"({trade.fx_id},%s,%s,%s,'{trade.server}','{trade.retri_id}',{trade.amount},{trade.deposit},'{money}','{aaa1}',1)"
                    cursor.execute(sql,(pas,account,accountpas))

                    conn.commit()
                    cursor.close()
                
                return True

        except Exception as e:
            print(e)    

    
    # # # Error Log 저장
    # # def save_error_log(self, error: ErrorModel) -> bool:
    # #     pass
  
    # KEY 암호화 함수
    def encrypt_data(self, data: str) -> bytes:
        response = self.KMS_CLIENT.encrypt(KeyId=AWS_KMS_KEY_ID,Plaintext=data.encode())
       
        encrypted_data = response["CiphertextBlob"]

        return encrypted_data

    # KEY 복호화 함수
    def decrypt_data(self, encrypted_data: bytes) -> str:
        response = self.KMS_CLIENT.decrypt(KeyId=AWS_KMS_KEY_ID,CiphertextBlob=encrypted_data)
        decrypted_data = response["Plaintext"].decode()
        
        return decrypted_data
    
    
    
    
    # 사용자 등록 확인
    def approve_user(self, user_num: int) -> int:
        return_num = 0
        conn = self._get_connection()
        new_dict={}
        try:
            if conn:
                with conn.cursor() as cursor:
                    sql = f"SELECT * FROM Main WHERE fx_id={user_num}"
                    cursor.execute(sql)
                    result = cursor.fetchall()
                    cursor.close()
                    new_dict['mt_id']=result[0]['fx_id']
                    new_dict['retri_id']=result[0]['retri_id']
                    new_dict['mt_pas']=self.decrypt_data(result[0]['password'])
                    new_dict['mt_sercer']=result[0]['server']
                    new_dict['account']=self.decrypt_data(result[0]['account'])
                    new_dict['accountpas']=self.decrypt_data(result[0]['accountpas'])
                    new_dict['initial_amount']=str(result[0]['initial_amount'])
                    new_dict['deposit']=str(result[0]['deposit'])
                    new_dict['money']=str(result[0]['money'])
                    
                    return new_dict  
        except Exception as e:
            pass
        
    def new_approve_user(self, user_num: str) -> int:
        return_num = 0
        conn = self._get_connection()
        new_dict={}
        try:
            if conn:
                with conn.cursor() as cursor:
                    sql = f"SELECT * FROM Main WHERE retri_id='{user_num}'"
                    cursor.execute(sql)
                    result = cursor.fetchall()
                    cursor.close()
                    new_dict['mt_id']=result[0]['fx_id']
                    new_dict['mt_pas']=self.decrypt_data(result[0]['password'])
                    new_dict['mt_sercer']=result[0]['server']
                    new_dict['account']=self.decrypt_data(result[0]['account'])
                    new_dict['accountpas']=self.decrypt_data(result[0]['accountpas'])
                    new_dict['initial_amount']=str(result[0]['initial_amount'])
                    new_dict['deposit']=str(result[0]['deposit'])
                    new_dict['money']=str(result[0]['money'])
                    
                    return new_dict  
        except Exception as e:
            pass    
        
        
    def get_status(self,user_num):
        conn = self._get_connection()
        check = MakeErrorType()
        
        
        if conn:
            with conn.cursor() as cursor:
                sql = f"SELECT * FROM Main WHERE fx_id={user_num}"
                cursor.execute(sql)
                result = cursor.fetchall()
                cursor.close()
                status=result[0]['status']
        
        
        return status
    
    
    def new_get_status(self,user_num):
        conn = self._get_connection()
        check = MakeErrorType()
        
        
        if conn:
            with conn.cursor() as cursor:
                sql = f"SELECT * FROM Main WHERE retri_id='{user_num}'"
                cursor.execute(sql)
                result = cursor.fetchall()
                cursor.close()
                status=result[0]['status']
        
        
        return status
    
    
    def new_get_checkstatus(self,user_num):
        conn = self._get_connection()
        check = MakeErrorType()
        
        
        if conn:
            with conn.cursor() as cursor:
                sql = f"SELECT * FROM Main WHERE retri_id='{user_num}'"
                cursor.execute(sql)
                result = cursor.fetchall()
                cursor.close()
                status=result[0]['check_status']
                
                print(status)
                if status==1:
                    return True
                
                
                else:
                    return False
                
    
    
    def get_trade_count(self) -> bool:
        conn = self._get_connection()
        check = MakeErrorType()
        try:
            if conn:
               
                
                with conn.cursor() as cursor:
                    user_sql = f"SELECT * from trade_history" 

                    cursor.execute(user_sql)

                    result=cursor.fetchall()
                    result=pd.DataFrame(result)
                    new_list=[]
                    if len(result):
                        # print(result)
                        
                        new_dict={}
                        buy=len(result[result['position']=='buy'])
                        sell=len(result[result['position']=='sell'])
                        total=buy+sell
                        result['cad']=result['cad'].replace(np.nan,0)
                       
                        totalcad=round(sum(result['cad']),2)
                        # print(totalcad)
                        new_dict['buy']=buy
                        new_dict['sell']=sell
                        new_dict['total']=total
                        new_dict['totalcad']=totalcad
                        new_list.append(new_dict)
                    # print(new_list)
                    # for i in result.iterrows():
                    #     new_dict={}
                    #     df_data=i[1]
                    #     # id=result[0]['fx_id']
                    #     # initial_amount=result[0]['initial_amount']
                    #     # deposit=result[0]['deposit']
                    #     new_dict['fx_id']=int(df_data['fx_id'])
                       #     new_dict['initial_amount']=str(df_data['initial_amount'])
                    #     new_dict['deposit']=str(df_data['deposit'])
                    #     new_list.append(new_dict)
                    # self.return_dict_data['reCode']=0
                self.return_dict_data['results']=new_list
                self.return_dict_data['reCode']=0  
                self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                return True
            else:
                self.return_dict_data['reCode']=1
                self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                return False
        except Exception as e:
        
            print(e)       
                
    def get_position_count(self) -> bool:
        conn = self._get_connection()
        check = MakeErrorType()
        
        now_minut=(datetime.now(timezone('Asia/Seoul'))).strftime('%M')
        
        if int(now_minut) > 12:


            now_time=(datetime.now(timezone('Asia/Seoul'))).strftime('%Y-%m-%d %H')
            print(now_time)
            # now_time1=(datetime.now(timezone('Asia/Seoul'))-timedelta(hours=1)).strftime('%Y-%m-%d %H')
            
        else:

            now_time=(datetime.now(timezone('Asia/Seoul'))-timedelta(hours=1)).strftime('%Y-%m-%d %H')
            
        try:
            if conn:
               
                print(now_time)
                with conn.cursor() as cursor:
                    user_sql = f"SELECT * from position_history where date_format( updatetime  , '%Y-%m-%d %H:%i:%s') >='{now_time}' " 

                    cursor.execute(user_sql)

                    result=cursor.fetchall()
                    result=pd.DataFrame(result)
                    # print(result)
                    new_list=[]
                    if len(result):
                        # print(result)
                        
                        new_dict={}
                        buy=len(result[result['type']=='0'])
                        sell=len(result[result['type']=='1'])
                        total=buy+sell
                        result['cad']=result['cad'].apply(float)
                        totalcad=round(sum(result['cad']),2)
                        new_dict['buy']=buy
                        new_dict['sell']=sell
                        new_dict['total']=total
                        new_dict['totalcad']=totalcad
                        new_list.append(new_dict)
                        
                 
                # print('ree')   
                self.return_dict_data['results']=new_list
                self.return_dict_data['reCode']=0  
                self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                return True
            else:
                # print('assa')
                self.return_dict_data['reCode']=1
                self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                return False
        except Exception as e:
        
            print(e)        
        
      
            
    def new_balance_chck(self,retri_id):
        
        conn = self._get_connection()
        check = MakeErrorType()
        
        
        if conn:
            with conn.cursor() as cursor:
                sql = f"SELECT * FROM balance WHERE retri_id='{retri_id}'"
                cursor.execute(sql)
                result = cursor.fetchall()
                cursor.close()
                if len(result):
                    
                    return True
                
                else:
                    return False 

    
    def new_userbalance_chck(self,retri_id):
        
        conn = self._get_connection()
        check = MakeErrorType()
        
        
        if conn:
            with conn.cursor() as cursor:
                sql = f"SELECT * FROM balance WHERE fx_id='{retri_id}'"
                cursor.execute(sql)
                result = cursor.fetchall()
                cursor.close()
                if len(result):
                    
                    return True
                
                else:
                    return False 
    
    
    
    def check_list(self,retri_id,):
        
        conn = self._get_connection()
        check = MakeErrorType()
        
        if self.new_find_user(retri_id)==False:
            
            
            if self.new_get_checkstatus(retri_id):
                new_list=[]
                if conn:
                    with conn.cursor() as cursor:
                        
                    
                        sql = f"UPDATE Main set check_status=0 WHERE retri_id ='{retri_id}';"
                        cursor.execute(sql)
                        conn.commit()
                        cursor.close()
                    
                        self.return_dict_data['reCode']=0
                        self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                        self.status_code=200
                        
                        return True
                        
                        
                else:
                    self.return_dict_data['reCode']=1
                    self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                    self.status_code=423
                    return False   
            else:
                self.return_dict_data['reCode']=105
                self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                self.status_code=423    

        else:
            self.return_dict_data['reCode']=100
            self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
            self.status_code=423
            return False
        
        
    def get_user_stop(self,user_num,select):
        conn = self._get_connection()
        
        
        
        
            
        check = MakeErrorType()
        if self.find_user(user_num)==False:
            
            
            status=self.get_status(user_num)
            if status != select:
                if conn:
                    with conn.cursor() as cursor:
                        sql = f"UPDATE Main set status={select} WHERE fx_id ={user_num};"
                        cursor.execute(sql)
                        conn.commit()
                        cursor.close()

                    if select==2:
                        self.status=2
                    elif select ==3:
                        self.status=3
                        

                    self.return_dict_data['reCode']=0
                    self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                    self.status_code=200
                    
                    return True
                else:
                    self.return_dict_data['reCode']=1
                    self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                    self.status_code=423
                    return False
                
            else:
                self.return_dict_data['reCode']=105
                self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                self.status_code=423
                
        else:
            self.return_dict_data['reCode']=100
            self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
            self.status_code=423
            return False

    def get_manager_stop(self,user_num,select):
        conn = self._get_connection()
        
        
        
        
            
        check = MakeErrorType()
        if self.new_find_user(user_num)==False:
            
            
            status=self.new_get_status(user_num)
            if status != select:
                if conn:
                    with conn.cursor() as cursor:
                        sql = f"UPDATE Main set status={select} WHERE retri_id ='{user_num}';"
                        cursor.execute(sql)
                        conn.commit()
                        cursor.close()

                    if select==2:
                        self.status=2
                    elif select ==3:
                        self.status=3
                        

                    self.return_dict_data['reCode']=0
                    self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                    self.status_code=200
                    
                    return True
                else:
                    self.return_dict_data['reCode']=1
                    self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                    self.status_code=423
                    return False
                
            else:
                self.return_dict_data['reCode']=105
                self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                self.status_code=423
                
        else:
            self.return_dict_data['reCode']=100
            self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
            self.status_code=423
            return False

    def position_list(self,user_num:int,position_id:str):
        
        conn = self._get_connection()
        check = MakeErrorType()
        now_minut=(datetime.now(timezone('Asia/Seoul'))).strftime('%M')
        
        if int(now_minut) >= 12:


            now_time=(datetime.now(timezone('Asia/Seoul'))).strftime('%Y-%m-%d %H')
            print(now_time)
            # now_time1=(datetime.now(timezone('Asia/Seoul'))-timedelta(hours=1)).strftime('%Y-%m-%d %H')
            
        else:

            now_time=(datetime.now(timezone('Asia/Seoul'))-timedelta(hours=1)).strftime('%Y-%m-%d %H')
        if self.find_user(user_num)==False: 
            
              
            # print(now_time)
            new_list=[]
            if conn:
                with conn.cursor() as cursor:
                    
                    if position_id :
                        sql = f"SELECT * FROM position_history ph  WHERE fx_id={user_num} and date_format( updatetime  , '%Y-%m-%d %H:%i:%s') >='{now_time}' and position_id='{position_id}'  order by `datetime`  asc ;"
                    
                    else:
                        # sql = f"SELECT * FROM  position_history m WHERE fx_id ={user_num} and updatetime >={now_time} ;"
                        sql = f"SELECT * FROM position_history ph  WHERE fx_id={user_num} and date_format( updatetime  , '%Y-%m-%d %H:%i:%s') >='{now_time}' order by `datetime`  asc ;"
                        
                    cursor.execute(sql)
                    result=cursor.fetchall()
                    result=pd.DataFrame(result)
                    print("sdsds",result)
                    if len(result)>0:
                        for i in result.iterrows():
                            df_data=i[1]
                            new_dict={}
                            new_dict['fx_id']=df_data['fx_id']
                            new_dict['symbol']=df_data['symbol']
                            new_dict['sl']=df_data['sl']
                            new_dict['tp']=df_data['tp']
                            new_dict['profit']=df_data['profit']
                            new_dict['price_open']=df_data['price_open']
                            new_dict['price_current']=df_data['price_current']
                            new_dict['type']=df_data['type']
                            new_dict['volume']=df_data['volume']
                            new_dict['position_id']=str(df_data['position_id'])
                            new_dict['datetime']=str(df_data['datetime'])
                            new_list.append(new_dict)
                    
                self.return_dict_data=dict(results=new_list)
                self.return_dict_data['reCode']=0
                self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                self.status_code=200
                return True
            
            
            else:
                self.return_dict_data['reCode']=1
                self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                self.status_code=423
                return False
        else:
            self.return_dict_data['reCode']=100
            self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
            self.status_code=423
            return False

            
    def trade_list(self,user_num:int,from_data:str,to_data:str,select:int,page:int,size:int):
        print(from_data,to_data,select,user_num)
        conn = self._get_connection()
        check = MakeErrorType()
        self.return_dict_data=dict(page=0,size=0,totalPages=0,totalCount=0,results=[], reCode=1, message='Server Error')
        
        if size==0:
            
            self.return_dict_data['reCode']=107
            self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
            self.status_code=423
        
        
        new_size=(page*size)-size
            
            
        if self.find_user(user_num)==False: 
             
            if  not from_data and not to_data and  not select:
                # print('1')
                self.return_dict_data['reCode']=106
                self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                self.status_code=423
                
                return False
            
            elif  from_data and  to_data and   select  or (from_data and not  to_data and   select) or  ( not from_data and   to_data and   select):
                # print('2')
                self.return_dict_data['reCode']=106
                self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                self.status_code=423
                
                return False
            
            
            
            
            
            
            
            if select==2:
                from_data=(datetime.now(timezone('Asia/Seoul')) - timedelta(days=datetime.today().weekday())).strftime('%Y-%m-%d')
                to_data=(datetime.now(timezone('Asia/Seoul'))).strftime('%Y-%m-%d')
            
            elif select==1:
                
                from_data=(datetime.now(timezone('Asia/Seoul'))).strftime('%Y-%m-%d')
                to_data=(datetime.now(timezone('Asia/Seoul'))).strftime('%Y-%m-%d')
            elif select==3:
                
                from_data=(datetime.now(timezone('Asia/Seoul'))).strftime('%Y-%m')
                to_data=(datetime.now(timezone('Asia/Seoul'))).strftime('%Y-%m-%d')
                
            else:
                if not from_data or not to_data  :
                    self.return_dict_data['reCode']=106
                    self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                    self.status_code=423
                    return False    
    
            

            
            
                
            print(from_data)
            to_data=to_data+' 23'
            print(to_data)
            new_list=[]
            if conn:
                with conn.cursor() as cursor:
                    
                    if self.new_userbalance_chck(user_num):
                        sql= f"select *from (SELECT * FROM balance where fx_id='{user_num}' ORDER BY `datetime` desc limit 1)  as u inner join trade_history as b on u.fx_id =b.fx_id  WHERE u.fx_id={user_num} and date_format( trade_end  , '%Y-%m-%d %H:%i:%s') >='{from_data}' and date_format( trade_end  , '%Y-%m-%d %H:%i:%s')<='{to_data}' order by `trade_end`  asc limit {new_size},{size} ;"
                        sql1= f"select count(symbol) from (SELECT * FROM balance where fx_id='{user_num}' ORDER BY `datetime` desc limit 1)  as u inner join trade_history as b on u.fx_id =b.fx_id  WHERE u.fx_id={user_num} and date_format( trade_end  , '%Y-%m-%d %H:%i:%s') >='{from_data}' and date_format( trade_end  , '%Y-%m-%d %H:%i:%s')<='{to_data}' order by `trade_end`  asc ;"
                    # sql = f"SELECT * FROM  position_history m WHERE fx_id ={user_num} and updatetime >={now_time} ;"
                    # sql = f"SELECT * FROM trade_history ph  WHERE fx_id={user_num} and date_format( trade_end  , '%Y-%m-%d %H:%i:%s') >='{from_data}' and date_format( trade_end  , '%Y-%m-%d %H:%i:%s')<='{to_data}' order by `trade_end`  asc  ;"  
                    else:
                        sql= f"select *from Main  as u inner join trade_history as b on u.fx_id =b.fx_id  WHERE u.fx_id={user_num} and date_format( trade_end  , '%Y-%m-%d %H:%i:%s') >='{from_data}' and date_format( trade_end  , '%Y-%m-%d %H:%i:%s')<='{to_data}' order by `trade_end`  asc limit {new_size},{size} ;"
                        sql1= f"select count(symbol) from Main  as u inner join trade_history as b on u.fx_id =b.fx_id  WHERE u.fx_id={user_num} and date_format( trade_end  , '%Y-%m-%d %H:%i:%s') >='{from_data}' and date_format( trade_end  , '%Y-%m-%d %H:%i:%s')<='{to_data}' order by `trade_end`  asc ;"
                    cursor.execute(sql)
                    result=cursor.fetchall()
                    result=pd.DataFrame(result)
                    
                    print(len(result))
                    
                    cursor.execute(sql1)
                    result1=cursor.fetchall()
                    result1=pd.DataFrame(result1)
                    print(result1)
                    print(result1['count(symbol)'][0])
                    if result1['count(symbol)'][0]>0:
                        
                        fx_id_count=result1['count(symbol)'][0]
                    else:
                        fx_id_count=0
                    self.return_dict_data['page']=int(page)
                    self.return_dict_data['size']=int(size)
                    self.return_dict_data['totalPages']=math.ceil((fx_id_count/size))
                    self.return_dict_data['totalCount']=int(fx_id_count)
                
                    if len(result)>0:
                        # profit=sum(result['profit'])
                        # befor_balance=result['initial_amount'].iloc[0]
                        # balance=Decimal(f'{befor_balance}') + Decimal(f'{profit}')
                        # roi=round((Decimal(f'{profit}')/Decimal(f'{befor_balance}'))*Decimal('100'),4)
                        # print(len(result))
                        
                        # bal_dict={}
                        # bal_dict['balance']=str(balance)
                        # bal_dict['initial_amount']=str(befor_balance)
                        # bal_dict['total profit']=str(profit)
                        # bal_dict['roi']=str(roi)
                        # bal_dict['datetime']=str(result['datetime'].iloc[0])
                        # self.return_dict_data['results']['balance'].append(bal_dict)
                        
                        for i in result.iterrows():
                            df_data=i[1]
                            new_dict={}
                            new_dict['fx_id']=df_data['fx_id']
                            new_dict['symbol']=df_data['symbol']
                            new_dict['profit']=str(df_data['profit'])
                            new_dict['price_open']=df_data['buy_price']
                            new_dict['price_close']=df_data['sell_price']
                            new_dict['type']=df_data['position']
                            new_dict['position_id']=str(df_data['position_id'])
                            new_dict['open_datetime']=str(df_data['trade_start'])
                            new_dict['close_datetime']=str(df_data['trade_end'])
                            new_dict['swap']=str(df_data['swap'])
                            new_dict['tax']=str(df_data['tax'])
                            new_dict['fee']=str(df_data['fee'])
                            new_dict['volume']=str(df_data['volume'])
                            self.return_dict_data['results'].append(new_dict)
                            # new_list.append(new_dict)
                    # print(new_list)
                # self.return_dict_data=dict(results=new_list)
                self.return_dict_data['reCode']=0
                self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                self.status_code=200
                
                return True
            
        
            else:
                self.return_dict_data['reCode']=1
                self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                self.status_code=423
                
                return False
        else:
            self.return_dict_data['reCode']=100
            self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
            self.status_code=423
            return False
        
    def connet_list(self,from_data,to_data,select,user_num,trade,page,size,check_status):
        
        
        self.return_dict_data=dict(page=0,size=0,totalPages=0,totalCount=0,results=[], reCode=1, message='Server Error')
        print(from_data,to_data,select,user_num)
        conn = self._get_connection()
        check = MakeErrorType()
        if size==0:
            
            self.return_dict_data['reCode']=107
            self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
            self.status_code=423
        
        
        new_size=(page*size)-size
       
        new_list=[]
        if user_num=='':
            
            if  from_data and  to_data and   select:
                # print('2')
                self.return_dict_data['reCode']=106
                self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                self.status_code=423
                
                return False
            
        
            if select==2:
                from_data=(datetime.now(timezone('Asia/Seoul')) - timedelta(days=datetime.today().weekday())).strftime('%Y-%m-%d')
                to_data=(datetime.now(timezone('Asia/Seoul'))).strftime('%Y-%m-%d')
            
            elif select==1:
                
                from_data=(datetime.now(timezone('Asia/Seoul'))).strftime('%Y-%m-%d')
                to_data=(datetime.now(timezone('Asia/Seoul'))).strftime('%Y-%m-%d')
            elif select==3:
                
                from_data=(datetime.now(timezone('Asia/Seoul'))).strftime('%Y-%m')
                to_data=(datetime.now(timezone('Asia/Seoul'))).strftime('%Y-%m-%d')
            else:
                if not from_data or not to_data  :
                    self.return_dict_data['reCode']=106
                    self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                    self.status_code=423
                    return False    
        
            to_data=to_data+' 23'
            print(from_data)
            print(to_data)
            if conn:
                with conn.cursor() as cursor:
                    # sql = f"SELECT * FROM  position_history m WHERE fx_id ={user_num} and updatetime >={now_time} ;"
                    if trade ==0:
                        
                        sql = f"SELECT * FROM Main  WHERE  date_format( datetime  , '%Y-%m-%d %H:%i:%s') >='{from_data}' and date_format( datetime  , '%Y-%m-%d %H:%i:%s')<='{to_data}' and status>={trade} order by `datetime`  asc limit {new_size},{size};"
                        sql1 = f"SELECT count(fx_id) FROM Main  WHERE  date_format( datetime  , '%Y-%m-%d %H:%i:%s') >='{from_data}' and date_format( datetime  , '%Y-%m-%d %H:%i:%s')<='{to_data}' and status>={trade} order by `datetime`  asc ;"  
                        
                        if check_status==0:
                            
                            sql = f"SELECT * FROM Main  WHERE  date_format( datetime  , '%Y-%m-%d %H:%i:%s') >='{from_data}' and date_format( datetime  , '%Y-%m-%d %H:%i:%s')<='{to_data}' and status>={trade} and check_status =0 order by `datetime`  asc limit {new_size},{size};"
                            sql1 = f"SELECT count(fx_id) FROM Main  WHERE  date_format( datetime  , '%Y-%m-%d %H:%i:%s') >='{from_data}' and date_format( datetime  , '%Y-%m-%d %H:%i:%s')<='{to_data}' and status>={trade} and check_status =0 order by `datetime`  asc ;" 
                        
                        elif check_status==1:
                               
                            sql = f"SELECT * FROM Main  WHERE  date_format( datetime  , '%Y-%m-%d %H:%i:%s') >='{from_data}' and date_format( datetime  , '%Y-%m-%d %H:%i:%s')<='{to_data}' and status>={trade} and check_status =1 order by `datetime`  asc limit {new_size},{size};"
                            sql1 = f"SELECT count(fx_id) FROM Main  WHERE  date_format( datetime  , '%Y-%m-%d %H:%i:%s') >='{from_data}' and date_format( datetime  , '%Y-%m-%d %H:%i:%s')<='{to_data}' and status>={trade} and check_status =1 order by `datetime`  asc ;" 
                        
                        else:
                            
                            
                            sql = f"SELECT * FROM Main  WHERE  date_format( datetime  , '%Y-%m-%d %H:%i:%s') >='{from_data}' and date_format( datetime  , '%Y-%m-%d %H:%i:%s')<='{to_data}' and status>={trade} and check_status >=0 order by `datetime`  asc limit {new_size},{size};"
                            sql1 = f"SELECT count(fx_id) FROM Main  WHERE  date_format( datetime  , '%Y-%m-%d %H:%i:%s') >='{from_data}' and date_format( datetime  , '%Y-%m-%d %H:%i:%s')<='{to_data}' and status>={trade} and check_status >=0 order by `datetime`  asc ;"         
                        
                    else:
                        print('동작')
                        sql = f"SELECT * FROM Main  WHERE  date_format( datetime  , '%Y-%m-%d %H:%i:%s') >='{from_data}' and date_format( datetime  , '%Y-%m-%d %H:%i:%s')<='{to_data}' and status={trade} order by `datetime`  asc limit {new_size},{size};"
                        sql1 = f"SELECT count(fx_id) FROM Main  WHERE  date_format( datetime  , '%Y-%m-%d %H:%i:%s') >='{from_data}' and date_format( datetime  , '%Y-%m-%d %H:%i:%s')<='{to_data}' and status={trade} order by `datetime`  asc ;"
                        
                        if check_status ==0:
                            sql = f"SELECT * FROM Main  WHERE  date_format( datetime  , '%Y-%m-%d %H:%i:%s') >='{from_data}' and date_format( datetime  , '%Y-%m-%d %H:%i:%s')<='{to_data}' and status={trade} and check_status =0 order by `datetime`  asc limit {new_size},{size};"
                            sql1 = f"SELECT count(fx_id) FROM Main  WHERE  date_format( datetime  , '%Y-%m-%d %H:%i:%s') >='{from_data}' and date_format( datetime  , '%Y-%m-%d %H:%i:%s')<='{to_data}' and status={trade} and check_status =0 order by `datetime`  asc ;"
                            
                        elif check_status ==1:
                           
                            sql = f"SELECT * FROM Main  WHERE  date_format( datetime  , '%Y-%m-%d %H:%i:%s') >='{from_data}' and date_format( datetime  , '%Y-%m-%d %H:%i:%s')<='{to_data}' and status={trade} and check_status =1 order by `datetime`  asc limit {new_size},{size};"
                            sql1 = f"SELECT count(fx_id) FROM Main  WHERE  date_format( datetime  , '%Y-%m-%d %H:%i:%s') >='{from_data}' and date_format( datetime  , '%Y-%m-%d %H:%i:%s')<='{to_data}' and status={trade} and check_status =1 order by `datetime`  asc ;"
                        else: 
                            
                            sql = f"SELECT * FROM Main  WHERE  date_format( datetime  , '%Y-%m-%d %H:%i:%s') >='{from_data}' and date_format( datetime  , '%Y-%m-%d %H:%i:%s')<='{to_data}' and status={trade} and check_status >=0 order by `datetime`  asc limit {new_size},{size};"
                            sql1 = f"SELECT count(fx_id) FROM Main  WHERE  date_format( datetime  , '%Y-%m-%d %H:%i:%s') >='{from_data}' and date_format( datetime  , '%Y-%m-%d %H:%i:%s')<='{to_data}' and status={trade} and check_status >=0 order by `datetime`  asc ;" 
                            
                    cursor.execute(sql)
                    result=cursor.fetchall()
                    result=pd.DataFrame(result)
                    print("1adsads",result)
                    
                    cursor.execute(sql1)
                    result1=cursor.fetchall()
                    result1=pd.DataFrame(result1)
                    print('2asdas',result1)
                    print(result1['count(fx_id)'][0])
                    if result1['count(fx_id)'][0]>0:
                        
                        fx_id_count=result1['count(fx_id)'][0]
                    else:
                        fx_id_count=0
                    
                    self.return_dict_data['page']=int(page)
                    self.return_dict_data['size']=int(size)
                    self.return_dict_data['totalPages']=math.ceil((fx_id_count/size))
                    self.return_dict_data['totalCount']=int(fx_id_count)
                    
                    
                    if len(result)>0:
                        for i in result.iterrows():
                            df_data=i[1]
                            new_dict={}
                            new_dict['fx_id']=df_data['fx_id']
                            new_dict['retri_id']=df_data['retri_id']
                            new_dict['initial_amount']=str(df_data['initial_amount'])
                            new_dict['deposit']=str(df_data['deposit'])
                            new_dict['datetime']=str(df_data['datetime'])
                            new_dict['connet']=df_data['status']
                            new_dict['check_status']=df_data['check_status']
                            self.return_dict_data['results'].append(new_dict)
                            # new_list.append(new_dict)
                    # print(self.return_dict_data)
                # self.return_dict_data['results']=new_list
                # self.return_dict_data=dict(results=new_list)
                self.return_dict_data['reCode']=0
                self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                self.status_code=200
                
                return True
            
            
            else:
                self.return_dict_data['reCode']=1
                self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                self.status_code=423
                
                return False
            
            
        else:
            
     
                
            
            
            if self.new_find_user(user_num)==False:
                
                if  from_data or  to_data or   select:
                    # print('2')
                    self.return_dict_data['reCode']=106
                    self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                    self.status_code=423
                    
                    return False
                
                # if not from_data and  to_data and select==0 and trade==0:
              
                with conn.cursor() as cursor:
                # sql = f"SELECT * FROM  position_history m WHERE fx_id ={user_num} and updatetime >={now_time} ;"
                    
                    sql = f"SELECT * FROM  Main m WHERE  retri_id ='{user_num}';"
                    cursor.execute(sql)
                    result=cursor.fetchall()
                    result=pd.DataFrame(result)
                    
                    
                    
                    if len(result)>0:
                        for i in result.iterrows():
                            df_data=i[1]
                            new_dict={}
                            df_data=i[1]
                            new_dict={}
                            new_dict['fx_id']=df_data['fx_id']
                            new_dict['retri_id']=df_data['retri_id']
                            new_dict['initial_amount']=str(df_data['initial_amount'])
                            new_dict['deposit']=str(df_data['deposit'])
                            new_dict['datetime']=str(df_data['datetime'])
                            new_dict['connet']=df_data['status']
                            new_dict['check_status']=df_data['check_status']
                            # new_list.append(new_dict)
                            
                        
                            self.return_dict_data['results'].append(new_dict)
                        # self.return_dict_data=dict(results=new_list)
                        self.return_dict_data['reCode']=0
                        self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                        self.status_code=200
                        
                        return True
                    
                    
                    else:
                        self.return_dict_data['reCode']=1
                        self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                        self.status_code=423
                        
                        return False

                    
            else:
                
                self.return_dict_data['reCode']=100
                self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                self.status_code=423
                return False
            #         if conn:
                        
                        
            #             with conn.cursor() as cursor:
            #                 # sql = f"SELECT * FROM  position_history m WHERE fx_id ={user_num} and updatetime >={now_time} ;"
            #                 if trade ==0:
            #                     sql = f"SELECT * FROM Main  WHERE fx_id={user_num} and date_format( datetime  , '%Y-%m-%d %H:%i:%s') >='{from_data}' and date_format( datetime  , '%Y-%m-%d %H:%i:%s')<='{to_data}' and status >={trade} order by `datetime`  asc  ;"  
            #                 else:
            #                     sql = f"SELECT * FROM Main  WHERE fx_id={user_num} and date_format( datetime  , '%Y-%m-%d %H:%i:%s') >='{from_data}' and date_format( datetime  , '%Y-%m-%d %H:%i:%s')<='{to_data}' and status ={trade} order by `datetime`  asc ;"
                                
                            
            #                 cursor.execute(sql)
            #                 result=cursor.fetchall()
            #                 result=pd.DataFrame(result)
                            
                            
                            
            #                 if len(result)>0:
            #                     for i in result.iterrows():
            #                         df_data=i[1]
            #                         new_dict={}
            #                         df_data=i[1]
            #                         new_dict={}
            #                         new_dict['retri_id']=df_data['retri_id']
            #                         new_dict['initial_amount']=str(df_data['initial_amount'])
            #                         new_dict['deposit']=str(df_data['deposit'])
            #                         new_dict['datetime']=str(df_data['datetime'])
            #                         new_dict['connet']=df_data['status']
            #                         new_list.append(new_dict)
            #                 print(new_list)
            #             self.return_dict_data=dict(results=new_list)
            #             self.return_dict_data['reCode']=0
            #             self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
            #             self.status_code=200
                        
            #             return True
                    
                    
            #         else:
            #             self.return_dict_data['reCode']=1
            #             self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
            #             self.status_code=423
                        
            #             return False
            # else:
                
            
    def connet_count(self):
        conn = self._get_connection()
        check = MakeErrorType()
        
        new_list=[]
        if conn:
            with conn.cursor() as cursor:
                # sql = f"SELECT * FROM  position_history m WHERE fx_id ={user_num} and updatetime >={now_time} ;"
                sql = f"SELECT * FROM Main;"
                
                cursor.execute(sql)
                result=cursor.fetchall()
                result=pd.DataFrame(result)
                
                new_dict={}
                if len(result)>0:
                    
                    pause=int(len(result[result['status']==3]))
                    notlinked=int(len(result[result['status']==1]))
                    
                    if (len(result[result['status']==2]))>0:
                        redy=int(len(result[result['status']==2]))
                    else:
                        redy=0

                    if len(result[result['status']==3])>0:
                        pause=int(len(result[result['status']==3]))
                    else:
                        pause=0    
                    
                    if len(result[result['status']==1])>0:
                        notlinked=int(len(result[result['status']==1]))
                    else:
                        notlinked=0
                        
                        
                        
                                               
                    new_dict['operation']=redy
                    new_dict['pause']=pause
                    new_dict['notlinked']=notlinked
                    new_dict['sum']=redy+pause+notlinked
            new_list.append(new_dict)    
            self.return_dict_data=dict(results=new_list)
            self.return_dict_data['reCode']=0
            self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
            self.status_code=200
            
        return True
    
    
    def manager_trade_list(self,user_num:str,from_data:str,to_data:str,select,trade,page:int,size:int):
        print(select)
        conn = self._get_connection()
        check = MakeErrorType()
        self.return_dict_data=dict(page=0,size=0,totalPages=0,totalCount=0,results={'history':[],'balance':[]}, reCode=1, message='Server Error')
        if size==0:
            
            self.return_dict_data['reCode']=107
            self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
            self.status_code=423
        new_size=(page*size)-size
        if self.new_find_user(user_num)==False:
             
            if  not from_data and not to_data and  not select:
                # print('1')
                self.return_dict_data['reCode']=106
                self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                self.status_code=423
                
                return False
            
            elif  (from_data and  to_data and   select) or (from_data and not  to_data and   select) or  ( not from_data and   to_data and   select) :
                # print('2')
                self.return_dict_data['reCode']=106
                self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                self.status_code=423
                
                return False
            
            
            if select==2:
                from_data=(datetime.now(timezone('Asia/Seoul')) - timedelta(days=datetime.today().weekday())).strftime('%Y-%m-%d')
                to_data=(datetime.now(timezone('Asia/Seoul'))).strftime('%Y-%m-%d')
            
            elif select==1:
                
                from_data=(datetime.now(timezone('Asia/Seoul'))).strftime('%Y-%m-%d')
                to_data=(datetime.now(timezone('Asia/Seoul'))).strftime('%Y-%m-%d')
            elif select==3:
                
                from_data=(datetime.now(timezone('Asia/Seoul'))).strftime('%Y-%m')
                to_data=(datetime.now(timezone('Asia/Seoul'))).strftime('%Y-%m-%d')
            else:
                if not from_data or not to_data  :
                    self.return_dict_data['reCode']=106
                    self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                    self.status_code=423
                    return False
            
            

            
            
                
            # print(from_data)
            to_data=to_data+' 23'
            print(to_data)
            new_list=[]
            if conn:
                with conn.cursor() as cursor:
                    
                    if self.new_balance_chck(user_num):
                        
                        if trade==0:
                            sql = f"select *from (SELECT * FROM balance where retri_id='{user_num}' ORDER BY `datetime` desc limit 1)  as u inner join trade_history as b on u.fx_id =b.fx_id  WHERE u.retri_id='{user_num}' and date_format( trade_end  , '%Y-%m-%d %H:%i:%s') >='{from_data}' and date_format( trade_end  , '%Y-%m-%d %H:%i:%s')<='{to_data}' order by `trade_end`  asc limit {new_size},{size} ;" 
                            sql1 = f"select *from (SELECT * FROM balance where retri_id='{user_num}' ORDER BY `datetime` desc limit 1)  as u inner join trade_history as b on u.fx_id =b.fx_id  WHERE u.retri_id='{user_num}' and date_format( trade_end  , '%Y-%m-%d %H:%i:%s') >='{from_data}' and date_format( trade_end  , '%Y-%m-%d %H:%i:%s')<='{to_data}' order by `trade_end`  asc  ;" 
                        elif trade==1:
                            sql = f"select *from (SELECT * FROM balance where retri_id='{user_num}' ORDER BY `datetime` desc limit 1)  as u inner join trade_history as b on u.fx_id =b.fx_id  WHERE u.retri_id='{user_num}' and date_format( trade_end  , '%Y-%m-%d %H:%i:%s') >='{from_data}' and date_format( trade_end  , '%Y-%m-%d %H:%i:%s')<='{to_data}'and position='buy'order by `trade_end`  asc limit {new_size},{size} ;"
                            sql1 = f"select *from (SELECT * FROM balance where retri_id='{user_num}' ORDER BY `datetime` desc limit 1)  as u inner join trade_history as b on u.fx_id =b.fx_id  WHERE u.retri_id='{user_num}' and date_format( trade_end  , '%Y-%m-%d %H:%i:%s') >='{from_data}' and date_format( trade_end  , '%Y-%m-%d %H:%i:%s')<='{to_data}'and position='buy'order by `trade_end`  asc  ;"
                        elif trade==2:
                            sql = f"select *from (SELECT * FROM balance where retri_id='{user_num}' ORDER BY `datetime` desc limit 1)  as u inner join trade_history as b on u.fx_id =b.fx_id  WHERE u.retri_id='{user_num}' and date_format( trade_end  , '%Y-%m-%d %H:%i:%s') >='{from_data}' and date_format( trade_end  , '%Y-%m-%d %H:%i:%s')<='{to_data}'and position='sell' order by `trade_end`  asc limit {new_size},{size};"
                            sql1 = f"select *from (SELECT * FROM balance where retri_id='{user_num}' ORDER BY `datetime` desc limit 1)  as u inner join trade_history as b on u.fx_id =b.fx_id  WHERE u.retri_id='{user_num}' and date_format( trade_end  , '%Y-%m-%d %H:%i:%s') >='{from_data}' and date_format( trade_end  , '%Y-%m-%d %H:%i:%s')<='{to_data}'and position='sell' order by `trade_end`  asc ;"
                    
                    else:
                        
                        # sql = f"SELECT * FROM  position_history m WHERE fx_id ={user_num} and updatetime >={now_time} ;"
                        if trade==0:
                            sql = f"select *from Main  as u inner join trade_history as b on u.fx_id =b.fx_id  WHERE u.retri_id='{user_num}' and date_format( trade_end  , '%Y-%m-%d %H:%i:%s') >='{from_data}' and date_format( trade_end  , '%Y-%m-%d %H:%i:%s')<='{to_data}' order by `trade_end`  asc limit {new_size},{size} ;"
                            sql1 = f"select *from Main  as u inner join trade_history as b on u.fx_id =b.fx_id  WHERE u.retri_id='{user_num}' and date_format( trade_end  , '%Y-%m-%d %H:%i:%s') >='{from_data}' and date_format( trade_end  , '%Y-%m-%d %H:%i:%s')<='{to_data}' order by `trade_end`  asc ;"  
                        elif trade==1:
                            sql = f"select *from Main  as u inner join trade_history as b on u.fx_id =b.fx_id  WHERE u.retri_id='{user_num}' and date_format( trade_end  , '%Y-%m-%d %H:%i:%s') >='{from_data}' and date_format( trade_end  , '%Y-%m-%d %H:%i:%s')<='{to_data}'and position='buy'order by `trade_end`  asc limit {new_size},{size} ;"
                            sql1 = f"select *from Main  as u inner join trade_history as b on u.fx_id =b.fx_id  WHERE u.retri_id='{user_num}' and date_format( trade_end  , '%Y-%m-%d %H:%i:%s') >='{from_data}' and date_format( trade_end  , '%Y-%m-%d %H:%i:%s')<='{to_data}'and position='buy'order by `trade_end`  asc  ;"
                        elif trade==2:
                            sql = f"select *from Main  as u inner join trade_history as b on u.fx_id =b.fx_id  WHERE u.retri_id='{user_num}' and date_format( trade_end  , '%Y-%m-%d %H:%i:%s') >='{from_data}' and date_format( trade_end  , '%Y-%m-%d %H:%i:%s')<='{to_data}'and position='sell' order by `trade_end`  asc limit {new_size},{size};"
                            sql1 = f"select *from Main  as u inner join trade_history as b on u.fx_id =b.fx_id  WHERE u.retri_id='{user_num}' and date_format( trade_end  , '%Y-%m-%d %H:%i:%s') >='{from_data}' and date_format( trade_end  , '%Y-%m-%d %H:%i:%s')<='{to_data}'and position='sell' order by `trade_end`  asc ;"
                    
                    
                    sql2 = f"select *from mt5_balance where retri_id='{user_num}' ;" 
                    
                      
                    cursor.execute(sql)
                    result=cursor.fetchall()
                    result=pd.DataFrame(result)
                    # print(result)
                  
                    cursor.execute(sql1)
                    result1=cursor.fetchall()
                    result1=pd.DataFrame(result1)
                    
                    cursor.execute(sql2)
                    result2=cursor.fetchall()
                    result2=pd.DataFrame(result2)
                    # print(result2)
                    
                    if len(result2)>0:
                        
                        balance=result2['mt5_balance'][0]
                    else:
                        balance=0
                    
                    if len(result1)>0:
                        
                        print('test 동작')
                        profit=sum(result1['profit'])
                        befor_balance=result1['initial_amount'].iloc[0]
                        deposit=result1['deposit'].iloc[0]
                        # balance=Decimal(f'{befor_balance}') + Decimal(f'{profit}')
                        roi=round((Decimal(f'{profit}')/Decimal(f'{befor_balance}'))*Decimal('100'),4)
                        if profit >0:
                            deposit_deduction=profit*Decimal('0.1')
                        else:
                            deposit_deduction=0
                        
                        bal_dict={}
                        bal_dict['balance']=str(balance)
                        bal_dict['initial_amount']=str(befor_balance)
                        bal_dict['total_profit']=str(profit)
                        bal_dict['deposit_deduction']=str(deposit_deduction)
                        bal_dict['deposit']=str(deposit)
                        bal_dict['roi']=str(roi)
                        bal_dict['round']=str(result1['check'].iloc[0])
                        bal_dict['datetime']=str(result1['datetime'].iloc[0])
                        self.return_dict_data['results']['balance'].append(bal_dict)
                    
                        
                   
                        self.return_dict_data['page']=page
                        self.return_dict_data['size']=size
                        self.return_dict_data['totalPages']=math.ceil((len(result1)/size))
                        self.return_dict_data['totalCount']=len(result1)
                        
                        
                    
                    else:
                        print('동작')
                        with conn.cursor() as cursor:
                        
                            if self.new_balance_chck(user_num):
                            
                            
                                print('ch')
                                new_sql=f"SELECT * FROM balance where retri_id='{user_num}' ORDER BY `datetime` desc limit 1"
                            else:
                                print('sh')
                                new_sql=f"SELECT * FROM Main where retri_id='{user_num}';"
                        
                        
                        
                            cursor.execute(new_sql)
                            result1=cursor.fetchall()
                            result1=pd.DataFrame(result1)
                            
                        
                        profit=0
                        befor_balance=result1['initial_amount'].iloc[0]
                        # balance=Decimal(f'{befor_balance}') + Decimal(f'{profit}')
                        roi=round((Decimal(f'{profit}')/Decimal(f'{befor_balance}'))*Decimal('100'),4)
                        deposit=result1['deposit'].iloc[0]
                        deposit_deduction=0
                        
                        
                        bal_dict={}
                        bal_dict['balance']=str(balance)
                        bal_dict['initial_amount']=str(befor_balance)
                        bal_dict['total profit']=str(profit)
                        bal_dict['deposit_deduction']=str(deposit_deduction)
                        bal_dict['deposit']=str(deposit)
                        bal_dict['roi']=str(roi)
                        bal_dict['round']=str(result1['check'].iloc[0])
                        bal_dict['datetime']=str(result1['datetime'].iloc[0])
                        self.return_dict_data['results']['balance'].append(bal_dict)
                    
                    
                   
                        self.return_dict_data['page']=page
                        self.return_dict_data['size']=size
                        self.return_dict_data['totalPages']=math.ceil((len(result1)/size))
                        self.return_dict_data['totalCount']=len(result1)
                        
                   
                    
                    if len(result)>0:
                        
                        
                        for i in result.iterrows():
                            
                            
                            
                            df_data=i[1]
                           
                            new_dict={}
                            new_dict['fx_id']=df_data['fx_id']
                            new_dict['retri_id']=df_data['retri_id']
                            new_dict['symbol']=df_data['symbol']
                            new_dict['profit']=str(df_data['profit'])
                            new_dict['price_open']=df_data['buy_price']
                            new_dict['price_close']=df_data['sell_price']
                            new_dict['type']=df_data['position']
                            new_dict['volume']=df_data['volume']
                            new_dict['deposit']=df_data['b.deposit']
                            new_dict['position_id']=str(df_data['position_id'])
                            new_dict['open_datetime']=str(df_data['trade_start'])
                            new_dict['close_datetime']=str(df_data['trade_end'])
                            new_dict['swap']=str(df_data['swap'])
                            new_dict['tax']=str(df_data['tax'])
                            new_dict['fee']=str(df_data['fee'])
                           
                            
                            self.return_dict_data['results']['history'].append(new_dict)
                    # print(profit1)
                
                
                # self.return_dict_data=dict(results=new_list)
                self.return_dict_data['reCode']=0
                self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                self.status_code=200
                
                return True
            
        
            else:
                self.return_dict_data['reCode']=1
                self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                self.status_code=423
                
                return False
        else:
            self.return_dict_data['reCode']=100
            self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
            self.status_code=423
            return False
    
    def manager_position_list(self,user_num:str,page:int=1,size:int=5):
        
        conn = self._get_connection()
        check = MakeErrorType()
        now_minut=(datetime.now(timezone('Asia/Seoul'))).strftime('%M')
        self.return_dict_data=dict(page=0,size=0,totalPages=0,totalCount=0,results=[], reCode=1, message='Server Error')
        
        if size==0:
            
            self.return_dict_data['reCode']=107
            self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
            self.status_code=423
        new_size=(page*size)-size
        
        if int(now_minut) >= 12:


            now_time=(datetime.now(timezone('Asia/Seoul'))).strftime('%Y-%m-%d %H')
            print(now_time)
            # now_time1=(datetime.now(timezone('Asia/Seoul'))-timedelta(hours=1)).strftime('%Y-%m-%d %H')
            
        else:

            now_time=(datetime.now(timezone('Asia/Seoul'))-timedelta(hours=1)).strftime('%Y-%m-%d %H')
        if self.new_find_user(user_num)==False: 
            
              
            # print(now_time)
            new_list=[]
            if conn:
                with conn.cursor() as cursor:
                    # sql = f"SELECT * FROM  position_history m WHERE fx_id ={user_num} and updatetime >={now_time} ;"
                    sql = f"select *from Main  as u inner join position_history  as b on u.fx_id =b.fx_id  WHERE u.retri_id ='{user_num}' and date_format( updatetime  , '%Y-%m-%d %H:%i:%s') >='{now_time}' order by `updatetime`  asc limit {new_size},{size};"
                    
                    sql1 = f"select count(symbol)from Main  as u inner join position_history  as b on u.fx_id =b.fx_id  WHERE u.retri_id ='{user_num}' and date_format( updatetime  , '%Y-%m-%d %H:%i:%s') >='{now_time}' order by `updatetime`  asc ;"
                    cursor.execute(sql)
                    result=cursor.fetchall()
                    result=pd.DataFrame(result)
                    print("sdsds",result)
                    
                    
                    cursor.execute(sql1)
                    result1=cursor.fetchall()
                    result1=pd.DataFrame(result1)
                    if result1['count(symbol)'][0] >0:
                        
                        fx_id_count=result1['count(symbol)'][0]
                    else:
                        fx_id_count=0
                        
                        
                    
                    self.return_dict_data['page']=int(page)
                    self.return_dict_data['size']=int(size)
                    self.return_dict_data['totalPages']=math.ceil((fx_id_count/size))
                    self.return_dict_data['totalCount']=int(fx_id_count)
                    
                    if len(result)>0:
                        for i in result.iterrows():
                            df_data=i[1]
                            new_dict={}
                            new_dict['fx_id']=df_data['fx_id']
                            new_dict['retri_id']=df_data['retri_id']
                            new_dict['symbol']=df_data['symbol']
                            new_dict['sl']=df_data['sl']
                            new_dict['tp']=df_data['tp']
                            new_dict['profit']=df_data['profit']
                            new_dict['price_open']=df_data['price_open']
                            new_dict['price_current']=df_data['price_current']
                            new_dict['type']=df_data['type']
                            new_dict['volume']=df_data['volume']
                            new_dict['position_id']=str(df_data['position_id'])
                            new_dict['datetime']=str(df_data['b.datetime'])
                            self.return_dict_data['results'].append(new_dict)
                            # new_list.append(new_dict)
                            
                
                # self.return_dict_data=dict(results=new_list)
                self.return_dict_data['reCode']=0
                self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                self.status_code=200
                return True
            
            
            else:
                self.return_dict_data['reCode']=1
                self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                self.status_code=423
                return False
        else:
            self.return_dict_data['reCode']=100
            self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
            self.status_code=423
            return False
    
    
    
    
    
    def dashboard_list(self,user_num:int):
       
        conn = self._get_connection()
        check = MakeErrorType()
        
        self.return_dict_data=dict(results={'deposit':[],'profit':[],'days':[]}, reCode=1, message='Server Error')
        from_data=(datetime.now(timezone('Asia/Seoul'))).strftime('%Y-%m')
        
        
        
        
        # print(from_data)
        if self.find_user(user_num)==False:
             
        
            
            
                
            # print(from_data)
        
            new_list=[]
            new_dict={}
            if conn:
                with conn.cursor() as cursor:
                    
                    if self.new_userbalance_chck(user_num):
                        
                        print('HE')
                        sql = f"select *from (SELECT * FROM balance where fx_id='{user_num}' ORDER BY `datetime` desc limit 1)  as u inner join trade_history as b on u.fx_id =b.fx_id  WHERE u.fx_id='{user_num}' and date_format( trade_end  , '%Y-%m-%d %H:%i:%s') >='{from_data}' order by `trade_end`  asc  ;"  
                        sql1=f'select *from Main where fx_id={user_num}'
                        sql2=f'select *from mt5_balance where fx_id={user_num}'
                        sql3=f"select *from DailyMoney where fx_id={user_num} and  date_format( datetime  , '%Y-%m-%d %H:%i:%s') >='{from_data}';"
                        sql4=f"select *from WeeklyMoney where fx_id={user_num} and  date_format( datetime  , '%Y-%m-%d %H:%i:%s') >='{from_data}';"
                    
                    else:
                        print('SE')
                        
                        # sql = f"SELECT * FROM  position_history m WHERE fx_id ={user_num} and updatetime >={now_time} ;"
                       
                        sql = f"select *from Main  as u inner join trade_history as b on u.fx_id =b.fx_id  WHERE u.fx_id='{user_num}' and date_format( trade_end  , '%Y-%m-%d %H:%i:%s') >='{from_data}'  order by `trade_end`  asc  ;"  
                        sql1=f'select *from Main where fx_id={user_num}'
                        sql2=f'select *from mt5_balance where fx_id={user_num}'
                        sql3=f"select *from DailyMoney where fx_id={user_num} and  date_format( datetime  , '%Y-%m-%d %H:%i:%s') >='{from_data}';"
                        sql4=f"select *from WeeklyMoney where fx_id={user_num} and  date_format( datetime  , '%Y-%m-%d %H:%i:%s') >='{from_data}';"
                        
                        
                        
                    cursor.execute(sql)
                    result=cursor.fetchall()
                    result=pd.DataFrame(result)
                   
                    print(len(result))
                    cursor.execute(sql1)
                    result1=cursor.fetchall()
                    result1=pd.DataFrame(result1) 
                    
                    
                    cursor.execute(sql2)
                    result2=cursor.fetchall()
                    result2=pd.DataFrame(result2) 
                    
                    cursor.execute(sql3)
                    result3=cursor.fetchall()
                    result3=pd.DataFrame(result3)

                    
                    cursor.execute(sql4)
                    result4=cursor.fetchall()
                    result4=pd.DataFrame(result4)
                    
                    
                    day_profit=0
                    week_profit=0
                    day_amount=0
                    week_amount=0
                    mt5_bal=0
                    if len(result3)>0:
                        
                        day_profit=round(sum(result3['profit'])/len(result3),2)
                        day_amount=round(sum(result3['daily_amount'])/len(result3),2)
                        print('dayprofit',day_profit,day_amount)
                        
                    if len(result3)>0:
                        
                        week_profit=round(sum(result4['profit'])/len(result4),2)
                        week_amount=round(sum(result4['weekly_amount'])/len(result4),2)
                        print('weekprofit',week_profit,week_amount)
                        
                    if len(result2)>0:
                        mt5_bal=result2['mt5_balance'].iloc[0]
                        
                    bal_dict={}
                    bal_dict['mt5_balance']=str(mt5_bal)
                    bal_dict['day_profit']=str(day_profit)
                    bal_dict['day_amount']=str(day_amount)
                    bal_dict['week_profit']=str(week_profit)
                    bal_dict['week_amount']=str(week_amount)
                   
                    
                        
                    start_time=result1['datetime'][0]  
                    
                    diffdays=datetime.now()-result1['datetime'][0]                  
                    days=diffdays.days
                    
                    
                    
                                    
                    if len(result)>0:
                        
                        print('동작')
                       
                        if conn:
                            with conn.cursor() as cursor:
                                
                                print('START',start_time)
                                    
                                sql=f"select *from trade_history where fx_id={user_num} and date_format( trade_end  , '%Y-%m-%d %H:%i:%s') >='{start_time}'"
                                cursor.execute(sql)
                                history=cursor.fetchall()
                                history=pd.DataFrame(history)
                                history_amount=sum(history['profit'])
                                
                               
                                aa=result['initial_amount'].iloc[0]
                                
                                
                                history_profit= round(Decimal(f'{history_amount}')/Decimal(f'{aa}')*100,3)
                                bal_dict['current_profit']=str(history_profit)
                                self.return_dict_data['results']['profit'].append(bal_dict)   
                        
                        # print("mew_data",new_data)
                        # print("mew_re",new_result)
                        
                        new_data=(datetime.now() - timedelta(days=datetime.today().weekday())).strftime('%Y-%m-%d')
                        print(new_data)
                        
                        new_result=result[result['trade_end']>=new_data]
                        
                        if len(new_result):
                            # print("mew_data",new_data)
                            print("mew_re",new_result)
                            
                            
                            new_result['day']=new_result['trade_end'].apply(lambda x : x.day)
                            new_result['month']=new_result['trade_end'].apply(lambda x : x.month)
                            month=new_result['month'].iloc[0]
                            for i in new_result.groupby('day'):
                                bal_dict={}
                                day=i[0]
                               
                                profit=0
                                baldata=i[1]
                                
                                profit=str(sum(baldata['profit']))
                            
                            
                                bal_dict['datetime']=f'{month}-{day}'
                                bal_dict['day_profit']=profit
                                
                                self.return_dict_data['results']['days'].append(bal_dict)   
                                
                        
                            
                        
                        # 시작금액
                        start_money=result['initial_amount'].iloc[0]
                        # 시작보증금
                        start_deposit=result['deposit'].iloc[0]
                        # 월수익
                        month_profit=sum(result['profit'])
                        
                        befor_balance=result['initial_amount'].iloc[0]
                        # 현재잔액
                        balance=Decimal(f'{befor_balance}') + Decimal(f'{month_profit}')
                        # 월수익률
                        month_roi=round((Decimal(f'{month_profit}')/Decimal(f'{befor_balance}'))*Decimal('100'),4) 
                        
                        if   month_profit>0:
                                           
                            deducted_amount=Decimal(f'{month_profit}') * Decimal(f'0.1')
                        else:
                            deducted_amount=0
                        
                        
                        recharge=deducted_amount
                        print(month_roi,month_profit,start_money,start_deposit,deducted_amount,recharge,days,balance)
                
                        new_dict['start_money']=str(start_money)
                        new_dict['start_deposit']=str(start_deposit)
                        new_dict['deducted_amount']=str(deducted_amount)
                        new_dict['recharge']=str(recharge)
                        new_dict['days']=str(days)
                        # new_dict['balance']=str(balance)
                        new_dict['month_profit']=str(month_profit)
                        new_dict['month_amount']=str(month_roi)
                        new_dict['round']=str(result['check'].iloc[0])
                        self.return_dict_data['results']['deposit'].append(new_dict)
                        # new_list.append(new_dict)
                        # print(new_list)
                        
                    else:
                        
                        
                        with conn.cursor() as cursor:
                        
                            if self.new_userbalance_chck(user_num):
                            
                            
                            
                                new_sql=f"SELECT * FROM balance where fx_id={user_num} ORDER BY `datetime` desc limit 1"
                            else:
                                new_sql=f"SELECT * FROM Main where fx_id={user_num};"
                                
                                
                            cursor.execute(new_sql)
                            result=cursor.fetchall()
                            result=pd.DataFrame(result)
                            
                            
                          
                            start_money=result['initial_amount'].iloc[0]
                            # 시작보증금
                            start_deposit=result['deposit'].iloc[0]
                            # 월수익
                            month_profit=0
                            
                            befor_balance=result['initial_amount'].iloc[0]
                            # 현재잔액
                            balance=Decimal(f'{befor_balance}') + Decimal(f'{month_profit}')
                            # 월수익률
                            month_roi=round((Decimal(f'{month_profit}')/Decimal(f'{befor_balance}'))*Decimal('100'),4) 
                            
                            if   month_profit>0:
                                            
                                deducted_amount=Decimal(f'{month_profit}') * Decimal(f'0.1')
                            else:
                                deducted_amount=0
                            
                            
                            recharge=deducted_amount
                            print(month_roi,month_profit,start_money,start_deposit,deducted_amount,recharge,days,balance)
                    
                            new_dict['start_money']=str(start_money)
                            new_dict['start_deposit']=str(start_deposit)
                            new_dict['deducted_amount']=str(deducted_amount)
                            new_dict['recharge']=str(recharge)
                            new_dict['days']=str(days)
                            new_dict['balance']=str(balance)
                            new_dict['month_profit']=str(month_profit)
                            new_dict['round']=str(result['check'].iloc[0])
                        
                            self.return_dict_data['results']['deposit'].append(new_dict)
                        
                # self.return_dict_data=dict(results=new_list)
                self.return_dict_data['reCode']=0
                self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                self.status_code=200
                
                return True
            
        
            else:
                self.return_dict_data['reCode']=1
                self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                self.status_code=423
                
                return False
        else:
            self.return_dict_data['reCode']=100
            self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
            self.status_code=423
            return False  
        
    
    
    
    
    def save_bot_log(self, bot: botmanagemodel) -> bool:
        conn = self._get_connection()
        
        aaa=datetime.strftime(self.now,"%Y-%m-%d %H:%M:%S")
        aaa1=datetime.strptime(aaa,"%Y-%m-%d %H:%M:%S")
        print(aaa1)
       
        try:
            if conn:
                with conn.cursor() as cursor:
                    sql = "INSERT INTO bot_manage (fx_id, retri_id, manage_id, status, datetime,check_status) VALUES " + \
                    f"({bot.fx_id},'{bot.retri_id}','{bot.manage_id}',{bot.status},'{aaa1}',{bot.check_status})"
                    cursor.execute(sql)

                    conn.commit()
                    cursor.close()
                
                return True

        except Exception as e:
            print(e)   
    
    
    def newsave_bot_log(self, bot: newbotmanagemodel) -> bool:
        conn = self._get_connection()
        
        aaa=datetime.strftime(self.now,"%Y-%m-%d %H:%M:%S")
        aaa1=datetime.strptime(aaa,"%Y-%m-%d %H:%M:%S")
        print(aaa1)
       
        try:
            if conn:
                with conn.cursor() as cursor:
                    sql = "INSERT INTO bot_manage ( retri_id, manage_id, status, datetime,check_status) VALUES " + \
                    f"('{bot.retri_id}','{bot.manage_id}',{bot.status},'{aaa1}',{bot.check_status})"
                    cursor.execute(sql)

                    conn.commit()
                    cursor.close()
                
                return True

        except Exception as e:
            print(e)   
            
    
    
    def manager_botmanage_list(self,user_num:str,from_data:str,to_data:str,select:int,page:int=1,size:int=5):
        
        conn = self._get_connection()
        check = MakeErrorType()
        
        self.return_dict_data=dict(page=0,size=0,totalPages=0,totalCount=0,results=[], reCode=1, message='Server Error')
        
        if size==0:
            
            self.return_dict_data['reCode']=107
            self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
            self.status_code=423
        new_size=(page*size)-size
        
        to_data=to_data+' 23'
        if self.new_find_user(user_num)==False: 
            
              
            # print(now_time)
            new_list=[]
            if conn:
                with conn.cursor() as cursor:
                    
                    if select==1:
                        # sql = f"SELECT * FROM  position_history m WHERE fx_id ={user_num} and updatetime >={now_time} ;"
                        sql = f"select *from  bot_manage WHERE retri_id ='{user_num}' and date_format( datetime  , '%Y-%m-%d %H:%i:%s') >='{from_data}' and date_format( datetime  , '%Y-%m-%d %H:%i:%s') <='{to_data}' and check_status=1 order by `datetime`  asc limit {new_size},{size};"
                        
                        sql1=f"select count(fx_id) from  bot_manage WHERE retri_id ='{user_num}' and date_format( datetime  , '%Y-%m-%d %H:%i:%s') >='{from_data}' and  date_format( datetime  , '%Y-%m-%d %H:%i:%s') <='{to_data}' and check_status=1  order by `datetime`  asc ;"
                    else:
                        sql = f"select *from  bot_manage WHERE retri_id ='{user_num}' and date_format( datetime  , '%Y-%m-%d %H:%i:%s') >='{from_data}' and  date_format( datetime  , '%Y-%m-%d %H:%i:%s') <='{to_data}' and check_status=2 order by `datetime`  asc limit {new_size},{size};"
                        
                        sql1=f"select count(fx_id) from  bot_manage WHERE retri_id ='{user_num}' and date_format( datetime  , '%Y-%m-%d %H:%i:%s') >='{from_data}' and  date_format( datetime  , '%Y-%m-%d %H:%i:%s') <='{to_data}' and check_status=2  order by `datetime`  asc ;"
                        
                    
                    cursor.execute(sql)
                    result=cursor.fetchall()
                    result=pd.DataFrame(result)
                    print("sdsds",result)
                    
                    
                    cursor.execute(sql1)
                    result1=cursor.fetchall()
                    result1=pd.DataFrame(result1)
                    
                    if result1['count(fx_id)'][0] >0:
                        
                        fx_id_count=result1['count(fx_id)'][0]
                    else:
                        fx_id_count=0
                        
                        
                    
                    self.return_dict_data['page']=int(page)
                    self.return_dict_data['size']=int(size)
                    self.return_dict_data['totalPages']=math.ceil((fx_id_count/size))
                    self.return_dict_data['totalCount']=int(fx_id_count)
                    
                    if len(result)>0:
                        for i in result.iterrows():
                            df_data=i[1]
                            new_dict={}
                            new_dict['datetime']=str(df_data['datetime'])
                            new_dict['status']=str(df_data['status'])
                            new_dict['check_status']=str(df_data['check_status'])
                            new_dict['manage_id']=str(df_data['manage_id'])
                            self.return_dict_data['results'].append(new_dict)
                            
                
                # self.return_dict_data=dict(results=new_list)
                self.return_dict_data['reCode']=0
                self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                self.status_code=200
                return True
            
            
            else:
                self.return_dict_data['reCode']=1
                self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                self.status_code=423
                return False
        else:
            self.return_dict_data['reCode']=100
            self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
            self.status_code=423
            return False 
    
    
    
    def user_botmanage_list(self,user_num:int,from_data:str,to_data:str,page:int,size:int):
        
        conn = self._get_connection()
        check = MakeErrorType()
        
        self.return_dict_data=dict(page=0,size=0,totalPages=0,totalCount=0,results=[], reCode=1, message='Server Error')
        
        if size==0:
            
            self.return_dict_data['reCode']=107
            self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
            self.status_code=423
        new_size=(page*size)-size
        
        to_data=to_data+' 23'
        if self.find_user(user_num)==False: 
            
              
            # print(now_time)
            new_list=[]
            if conn:
                with conn.cursor() as cursor:
                    # sql = f"SELECT * FROM  position_history m WHERE fx_id ={user_num} and updatetime >={now_time} ;"
                    sql = f"select *from  bot_manage WHERE fx_id ='{user_num}' and date_format( datetime  , '%Y-%m-%d %H:%i:%s') >='{from_data}' and date_format( datetime  , '%Y-%m-%d %H:%i:%s') <='{to_data}' order by `datetime`  asc limit {new_size},{size};"
                    
                    sql1=f"select count(fx_id) from  bot_manage WHERE fx_id ='{user_num}' and date_format( datetime  , '%Y-%m-%d %H:%i:%s') >='{from_data}' and date_format( datetime  , '%Y-%m-%d %H:%i:%s') <='{to_data}' order by `datetime`  asc ;"
                    
                    cursor.execute(sql)
                    result=cursor.fetchall()
                    result=pd.DataFrame(result)
                    print("sdsds",result)
                    
                    
                    cursor.execute(sql1)
                    result1=cursor.fetchall()
                    result1=pd.DataFrame(result1)
                    
                    print(result1)
                    if result1['count(fx_id)'][0] >0:
                        
                        fx_id_count=result1['count(fx_id)'][0]
                    else:
                        fx_id_count=0
                        
                        
                    
                    self.return_dict_data['page']=int(page)
                    self.return_dict_data['size']=int(size)
                    self.return_dict_data['totalPages']=math.ceil((fx_id_count/size))
                    self.return_dict_data['totalCount']=int(fx_id_count)
                    
                    if len(result)>0:
                        for i in result.iterrows():
                            df_data=i[1]
                            new_dict={}
                            new_dict['datetime']=str(df_data['datetime'])
                            new_dict['status']=str(df_data['status'])
                            new_dict['check_status']=str(df_data['check_status'])
                          
                            self.return_dict_data['results'].append(new_dict)
                            # new_list.append(new_dict)
                            
                
                # self.return_dict_data=dict(results=new_list)
                self.return_dict_data['reCode']=0
                self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                self.status_code=200
                return True
            
            
            else:
                self.return_dict_data['reCode']=1
                self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                self.status_code=423
                return False
        else:
            self.return_dict_data['reCode']=100
            self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
            self.status_code=423
            return False 
        
    
    
    def manage_maindash(self):
        conn = self._get_connection()
        check = MakeErrorType()
        
        now_time=(datetime.now(timezone('Asia/Seoul'))).strftime('%Y-%m-%d')
        now_time1=(datetime.now(timezone('Asia/Seoul'))).strftime('%Y-%m')
        print(now_time)
        new_list=[]
        if conn:
            with conn.cursor() as cursor:
                # sql = f"SELECT * FROM  position_history m WHERE fx_id ={user_num} and updatetime >={now_time} ;"
                sql = f"SELECT * FROM Main;"
                
                sql1 = f"SELECT * FROM DailyMoney;"
                
                cursor.execute(sql)
                result=cursor.fetchall()
                result=pd.DataFrame(result)
                # print("1",result)
                
                
                cursor.execute(sql1)
                result1=cursor.fetchall()
                result1=pd.DataFrame(result1)
                
                month=result1[result1['datetime']>=now_time1]
                
                if len(month) >0:
                    
                    month_amount=round(sum(month['daily_amount']),2)
                    month_profit=round(sum(month['profit']),2)
                else:
                    month_amount=0
                    month_profit=0
                    
                total_amount=round(sum(result1['daily_amount']),2)
                
                total_profit=round(sum(result1['profit']),2)
                
                
                new_dict={}
                if len(result) >0:
                    
                    total_user=len(result)
                    # print("2",total_user)
                    
                    new_df=result[result['datetime']>=now_time]
                    # print("3",new_df)
                    if len(new_df)>0:
                        
                        day_user=len(new_df)
                    else:
                        day_user=0
                    
                    
                    new_df1=result[result['check_status']==1]
                    if len(new_df1)>0:
                        variance_user=len(new_df1)
                    else:
                        variance_user=0
                        
                
                
                new_dict['total_user']=str(total_user)
                new_dict['day_user']=str(day_user)
                new_dict['variance_user']=str(variance_user)  
                new_dict['total_amount']=str(total_amount)
                new_dict['total_profit']=str(total_profit)
                new_dict['month_amount']=str(month_amount)
                new_dict['month_profit']=str(month_profit)
                new_list.append(new_dict)
                # if len(result) >0:
                    
                #     for i in result.iterrows():
                #         df_data=i[1]
                #         new_dict={}
                #         new_dict['datetime']=str(df_data['datetime'])
                #         new_dict['status']=str(df_data['status'])
                #         new_dict['check_status']=str(df_data['check_status'])
                        
                #         # self.return_dict_data['results'].append(new_dict)
                #         new_list.append(new_dict)
                    
                print(new_list)
                # print(result) 
            # new_list.append(new_dict)    
            self.return_dict_data=dict(results=new_list)
            self.return_dict_data['reCode']=0
            self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
            self.status_code=200
            
        return True
    
    
    
    
    def user_position_count(self,user_num:int):
        
        conn = self._get_connection()
        check = MakeErrorType()
        now_minut=(datetime.now(timezone('Asia/Seoul'))).strftime('%M')
        
        if int(now_minut) >= 12:


            now_time=(datetime.now(timezone('Asia/Seoul'))).strftime('%Y-%m-%d %H')
            print(now_time)
            # now_time1=(datetime.now(timezone('Asia/Seoul'))-timedelta(hours=1)).strftime('%Y-%m-%d %H')
            
        else:

            now_time=(datetime.now(timezone('Asia/Seoul'))-timedelta(hours=1)).strftime('%Y-%m-%d %H')
        if self.find_user(user_num)==False: 
            
              
            # print(now_time)
            new_list=[]
            if conn:
                with conn.cursor() as cursor:
                   
                    # sql = f"SELECT * FROM  position_history m WHERE fx_id ={user_num} and updatetime >={now_time} ;"
                    sql = f"SELECT * FROM position_history ph  WHERE fx_id={user_num} and date_format( updatetime  , '%Y-%m-%d %H:%i:%s') >='{now_time}' order by `datetime`  asc ;"
                        
                    cursor.execute(sql)
                    result=cursor.fetchall()
                    result=pd.DataFrame(result)
                    print("sdsds",result)
                    if len(result)>0:
                        new_dict={}
                        new_dict['position_count']=str(len(result))    
                        new_list.append(new_dict)
                
                self.return_dict_data=dict(results=new_list)
                self.return_dict_data['reCode']=0
                self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                self.status_code=200
                return True
            
            
            else:
                self.return_dict_data['reCode']=1
                self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                self.status_code=423
                return False
        else:
            self.return_dict_data['reCode']=100
            self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
            self.status_code=423
            return False
    
    
    
    def manage_position_count(self,user_num:int):
        
        conn = self._get_connection()
        check = MakeErrorType()
        now_minut=(datetime.now(timezone('Asia/Seoul'))).strftime('%M')
        
        if int(now_minut) >= 12:


            now_time=(datetime.now(timezone('Asia/Seoul'))).strftime('%Y-%m-%d %H')
            print(now_time)
            # now_time1=(datetime.now(timezone('Asia/Seoul'))-timedelta(hours=1)).strftime('%Y-%m-%d %H')
            
        else:

            now_time=(datetime.now(timezone('Asia/Seoul'))-timedelta(hours=1)).strftime('%Y-%m-%d %H')
        if self.new_find_user(user_num)==False: 
            
              
            # print(now_time)
            new_list=[]
            if conn:
                with conn.cursor() as cursor:
                   
                    # sql = f"SELECT * FROM  position_history m WHERE fx_id ={user_num} and updatetime >={now_time} ;"
                    sql = f"select *from Main  as u inner join position_history  as b on u.fx_id =b.fx_id  WHERE u.retri_id ='{user_num}' and date_format( updatetime  , '%Y-%m-%d %H:%i:%s') >='{now_time}' order by `updatetime`  asc ;"
                        
                    cursor.execute(sql)
                    result=cursor.fetchall()
                    result=pd.DataFrame(result)
                    print("sdsds",result)
                    if len(result)>0:
                        new_dict={}
                        new_dict['position_count']=str(len(result))    
                        new_list.append(new_dict)
                
                self.return_dict_data=dict(results=new_list)
                self.return_dict_data['reCode']=0
                self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                self.status_code=200
                return True
            
            
            else:
                self.return_dict_data['reCode']=1
                self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                self.status_code=423
                return False
        else:
            self.return_dict_data['reCode']=100
            self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
            self.status_code=423
            return False   
        
    
    
    def manager_deposit_list(self,user_num:str):
        
        conn = self._get_connection()
        check = MakeErrorType()
       
        if self.new_find_user(user_num)==False: 
            
              
            # print(now_time)
            new_list=[]
            if conn:
                with conn.cursor() as cursor:
                    
                    
                    if self.new_balance_chck(user_num):
                        
                        # sql = f"SELECT * FROM  position_history m WHERE fx_id ={user_num} and updatetime >={now_time} ;"
                        # print("동작")
                        sql = f"SELECT * FROM balance where retri_id='{user_num}' ORDER BY `datetime` desc limit 1;"
                    
                    else:
                        # print("비동작")
                        sql = f"select *from Main where retri_id='{user_num}';"  
                    
                    cursor.execute(sql)
                    result=cursor.fetchall()
                    result=pd.DataFrame(result)
                    # print("sdsds",result)
                    
                    new_dict={}
                    if len(result)>0:
                        new_dict['round']=str(result['check'].iloc[0])
                        new_dict['deposit']=str(result['deposit'].iloc[0])
                        new_list.append(new_dict)
                    # if len(result)>0:
                    #     for i in result.iterrows():
                    #         df_data=i[1]
                    #         new_dict={}
                    #         new_dict['fx_id']=df_data['fx_id']
                    #         new_dict['retri_id']=df_data['retri_id']
                    #         new_dict['symbol']=df_data['symbol']
                    #         new_dict['sl']=df_data['sl']
                    #         new_dict['tp']=df_data['tp']
                    #         new_dict['profit']=df_data['profit']
                    #         new_dict['price_open']=df_data['price_open']
                    #         new_dict['price_current']=df_data['price_current']
                    #         new_dict['type']=df_data['type']
                    #         new_dict['volume']=df_data['volume']
                    #         new_dict['position_id']=str(df_data['position_id'])
                    #         new_dict['datetime']=str(df_data['b.datetime'])
                    #         self.return_dict_data['results'].append(new_dict)
                            # new_list.append(new_dict)
                                
                
                self.return_dict_data=dict(results=new_list)
                self.return_dict_data['reCode']=0
                self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                self.status_code=200
                return True
            
            
            else:
                self.return_dict_data['reCode']=1
                self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
                self.status_code=423
                return False
        else:
            self.return_dict_data['reCode']=100
            self.return_dict_data['message'] = check.error(self.return_dict_data['reCode'])
            self.status_code=423
            return False
        
        
    # # Front KEY 복호화 함수
    # def front_decrypt_data(self, encrypted_data: bytes) -> str:
    #     try:
    #         key = b"blocksquareretri"  # 암호화에 사용된 동일한 키
    #         iv = b"retriblocksquare"  # 암호화에 사용된 동일한 IV

    #         cipher = AES.new(key, AES.MODE_CBC, iv)
    #         decrypted_data = unpad(cipher.decrypt(b64decode(encrypted_data)), AES.block_size).decode()
    #     except Exception as e:
    #         print(e)
    #         decrypted_data = ''
        
    #     return decrypted_data

# # # # # a.get_test()
# aa=a.encrypt_data('Cmcmc9208!')
# print(aa)
# # a.get_udate(80286327)
# print(a.decrypt_data())
# a.save_user_log(UserMdel(id=8083,pas='asdasdadas',account='8000000',accountpas='asdasdasdasds',

#     server='asdadsadsa',
#     retri='adasdsadsa',
#     amount=0.0,
#     deposit=0.0

# # ))
# a=MySQLAdapter()
# a.save_bot_log(botmanagemodel(fx_id=80286327,retri_id='retri1',status=1))
# a.manager_position_list('retri1')
# a.get_trade_count()
# print(a.new_find_user('retri1'))

# a.trade_list(80310443,'','',0)
# a.manager_trade_list(8086327,'','',3,0)
# a.connet_list(select=1)
# a.connet_list(select=0)