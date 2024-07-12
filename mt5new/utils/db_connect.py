
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
                    
                    if len(result):
                        # print(result)
                        new_list=[]
                        new_dict={}
                        buy=len(result[result['position']=='buy'])
                        sell=len(result[result['position']=='sell'])
                        total=buy+sell
                        new_dict['buy']=buy
                        new_dict['sell']=sell
                        new_dict['total']=total
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

    def position_list(self,user_num:int):
        
        conn = self._get_connection()
        check = MakeErrorType()
        now_minut=(datetime.now(timezone('Asia/Seoul'))).strftime('%M')
        
        if int(now_minut) > 4:


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

            
    def trade_list(self,user_num:int,from_data:str,to_data:str,select:int):
        print(from_data,to_data,select,user_num)
        conn = self._get_connection()
        check = MakeErrorType()
        self.return_dict_data=dict(results={'history':[],'balance':[]}, reCode=1, message='Server Error')
        
            
            
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
                        sql= f"select *from (SELECT * FROM balance where fx_id='{user_num}' ORDER BY `datetime` desc limit 1)  as u inner join trade_history as b on u.fx_id =b.fx_id  WHERE u.fx_id={user_num} and date_format( trade_end  , '%Y-%m-%d %H:%i:%s') >='{from_data}' and date_format( trade_end  , '%Y-%m-%d %H:%i:%s')<='{to_data}' order by `trade_end`  asc;"
                        
                    # sql = f"SELECT * FROM  position_history m WHERE fx_id ={user_num} and updatetime >={now_time} ;"
                    # sql = f"SELECT * FROM trade_history ph  WHERE fx_id={user_num} and date_format( trade_end  , '%Y-%m-%d %H:%i:%s') >='{from_data}' and date_format( trade_end  , '%Y-%m-%d %H:%i:%s')<='{to_data}' order by `trade_end`  asc  ;"  
                    else:
                        sql= f"select *from Main  as u inner join trade_history as b on u.fx_id =b.fx_id  WHERE u.fx_id={user_num} and date_format( trade_end  , '%Y-%m-%d %H:%i:%s') >='{from_data}' and date_format( trade_end  , '%Y-%m-%d %H:%i:%s')<='{to_data}' order by `trade_end`  asc;"
                    
                    cursor.execute(sql)
                    result=cursor.fetchall()
                    result=pd.DataFrame(result)
                    
                    print(len(result))
                    
                    if len(result)>0:
                        profit=sum(result['profit'])
                        befor_balance=result['initial_amount'].iloc[0]
                        balance=Decimal(f'{befor_balance}') + Decimal(f'{profit}')
                        roi=round((Decimal(f'{profit}')/Decimal(f'{befor_balance}'))*Decimal('100'),4)
                        print(len(result))
                        
                        bal_dict={}
                        bal_dict['balance']=str(balance)
                        bal_dict['initial_amount']=str(befor_balance)
                        bal_dict['total profit']=str(profit)
                        bal_dict['roi']=str(roi)
                        bal_dict['datetime']=str(result['datetime'].iloc[0])
                        self.return_dict_data['results']['balance'].append(bal_dict)
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
                            self.return_dict_data['results']['history'].append(new_dict)
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
        
    def connet_list(self,from_data,to_data,select,user_num,trade):
        
        print(from_data,to_data,select,user_num)
        conn = self._get_connection()
        check = MakeErrorType()
        
        
        

       
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
                        
                        sql = f"SELECT * FROM Main  WHERE  date_format( datetime  , '%Y-%m-%d %H:%i:%s') >='{from_data}' and date_format( datetime  , '%Y-%m-%d %H:%i:%s')<='{to_data}' and status>={trade} order by `datetime`  asc;"  
                    else:
                        sql = f"SELECT * FROM Main  WHERE  date_format( datetime  , '%Y-%m-%d %H:%i:%s') >='{from_data}' and date_format( datetime  , '%Y-%m-%d %H:%i:%s')<='{to_data}' and status={trade} order by `datetime`  asc;"
                        
                    
                    cursor.execute(sql)
                    result=cursor.fetchall()
                    result=pd.DataFrame(result)
                    print(result)
                    
                    
                    if len(result)>0:
                        for i in result.iterrows():
                            df_data=i[1]
                            new_dict={}
                            new_dict['retri_id']=df_data['retri_id']
                            new_dict['initial_amount']=str(df_data['initial_amount'])
                            new_dict['deposit']=str(df_data['deposit'])
                            new_dict['datetime']=str(df_data['datetime'])
                            new_dict['connet']=df_data['status']
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
                            new_dict['retri_id']=df_data['retri_id']
                            new_dict['initial_amount']=str(df_data['initial_amount'])
                            new_dict['deposit']=str(df_data['deposit'])
                            new_dict['datetime']=str(df_data['datetime'])
                            new_dict['connet']=df_data['status']
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
    
    
    def manager_trade_list(self,user_num:str,from_data:str,to_data:str,select,trade):
        print(select)
        conn = self._get_connection()
        check = MakeErrorType()
        self.return_dict_data=dict(results={'history':[],'balance':[]}, reCode=1, message='Server Error')
        
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
                            sql = f"select *from (SELECT * FROM balance where retri_id='{user_num}' ORDER BY `datetime` desc limit 1)  as u inner join trade_history as b on u.fx_id =b.fx_id  WHERE u.retri_id='{user_num}' and date_format( trade_end  , '%Y-%m-%d %H:%i:%s') >='{from_data}' and date_format( trade_end  , '%Y-%m-%d %H:%i:%s')<='{to_data}' order by `trade_end`  asc  ;"  
                        elif trade==1:
                            sql = f"select *from (SELECT * FROM balance where retri_id='{user_num}' ORDER BY `datetime` desc limit 1)  as u inner join trade_history as b on u.fx_id =b.fx_id  WHERE u.retri_id='{user_num}' and date_format( trade_end  , '%Y-%m-%d %H:%i:%s') >='{from_data}' and date_format( trade_end  , '%Y-%m-%d %H:%i:%s')<='{to_data}'and position='buy'order by `trade_end`  asc ;"
                        elif trade==2:
                            sql = f"select *from (SELECT * FROM balance where retri_id='{user_num}' ORDER BY `datetime` desc limit 1)  as u inner join trade_history as b on u.fx_id =b.fx_id  WHERE u.retri_id='{user_num}' and date_format( trade_end  , '%Y-%m-%d %H:%i:%s') >='{from_data}' and date_format( trade_end  , '%Y-%m-%d %H:%i:%s')<='{to_data}'and position='sell' order by `trade_end`  asc;"
                    
                    
                    else:
                        
                        # sql = f"SELECT * FROM  position_history m WHERE fx_id ={user_num} and updatetime >={now_time} ;"
                        if trade==0:
                            sql = f"select *from Main  as u inner join trade_history as b on u.fx_id =b.fx_id  WHERE u.retri_id='{user_num}' and date_format( trade_end  , '%Y-%m-%d %H:%i:%s') >='{from_data}' and date_format( trade_end  , '%Y-%m-%d %H:%i:%s')<='{to_data}' order by `trade_end`  asc  ;"  
                        elif trade==1:
                            sql = f"select *from Main  as u inner join trade_history as b on u.fx_id =b.fx_id  WHERE u.retri_id='{user_num}' and date_format( trade_end  , '%Y-%m-%d %H:%i:%s') >='{from_data}' and date_format( trade_end  , '%Y-%m-%d %H:%i:%s')<='{to_data}'and position='buy'order by `trade_end`  asc ;"
                        elif trade==2:
                            sql = f"select *from Main  as u inner join trade_history as b on u.fx_id =b.fx_id  WHERE u.retri_id='{user_num}' and date_format( trade_end  , '%Y-%m-%d %H:%i:%s') >='{from_data}' and date_format( trade_end  , '%Y-%m-%d %H:%i:%s')<='{to_data}'and position='sell' order by `trade_end`  asc;"
                        
                    cursor.execute(sql)
                    result=cursor.fetchall()
                    result=pd.DataFrame(result)
                    print(result)
                  
                    
                    if len(result)>0:
                        
                        profit=sum(result['profit'])
                        befor_balance=result['initial_amount'].iloc[0]
                        balance=Decimal(f'{befor_balance}') + Decimal(f'{profit}')
                        roi=round((Decimal(f'{profit}')/Decimal(f'{befor_balance}'))*Decimal('100'),4)
                        print(len(result))
                        bal_dict={}
                        bal_dict['balance']=str(balance)
                        bal_dict['initial_amount']=str(befor_balance)
                        bal_dict['total profit']=str(profit)
                        bal_dict['roi']=str(roi)
                        bal_dict['datetime']=str(result['datetime'].iloc[0])
                        self.return_dict_data['results']['balance'].append(bal_dict)
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
    
    def manager_position_list(self,user_num:str):
        
        conn = self._get_connection()
        check = MakeErrorType()
        now_minut=(datetime.now(timezone('Asia/Seoul'))).strftime('%M')
        
        if int(now_minut) > 4:


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
                    sql = f"select *from Main  as u inner join position_history  as b on u.fx_id =b.fx_id  WHERE u.retri_id ='{user_num}' and date_format( updatetime  , '%Y-%m-%d %H:%i:%s') >='{now_time}' order by `updatetime`  asc;"
                    
                    cursor.execute(sql)
                    result=cursor.fetchall()
                    result=pd.DataFrame(result)
                    print("sdsds",result)
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
    
    
    
    
    
    def dashboard_list(self,user_num:int):
       
        conn = self._get_connection()
        check = MakeErrorType()
        
        self.return_dict_data=dict(results={'deposit':[],'profit':[]}, reCode=1, message='Server Error')
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
                    
                    else:
                        print('SE')
                        
                        # sql = f"SELECT * FROM  position_history m WHERE fx_id ={user_num} and updatetime >={now_time} ;"
                       
                        sql = f"select *from Main  as u inner join trade_history as b on u.fx_id =b.fx_id  WHERE u.fx_id='{user_num}' and date_format( trade_end  , '%Y-%m-%d %H:%i:%s') >='{from_data}'  order by `trade_end`  asc  ;"  
                        sql1=f'select *from Main where fx_id={user_num}'
                    cursor.execute(sql)
                    result=cursor.fetchall()
                    result=pd.DataFrame(result)
                   
                    # print(result)
                    cursor.execute(sql1)
                    result1=cursor.fetchall()
                    result1=pd.DataFrame(result1) 
                    
                    print(result)
                    
                    print(result1)                 
                    diffdays=datetime.now()-result1['datetime'][0]                  
                    days=diffdays.days
                    if len(result)>0:
                        
                 
                       
                        
                        new_data=(datetime.now() - timedelta(days=datetime.today().weekday())).strftime('%Y-%m-%d')
                        new_result=result[result['trade_end']>=new_data]
                        # print("mew_data",new_data)
                        # print("mew_re",new_result)
                        
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
                            
                            self.return_dict_data['results']['profit'].append(bal_dict)    
                            
                        
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
                        new_dict['balance']=str(balance)
                        new_dict['month_profit']=str(month_profit)
                        
                        self.return_dict_data['results']['deposit'].append(new_dict)
                        # new_list.append(new_dict)
                        # print(new_list)
                        
                    else:
                        
                        print('asdsad')
                        with conn.cursor() as cursor:
                        
                            if self.new_userbalance_chck(user_num):
                            
                            
                            
                                new_sql=f"SELECT * FROM balance where fx_id={user_num} ORDER BY `datetime` desc limit 1"
                            else:
                                new_sql=f"SELECT * FROM Main where fx_id={user_num};"
                                
                                
                            cursor.execute(new_sql)
                            result=cursor.fetchall()
                            result=pd.DataFrame(result)
                            
                            
                            print(result)
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
# a.manager_position_list('retri1')
# a.get_trade_count()
# print(a.new_find_user('retri1'))

# a.trade_list(80310443,'','',0)
# a.manager_trade_list(8086327,'','',3,0)
# a.connet_list(select=1)
# a.connet_list(select=0)