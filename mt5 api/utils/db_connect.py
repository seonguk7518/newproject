
import os
import sys
import pymysql.cursors
from pymysql.connections import Connection
from starlette.config import Config
from datetime import datetime
from pytz import timezone
from boto3 import client
from base64 import b64decode
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

from base64 import b64decode
from models import *




# 환경 변수 파일 관리
config = Config(".env")
# AWS_KMS_KEY_ID = config.get('AWS_KMS_KEY_ID')
AWS_KMS_KEY_ID = config.get('AWS_KMS_KEY_ID')

class MySQLAdapter:
    def __init__(self) -> None:

        self.KMS_CLIENT= client("kms", region_name='ap-northeast-2')
        self.exchange_id = 3
        self.now = datetime.now(timezone('Asia/Seoul'))
        self.return_dict_data=dict(results={})
        self.status_code=200
        
    # DB Connection 확인
    def _get_connection(self):
        try:
            connection = Connection(host=config.get('HOST'),
                                    user=config.get('USERNAME'),
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

                        
        except Exception as e:
            pass
    #         # DB 연결 Error인 경우 TXT 저장
    #         # message = f"Bybit DB Find User Error : {user_num} / {e}\n"
    #         # with open(f"logs/db_error.log", "a") as f:
    #         #     f.write(message)

        return return_num

    
        
    

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
        
        new_list=[]
        if conn:
            with conn.cursor() as cursor:
                sql = f"SELECT * FROM  Main m WHERE status ={user_num};"
                cursor.execute(sql)
                result=cursor.fetchall()
                
                if len(result)>0:
                    for i in result:
                        new_dict={}
                        new_dict['fx_id']=i['fx_id']
                        new_list.append(new_dict)
            print(new_list)
            self.return_dict_data=dict(results=new_list)
            self.return_dict_data['reCode']=0
            self.status_code=200

            return True
        else:
            self.return_dict_data['reCode']=1
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
        conn = self._get_connection()
        if conn:
            with conn.cursor() as cursor:
                sql = f"UPDATE Main set status=2 WHERE fx_id ={user_num};"
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




    def get_approve(self) -> bool:
        conn = self._get_connection()

        try:
            if conn:
               
                
                with conn.cursor() as cursor:
                    user_sql = f"SELECT * FROM `Main` WHERE status='1'" 

                    cursor.execute(user_sql)

                    result=cursor.fetchall()
                    print(result)
                    new_dict={}
                    id=result[0]['fx_id']
                    initial_amount=result[0]['initial_amount']
                    deposit=result[0]['deposit']
                    new_dict['fx_id']=int(id)
                    new_dict['initial_amount']=str(initial_amount)
                    new_dict['deposit']=str(deposit)
                    self.return_dict_data['reCode']=0
                    self.return_dict_data['results']=new_dict
                   
                return True
            else:
                self.return_dict_data['reCode']=1
                return False
        except Exception as e:
        
            print(e)

    


    # Save Trade Log
    def save_user_log(self, trade: UserMdel) -> bool:
        conn = self._get_connection()
        
        aaa=datetime.strftime(self.now,"%Y-%m-%d %H:%M:%S")
        aaa1=datetime.strptime(aaa,"%Y-%m-%d %H:%M:%S")
        
        pas=self.encrypt_data(trade.pas)
        account=self.encrypt_data(trade.account)
        accountpas=self.encrypt_data(trade.accountpas)
        
        try:
            if conn:
                with conn.cursor() as cursor:
                    sql = "INSERT INTO Main (fx_id,password,account,accountpas,server,retri_id,initial_amount,deposit,datetime,status) VALUES " + \
                    f"({trade.id},%s,%s,%s,'{trade.server}','{trade.retri}',{trade.amount},{trade.deposit},'{aaa1}',1)"
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
a=MySQLAdapter()
# # # a.get_test()
# aa=a.encrypt_data('Cmcmc9208!')
# print(aa)
# a.get_udate(80286327)
# print(a.decrypt_data(aa))
# a.save_user_log(UserMdel(id=8083,pas='asdasdadas',account='8000000',accountpas='asdasdasdasds',

#     server='asdadsadsa',
#     retri='adasdsadsa',
#     amount=0.0,
#     deposit=0.0

# ))
a.get_link_list(2)