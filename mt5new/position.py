import MetaTrader5 as mt5
import pandas as pd
from datetime import datetime,timedelta
import calendar
import pymysql.cursors
from pymysql.connections import Connection
from base64 import b64decode
from starlette.config import Config
from boto3 import client
from sqlalchemy import create_engine
from pytz import timezone

config = Config(".env")
AWS_KMS_KEY_ID = config.get('AWS_KMS_KEY_ID')

user_dict={'id':[],'pas':[],'server':[]}

def encrypt_data( data: str) -> bytes:
    KMS_CLIENT= client("kms", region_name='ap-northeast-2')
    response = KMS_CLIENT.encrypt(KeyId=AWS_KMS_KEY_ID,Plaintext=data.encode())
    
    encrypted_data = response["CiphertextBlob"]

    return encrypted_data

    # KEY 복호화 함수
def decrypt_data( encrypted_data: bytes) -> str:
    KMS_CLIENT= client("kms", region_name='ap-northeast-2')
    response = KMS_CLIENT.decrypt(KeyId=AWS_KMS_KEY_ID,CiphertextBlob=encrypted_data)
    decrypted_data = response["Plaintext"].decode()

    return decrypted_data


class Adapter:
    def __init__(self) -> None:

        # self.KMS_CLIENT= client("kms", region_name='ap-northeast-2')
        # self.exchange_id = 3
        # self.now = datetime.now(timezone('Asia/Seoul'))
        self.return_dict_data=dict(results={})
        self.status_code=200
        
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
    
    def get_user(self):
        conn= self._get_connection()
        try:
            
            sql = f"""
            SELECT * FROM Main m WHERE status =2;
            """
            with conn.cursor() as cursor:
                cursor.execute(sql)

                    
                data=cursor.fetchall()
                data=pd.DataFrame(data)
              
        except Exception as e :
            pass

        for i in data.iterrows():
            user_data=i[1]
            
            user_dict['id'].append(user_data['fx_id'])
            user_dict['pas'].append(decrypt_data(user_data['password']))
            user_dict['server'].append(user_data['server'])


        df_user_dict=pd.DataFrame(user_dict)
        
        return df_user_dict
    
    def find_user(self, user_num: str) -> int:
        return_num = 0
        conn = self._get_connection()

        try:
            if conn:
                with conn.cursor() as cursor:
                    sql = f"SELECT * FROM position_history WHERE position_id={user_num}"
                    cursor.execute(sql)
                    result = cursor.fetchall()
                    cursor.close()
                    print(result)
                    if len(result)>0:
                        
                        return False
                    
                    
                    else: 
                        
                        return True
                    
                        
        except Exception as e:
            pass


now_time=datetime.now(timezone('UTC'))
print(now_time)

def get_date(y, m, d):
  '''y: year(4 digits)
   m: month(2 digits)
   d: day(2 digits'''
  s = f'{y:04d}-{m:02d}-{d:02d}'
  return datetime.strptime(s, '%Y-%m-%d')

def get_week_no(y, m, d):
    target = get_date(y, m, d)
    firstday = target.replace(day=1)
    if firstday.weekday() == 6:
        origin = firstday
    elif firstday.weekday() < 3:
        origin = firstday - timedelta(days=firstday.weekday() + 1)
    else:
        origin = firstday + timedelta(days=6-firstday.weekday())
    return (target - origin).days // 7 + 1

def get_month_dates(year, month):
    # 특정 월의 첫 번째 날과 마지막 날의 요일과 날짜 수 구하기
    first_day, last_day = calendar.monthrange(year, month)
    # 시작 날짜는 첫 번째 날
    start_date = 1
    # 마지막 날짜는 마지막 날
    end_date = last_day
    return start_date, end_date


year=now_time.year
month=now_time.month
day=now_time.day
start_date, end_date = get_month_dates(year, month)

aa=get_week_no(year,month,end_date)


bb=get_week_no(year,month,day)
print(aa,bb)
if bb==aa and   now_time.month==3:
    
    new_hours=3
    
elif now_time.month>=4  and now_time.month <10 and bb!=aa:
    new_hours=3


elif bb==aa and   now_time.month==10:
    new_hours=2


else:
    
    new_hours=2
    
# db = pymysql.connect(
#             host=config.get('HOST'),
#             user=config.get('USER'),
#             password=config.get('PASSWORD'),
#             db=config.get('DBNAME'),
#             charset='utf8',
#             port=3306           
#             )

# try:
                
#     engine = create_engine(
#     f"""mysql+pymysql://{config.get('USER')}:{config.get('PASSWORD')}@{config.get('HOST')}/{config.get('DBNAME')}"""
#     )


# except pymysql.MySQLError as e:
#     print(e)
# try:
#     conn = engine.connect()
#     sql = f"""
#     SELECT * FROM Main m WHERE status =2;
#     """
#     with conn as con:
#         data=pd.read_sql(sql,con )
    
# except Exception as e :
#     pass
a=Adapter()
df_user_dict=a.get_user()
# for i in data.iterrows():
#     user_data=i[1]
    
#     user_dict['id'].append(user_data['fx_id'])
#     user_dict['pas'].append(decrypt_data(user_data['password']))
#     user_dict['server'].append(user_data['server'])


# df_user_dict=pd.DataFrame(user_dict)

# a = 80286327  # 실제 계좌 번호로 변경
# b = "Asd237525!"  # 실제 비밀번호로 변경
# c = "FPMarketsSC-Live"  # 브로커 서버 이름으로 변경
# # MetaTrader 5 터미널과의 연결 설정

for i in df_user_dict.iterrows():
    df_data=i[1]

    if not mt5.initialize(login=df_data['id'],password=df_data['pas'],server=df_data['server']):
        print("initialize() 실패, 오류 코드 =",mt5.last_error())
        quit()


    # USDCHF에서의 오픈 포지션 가져오기
    positions=mt5.positions_get()
    if positions==None:
        print("USDCHF에 포지션 없음, 오류 코드={}".format(mt5.last_error()))
    elif len(positions)>0:
        print("USDCHF에서의 총 포지션 =",len(positions))
        # 모든 오픈 포지션 표시
        for position in positions:
            
            datetimeobj = datetime.fromtimestamp(position.time)
            datetimeobj=datetimeobj-timedelta(hours=new_hours)
            print('newtime',datetimeobj)
            connection = Connection(host=config.get('HOST'),
                                    user=config.get('USER'),
                                    password=config.get('PASSWORD'),
                                    database=config.get('DBNAME'),
                                    cursorclass=pymysql.cursors.DictCursor)
            conn = connection
            now = datetime.now(timezone('Asia/Seoul'))
            aaa=datetime.strftime(now,"%Y-%m-%d %H:%M:%S")
            aaa1=datetime.strptime(aaa,"%Y-%m-%d %H:%M:%S")
            if a.find_user(position.ticket):
                
                try:
                    if conn:
                        with conn.cursor() as cursor:

                            sql = "INSERT INTO position_history (fx_id,symbol,sl,tp,profit,price_open,price_current,type,volume,position_id,datetime,updatetime) VALUES " + \
                            f"({df_data['id']},'{position.symbol}','{position.sl}','{position.tp}','{position.profit}','{position.price_open}','{position.price_current}','{position.type}','{position.volume}',{position.ticket},'{datetimeobj}','{aaa1}')"
                            cursor.execute(sql)

                            conn.commit()
                            cursor.close()            
                        

                except Exception as e:
                    print(e)   
            
            else:
                
                with conn.cursor() as cur:
                
            
                    sql = """UPDATE position_history set sl=%s,tp=%s,profit=%s,price_open=%s,price_current=%s,volume=%s,updatetime=%s where position_id=%s  """
                    
                    data1=(
                        position.sl, 
                        position.tp,
                        round(position.profit,2),
                        position.price_open,
                        position.price_current,
                        position.volume,
                        aaa1,
                        position.ticket
                        )
                    cur.execute(sql,data1)
                    
                conn.commit()  
                    # print("업데이트 데이터",data)  
                        
        # MetaTrader 5 터미널 연결 종료
        mt5.shutdown()


now_data=datetime.now(timezone('Asia/Seoul'))
now_data=now_data.strftime('%Y-%m-%d %H:%M:%S')
with open("C:/Users/user/Desktop/mt5new/logs/log.txt","a") as f:
    f.write(f"{now_data}\n")