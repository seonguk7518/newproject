from starlette.config import Config
from sqlalchemy import create_engine, MetaData, Table

# 환경 변수 파일 관리
config = Config(".env")

# 데이터베이스 접속 엔진 생성
sql_view = f"mysql+mysqldb://{config.get('DB_USER')}:{config.get('DB_PASSWORD')}@" + \
           f"{config.get('DB_HOST')}:{config.get('DB_PORT')}/{config.get('DB_NAME')}"
engine = create_engine(sql_view, echo=False)

# MetaData 생성
metaData = MetaData(bind=engine)
metaData.reflect()

# 필요한 테이블 정의
monthly_money_table = Table('MonthlyMoney', metaData, autoload_with=engine)
main_table = Table('Main', metaData, autoload_with=engine)
balance_table = Table('balance', metaData, autoload_with=engine)
daily_money_table = Table('DailyMoney', metaData, autoload_with=engine)
