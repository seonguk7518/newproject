from starlette.config import Config
from sqlalchemy import create_engine, Table
from sqlalchemy.ext.declarative import declarative_base


# 환경 변수 파일 관리
config = Config(".env")

# Table 모델 선언
metaData = declarative_base().metadata

# 데이터베이스 접속 엔진 생성
sql_view = f"mysql+mysqldb://{config.get('user_name')}:{config.get('password')}@" + \
    f"{config.get('host')}:{config.get('port')}/{config.get('dbname')}"

engine = create_engine(sql_view, echo=False)
retri_table = Table('retri', metaData, autoload_with=engine)
signal_data =  Table('signal_data', metaData, autoload_with=engine)