# Rising-X API

> 

## 1. 가상환경 설정 후 실행

```sh
# Windows 가상 환경 생성 및 실행 (Virtualenv 활용) - 다른 가상 환경 사용 가능
User $ pip install virtualenv virtualenvwrapper
User $ virtualenv --python=python3 가상환경명(venv)
User $ cd 가상환경명(venv)/Scripts
User $ activate

# 프로젝트 경로 이동 후 패키지 관리
(venv) User $ pip freeze > requirements.txt   # 모든 패키지 Text 파일 생성
(venv) User $ pip install -r requirements.txt # 모든 패키지 Install

# Uvicorn 실행 (host, port는 선택 사항)
(venv) User $ uvicorn main:app --host=0.0.0.0 --port=8000
```

## 2. Install 목록

> [FastAPI](https://fastapi.tiangolo.com/ko/) 는 현대적이고, 빠르며(고성능), 파이썬 표준 타입 힌트에 기초한 Python3.6+의 API를 빌드하기 위한 웹 프레임워크입니다.   
> [Uvicorn](https://www.uvicorn.org/) 은 Python용 ASGI 웹 서버 구현입니다.   
> [Gunicorn](https://gunicorn.org/) 은 파이썬 웹 서버 게이트웨이 인터페이스 HTTP 서버입니다.   
> [Requests](https://requests.readthedocs.io/) 는 Python 용 단순한 HTTP 라이브러리입니다.   
> [Pytz](https://pypi.org/project/pytz/) 는 세계 시간대 정의를 위한 Python 라이브러리입니다.   
> [Pandas](https://pandas.pydata.org/) 는 데이터 조작 및 분석을 위한 파이썬 프로그래밍 언어 용으로 작성된 소프트웨어 라이브러리입니다.   
> [SQLAlchemy](https://pypi.org/project/SQLAlchemy/) 는 응용 프로그램 개발자에게 SQL의 모든 기능과 유연성을 제공하는 Python SQL 툴킷 및 개체 관계형 매퍼입니다.
> [Pymysql](https://pypi.org/project/pymysql/) 는 mysql을 python에서 사용할 수 있는 라이브러리입니다.
> [Mysqlclient](https://pypi.org/project/mysqlclient/) 는 mysql을 python에서 사용할 수 있는 라이브러리입니다.


```sh
# Python Version : Python 3.11.4
# FastAPI
(venv) User $ pip install fastapi
# Uvicorn
(venv) User $ pip install uvicorn
# Requests
(venv) User $ pip install requests
# Pytz
(venv) User $ pip install pytz
# Pandas
(venv) User $ pip install pandas pandas_ta
# SQLAlchemy - 대체 Pymysql 사용 가능
(venv) User $ pip install SQLAlchemy
# Mysqlclient
(venv) User $ pip install mysqlclient
```

## 3. .env 파일 관리

> main.py와 같은 경로에 .env 파일을 생성하여 주셔야 합니다.