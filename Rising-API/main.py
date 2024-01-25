from fastapi import FastAPI
from starlette.responses import JSONResponse
from routers import que_chart,ticker


app = FastAPI()

# Router 생성
app.include_router(que_chart.router, prefix='/que', tags=['Que Chart API'])
app.include_router(ticker.router, prefix='/ticker', tags=['Ticker API'])

# 메인 화면
@app.get('/', tags=['Main'], summary='메인 화면 200 지정', deprecated=True)
async def root():

    return JSONResponse(dict(tickers='Good'), status_code=200)