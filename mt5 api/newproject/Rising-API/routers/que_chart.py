from fastapi import APIRouter
from starlette.responses import JSONResponse
from models.models import QueIntervalModel
from utils.que_func import QueFunc
from utils.tl import RSIQueFunc

router = APIRouter()

@router.get('/signal', summary='Que Chart Signal API')
async def que_signal(ticker: str, limit: QueIntervalModel):

    res = QueFunc()
    res.que_signal(ticker, limit.name[1:])

    return JSONResponse(res.return_dict_data, status_code=res.status_code)

@router.get('/rsi', summary='Que Chart RSI_Signal API')
async def rsi_que_signal(ticker: str, limit: QueIntervalModel):

    res = RSIQueFunc()
    res.que_signal(ticker, limit.name[1:])

    return JSONResponse(res.return_dict_data, status_code=res.status_code)