from fastapi import APIRouter
from starlette.responses import JSONResponse


router = APIRouter()


@router.get('/list', summary='Ticker List API')
async def ticker_list():

    res = ''

    return JSONResponse(res.return_dict_data, status_code=res.status_code)