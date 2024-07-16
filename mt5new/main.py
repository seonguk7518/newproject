from fastapi import FastAPI, Request, status
from starlette.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from fastapi.encoders import jsonable_encoder
from routers import mt5
import uvicorn
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()
origins = ["http://localhost:8080", "http://localhost:8081"
           
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(mt5.router, prefix='/mt5')

# @app.exception_handler(RequestValidationError)
# async def validation_exception_handler(request: Request, exc: RequestValidationError):
#     return JSONResponse(
#         status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
#         content=jsonable_encoder({"results": {'result' : exc.errors()[0]['type']}, 
#                                   "retCode": 422,
#                                   "message": f"{exc.errors()[0]['msg']} : {list(i for i in exc.errors()[0]['loc'])}"
#         })
#     )


# 메인 화면
@app.get('/', tags=['Main'], summary='메인 화면 200 지정', deprecated=True)
async def root():

    return JSONResponse(dict(result='Good'), status_code=200)

# @app.get('/okx/check', tags=['Main'], summary='연결 테스트', deprecated=True)
# async def okx_check():

#     return JSONResponse(dict(result='OKX'), status_code=200)


if __name__ == "__main__":
    uvicorn.run(app="main:app", host="0.0.0.0", port=8000,reload=True)
