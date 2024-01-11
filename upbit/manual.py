import logging
import sqlite3
import time
from dataclasses import dataclass
import logging
import logging.handlers
from pyupbit  import Upbit
import pyupbit
import pandas as pd
from enum import Enum, auto
from decimal import Decimal
from sqlalchemy import create_engine
from datetime import datetime
import os
import pymysql.cursors
from pymysql.connections import Connection
import pymysql
from pytz import timezone
import time
from datetime import timedelta
from sqlalchemy.engine.url import URL


class Client:

    def __init__(self,access_key,secret_key):

        self.access_key=access_key
        self.secret_key=secret_key
        self.upbit = Upbit(self.access_key, self.secret_key)


    # 지정가 마켓주문 buy_limit_order("XRP",550,10000)
    def buy_limit_order(self,symbol, price, money):

        self.symbol='KRW-'+symbol
        self.price=price
        self.money=money
        

        
        # 호가단위 계산
        self.price1=pyupbit.get_tick_size(self.price)
        # 주문수량
        self.qut=round((self.money/self.price),3)
        
        try:
            return_result = self.upbit.buy_limit_order(self.symbol, self.price1, self.qut)

           
            
            if list(return_result.keys())[0]=='error' or return_result is None:

                return f"{return_result}지정가 주문실패"
            
            else:

                return f"{symbol}코인 {price}원에 {self.qut}개주문완료 "
            
        except Exception as e:
            
            return f"지정가 주문실패 코인명 투입금액을 확인하세요"
        
       
    

    # 오픈오더 모두 취소 cancel_all_open_orders("XRP")
    # 특정 오픈오더  취소 cancel_all_open_orders("XRP",'82ea0a50-8a2a-42a4-b42d-193d1a066c32')
    def cancel_all_open_orders(self,symbol,uuid=''):
        self.uuid=uuid

        self.symbol='KRW-'+symbol
        
        if len(self.uuid)>0:
            try:

                return_result = self.upbit.cancel_order(self.uuid)
                
                if list(return_result.keys())[0]=='error' or return_result is None :

                    return f"{return_result}오픈오더 취소실패"
            
                else:

               
                    return f"{self.symbol} 오픈오더가 취소 되었습니다"
                
            except Exception as e:
                return "오픈오더주문취소 실패 코인명 밒 주문번호를 확인하세요"

        else:
            try:
                return_result= self.upbit.get_order(self.symbol)
                

                if  return_result is None:

                    return f"{return_result} 모든오픈오더 취소실패"
            
                else:
                    
                    for i in return_result:
                        
                        uid=i['uuid']
                        self.upbit.cancel_order(uid)

                    return f"{self.symbol}의 모든 오픈오더가 취소 되었습니다"
                
            except Exception as e:

                return "모드오픈오더 주문취소 실패"

        

    # 오픈오더 리스트 조회
    def order_list(self,symbol):
        self.symbol='KRW-'+symbol
        data_list=[]
        data_dict={}
        try:
            open_or=self.upbit.get_order(self.symbol)
            print(open_or)
            for i in open_or:

                data_dict['코인']=i['market']
                data_dict['주문번호']=i['uuid']
                data_dict['주문시간']=i['created_at']
                data_dict['주문수량']=i['volume']
                data_dict['주문상태']=i['state']
                data_list.append(data_dict)

        except Exception as e:
            print(e,"오더리스트 조회실패")
            
        return data_list
    
    # 시장가 판매
    def  sell_market_order (self,symbol,quantity):

        self.symbol='KRW-'+symbol
        self.quantity=quantity
        try:
            
            return_result=self.upbit.sell_market_order(self.symbol, self.quantity)
            
            if list(return_result.keys())[0]=='error' or return_result is None :

                return f"{return_result} 시장가 판매실패"
            
            else:

                return f"{self.symbol} 코인 {self.quantity}개 시장가 판매되었습니다"
        except Exception as e:
            return f"시장가 판매 실패 코인 수량 이름을 확인하세요"

        

   
    # 포지션 조회
    def get_position_list(self):


        ticker_list=[]
        self.ticker=self.upbit.get_balances()
        
        try:
            for i in self.ticker:
                current_p=float(i['avg_buy_price'])*float(i['balance'])

                if i['currency']!='KRW' and current_p>5000:

                    ticker_list.append({"symbol":i['currency'],"positionAmt":i['balance'],"평단":i['avg_buy_price']})
                     
        except Exception as e:
            print(e,"밸런스 조회")

        return ticker_list
    
    # 보유 포지션 모두 종료
    def close_all_position_orders(self):
        position_list=self.get_position_list()
        
        if len(position_list)>0 :
            for position in position_list:
                
                try:
                    
                    self.upbit.sell_market_order("KRW-"+position['symbol'], position['positionAmt'])

                    
                except Exception as e:
                    print(e,"시장가 판매 실패")
        else:
            msg=" 보유 포지션이 존재 하지않습니다"
            return msg
        
        return " 모든 보유 포지션이 종료 되었습니다"
    
    # 시장가 진입 market_order("XRP",10000)
    def market_order(self,symbol,price):

        self.symbol='KRW-'+symbol
        self.price=price
        try:
            return_result = self.upbit.buy_market_order(self.symbol,self.price)
            
           

            if  list(return_result.keys())[0]=='error' or return_result is None:

                return f"시장가 매수 실패{return_result}"
            else:

                return f"{self.symbol}코인{self.price}원 시장가 주문이 완료됬습니다 "
            
        except Exception as e:
            return f"시장가 매수 실패 코인명 가격을 확인하세요"
            

    
    
    # 사용가능 잔고 조회
        
    def balance_list(self):
        data_dict={}
        balances=self.upbit.get_balances()
        for i in balances:
            try:
                if i['currency']=='KRW' :
                    data_dict['사용 가능원화']=i['balance']
                    data_dict['주문예약 총원화']=i['locked']
                else:
                    sum_krw=float(i['balance'])*float(i['avg_buy_price'])
                    sum_krw=+sum_krw
                    data_dict['사용중인 원화']=sum_krw
            except Exception as e:

                print(e,"잔고리스트 조회 실패")

        return data_dict
    
    # 미실현 수익

    def get_unrealized_pnl(self):
        pnl_list=[]
        open_positions = self.get_position_list()
        
        if len(open_positions)>0:
            for ticker in open_positions:
                try:
                    current=pyupbit.get_current_price("KRW-"+ticker['symbol'])
                    befor_pnl=float(ticker['positionAmt'])*float(ticker['평단'])
                    un_pnl =(current*float(ticker['positionAmt']))-befor_pnl
                    pnl_list.append(un_pnl )
                    
                except Exception as e:
                    print(e,"미실현수익 조회 실패")
                    return "미실현수익 조회 실패"

        else:
            return "미실현수익이 존재하지않습니다"
        
        return f"총 미실현 수익{round(sum(pnl_list),3)} KRW"
        


a=Client('ICMtelMwTtoTbCyMVY8RDbwTgF9XItl5dBNmMxK8','09lDrz70uCNgz1bGWiEbwhbGcFqTmVxTix6ZRw1F')

print(a.get_unrealized_pnl())




