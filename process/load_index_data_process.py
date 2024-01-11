import pandas as pd
import numpy as np
import ccxt
import datetime
import time
import requests
from pytz import timezone

import pymysql

import warnings
import talib

warnings.filterwarnings("ignore")

binance = ccxt.binanceusdm()
coins_trd = binance.fetch_tickers()
host = "database-1-instance-1.ccjdvtwjk9lh.ap-northeast-2.rds.amazonaws.com"
port=3306
name="admin"
password="onebot7085!!"
db_name="data"

try:
    conn = pymysql.connect(host=host,port=port, user=name, passwd=password, db=db_name, connect_timeout=5)
except pymysql.MySQLError as e:
    print(e)

def get_10_days_ago_date(days=6) :
    base_line_date = (datetime.datetime.now() - datetime.timedelta(days=days)).strftime("%Y%m%d%H")
    return base_line_date

def get_previus_data(base_line_date, con) :
    sql = f"""select * from data.ticker_price_score where datetime >='{base_line_date}'"""
    previus_data = pd.read_sql(sql,con=con)
    return previus_data

def get_derivative_df(df) : 
    df = df.copy()
    df['score-derivative'] = (df['score'].diff(1))
    df['derivative'] = (df['open'].diff(1)/df['open'].shift(1)*100)
    df['score-signal'] = df['score-derivative']*df['derivative']
    # df[f'{ticker}-score-signal-2'] = df[f'{ticker}-score-signal'].apply(lambda x : 1 if x<-10 else 0)
    return df

def get_rsi(df,window=10) : 
    temp_df = df.copy()
    temp_df[f'derivative'] = (temp_df[f'open'].diff(1)/temp_df[f'open'].shift(1)*100)
    temp_df['U'] = np.where(temp_df['derivative'] > 0,temp_df['derivative'],0 ) 
    temp_df['D'] = np.where(temp_df['derivative'] < 0,-temp_df['derivative'],0 )
    AU = temp_df['U'].ewm(com=window-1,min_periods=window).mean()
    AD = temp_df['D'].abs().ewm(com=window-1,min_periods=window).mean()
    temp_df['AU'] = AU 
    temp_df['AD'] = AD
    RS = temp_df['AU']/temp_df['AD']
    RSI = RS/(1+RS)*100
    # df['RSI_distance'] = (df['RSI']-0.5).abs()
    return RSI

def get_volinger_band(df, window=48) : 
    df['ma'] = df[f'open'].rolling(window=window).mean()
    df['upper_volinger'] = df['ma'] + df[f'open'].rolling(window=window).std()*2
    df['lower_volinger'] = df['ma'] - df[f'open'].rolling(window=window).std()*2
    df['upper_volinger_distance'] = (df['upper_volinger'] - df[f'open'])/(df['upper_volinger'] - df['lower_volinger'])
    df['lower_volinger_distance'] = (df[f'open']-df['lower_volinger']) / (df['upper_volinger']-df['lower_volinger'])
    df['volinger_distance'] = (df['lower_volinger_distance'])
    return df

def get_momentum_df(df) : 
    df['momentum'] = df['RSI'] * df['volinger_distance']
    return df

def get_macd(df, fast_window=12,slow_window=26, signal_window=9) :
    df["MACD_short"]=df[f'open'].rolling(fast_window).mean()
    df["MACD_long"]=df[f'open'].rolling(slow_window).mean()
    df["MACD"]=df.apply(lambda x: (x["MACD_short"]-x["MACD_long"]), axis=1)
    df['MACD-index'] = np.abs(df['MACD_short'] / df['MACD_long']-1)
    df['MACD-cross'] = df['MACD']*df['MACD'].shift(1)
    df['MACD-cross-signal'] = df['MACD-cross'].apply(lambda x: 1 if x<0 else 0)
    return df

def get_obv(df,window=10,) : 
    obv = []
    obv.append(0)
    for i in range(1,len(df[f'open'])) :
        if df['open'].iloc[i] > df['open'].iloc[i-1] :
            obv.append(obv[-1]+df['volume'].iloc[i])
        elif df['open'].iloc[i] < df['open'].iloc[i-1] :
            obv.append(obv[-1]-df['volume'].iloc[i])
        else : 
            obv.append(obv[-1])
    df['obv'] = obv
    df['obv-ema'] = df['obv'].ewm(com=window).mean()
    df['obv-signal'] = df['obv'] - df['obv-ema']
    return df

def get_ema_df(df, window=12) : 
    df[f'ema-{window}'] = df[f'open'].ewm(com=window).mean()
    df[f'ema-distance-{window}'] = (df[f'open'] - df[f'ema-{window}'])/(df[f'open'])
    return df

def get_support_resistance_df(df,window) : 
    temp_df = df.copy()
    temp_df['max']=temp_df[f'high'].rolling(window=window).max()
    temp_df['min']=temp_df[f'low'].rolling(window=window).min()
    temp_df['distin']=temp_df['max']-temp_df['min']
    temp_df['open-low']=temp_df[f'open']-temp_df['min']
    df[f'support_resistance_index_{window}']=(temp_df['open-low']/temp_df['distin'])*100    
    return df

def get_stochastic(df, n=14,m=5,t=5):
    #n일중 최고가
    df["ndays_high"] =df[f'high'].rolling(window=n, min_periods=1).max()
    #n일중 최저가
    df["ndays_low"]= df[f'low'].rolling(window=n , min_periods=1).min() 
    # Fast%K 계산
    df["fast_k"] =((df[f'open']-df["ndays_low"])/(df["ndays_high"]-df["ndays_low"])) *100

    # Fast%D (=Slow%K) 계산

    df["slow_k"] = df["fast_k"].ewm(span=m).mean()
    # Slow%D 계산

    df["slow_d"] = df["slow_k"].ewm(span=t).mean()

    # slow%d와 slow%k의 교차점 구하기 위한 함수
    df['slow'] =df.apply(lambda x: (x["slow_k"]-x["slow_d"]),axis=1)
    df['slow-signal'] = df['slow'].rolling(n).mean()
    df["slow-signal-division"] = df['slow']/df['slow-signal']
    
    df["slow-cross"] = df['slow'] * df['slow'].shift(1)
    df['slow-cross-signal'] = df['slow-cross'].apply(lambda x:1 if x<0 else 0)

    return df

def get_rescaling_df(df,column,max,min) :
    column_key = column+"-scaler"
    df[column_key] = (df[column] - min)/(max-min)
    return df

def get_zscore_df(df,column, window=100) :
    column_key = column+"-zscore"
    df[column_key] = (df[column] - df[column].rolling(window).mean())/df[column].rolling(window).std()
    return df

def get_sar_df(df,acceleration=0.02, maximum=0.2) : 
    def get_sar_ls(sample_df) : 
        if (sample_df['SAR_cross_signal'] == 1 ) and (sample_df['SAR_signal'] == 1)  :
            return 1
        elif (sample_df['SAR_cross_signal'] == 1 ) and (sample_df['SAR_signal']== -1 ) : 
            return -1
        else : 
            return 0
        
    df['SAR'] = talib.SAR(df['high'], df['low'], acceleration=acceleration, maximum=maximum)
    df['SAR_signal']=np.where(df['real_open']>df['SAR'],1,-1)
    SAR_cross=df['SAR_signal'].shift(1)*df['SAR_signal']
    df['SAR_cross_signal']=np.where(SAR_cross<0,1,0)
    df['SAR_ls'] = df[['SAR_cross_signal','SAR_signal']].apply(lambda x:get_sar_ls(x),axis=1)
    return df

def insert_data_to_db(index_data_list, conn) :
    with conn.cursor() as cur : 
        for index_data in index_data_list : 
            ticker= index_data['ticker']
            datetime_str = index_data['datetime']
            value = index_data['value']
            parameter = index_data['parameter']
            values_String = f"'{datetime_str}','{ticker}',{value},'{parameter}'"
            sql = f"Insert into index_history (datetime,ticker,value,parameter) values ({values_String})"
            cur.execute(sql)
        conn.commit()

base_line_date = get_10_days_ago_date(days=11)

previus_data = get_previus_data(base_line_date,conn)

ticker_list = previus_data['ticker'].unique()

data_list = []
for ticker in ticker_list : 
    try : 
        new_df = previus_data[previus_data['ticker']==ticker]
        new_df = new_df.sort_values(by='datetime')
        
        new_df = get_derivative_df(new_df)
        
        new_df['RSI'] = get_rsi(new_df,window=9)

        new_df = get_volinger_band(new_df,window=20)

        new_df = get_momentum_df(new_df)
        new_df = get_zscore_df(new_df,column="momentum",window=100)

        new_df = get_obv(new_df,window=9)
        new_df = get_zscore_df(new_df,column="obv-signal",window=100)
        new_df = get_stochastic(new_df,n=24,m=5,t=5)

        new_df = get_support_resistance_df(new_df,window=24)
        new_df = get_support_resistance_df(new_df,window=48)
        new_df = get_support_resistance_df(new_df,window=72)
        new_df = get_support_resistance_df(new_df,window=96)
        new_df = get_support_resistance_df(new_df,window=120)
        new_df = get_support_resistance_df(new_df,window=240)

        new_df = get_ema_df(new_df,window=240)
        new_df = get_ema_df(new_df,window=200)
        new_df = get_ema_df(new_df,window=120)
        new_df = get_ema_df(new_df,window=60)
        new_df = get_ema_df(new_df,window=20)
        new_df = get_ema_df(new_df,window=7)

        new_df = get_macd(new_df,fast_window=12,slow_window=26,signal_window=9)
        
        try : 
            new_df = get_sar_df(new_df)
        except Exception as e:
            print(e)
        
        new_df['momentum'] = new_df['RSI'] * new_df['volinger_distance']
        new_df['momentum-signal'] = (new_df['momentum']) / (100) 
        new_df.dropna(inplace=True)
        if new_df.shape[0] == 0 :
            continue
        row = new_df.iloc[-1,:]
        
        # # algorithm 9
        # try :
        #     request_data_dict = {
        #         'datetime':[row['datetime']],
        #         'score':[float(row['score'])],
        #         'momentum_zscore':[float(row['momentum-zscore'])],
        #         'MACD_index':[float(row['MACD-index'])],
        #         'MACD_cross_signal':[float(row['MACD-cross-signal'])],
        #         'derivative':[float(row['derivative'])],
        #         'score_diff':[float(row['score-derivative'])],
        #         'score_signal':[float(row['score-signal'])]
        #     }
        #     return_data = requests.post("http://ec2-52-79-152-193.ap-northeast-2.compute.amazonaws.com:8083/autobot/v4/ml/1/1/predict",json=request_data_dict).json()
        #     data_list.append(
        #         {'datetime':row['datetime'],
        #         'ticker':ticker,
        #         'value':return_data['proba'][0],
        #         'parameter':'ml_probability'
        #         }
        #     )
        # except Exception as e:
        #     print(e)
        
        # # algorithm 10
        # try :
        #     request_data_dict = {
        #         'datetime':[row['datetime']],
        #         'score':[float(row['score'])],
        #         'momentum_zscore':[float(row['momentum-zscore'])],
        #         'MACD_index':[float(row['MACD-index'])],
        #         'MACD_cross':[float(row['MACD-cross'])],
                    
        #         'ema_distance_200':[float(row['ema-distance-200'])],
        #         'ema_distance_120':[float(row['ema-distance-120'])],
        #         'ema_distance_60':[float(row['ema-distance-60'])],
        #         'ema_distance_20':[float(row['ema-distance-20'])],
                
        #         'support_resistance_index_24':[float(row['support_resistance_index_24'])],
        #         'support_resistance_index_48':[float(row['support_resistance_index_48'])],
        #         'support_resistance_index_72':[float(row['support_resistance_index_72'])],
        #         'support_resistance_index_96':[float(row['support_resistance_index_96'])],
        #         'support_resistance_index_120':[float(row['support_resistance_index_120'])],
                
        #         'derivative':[float(row['derivative'])],
        #         'score_diff':[float(row['score-derivative'])],
        #         'score_signal':[float(row['score-signal'])]
        #     }
        #     return_data = requests.post("http://ec2-52-79-152-193.ap-northeast-2.compute.amazonaws.com:8083/autobot/v4/ml/1/2/predict",json=request_data_dict).json()
        #     data_list.append(
        #         {'datetime':row['datetime'],
        #         'ticker':ticker,
        #         'value':return_data['proba'][0],
        #         'parameter':'ml_probability_v2'
        #         }
        #     )
        # except Exception as e:
        #     print(e)
        
        # algorithm 11
        try :
            request_data_dict = {
                'datetime':[row['datetime']],
                'score':[float(row['score'])],
                'score_diff':[float(row['score-derivative'])],
                'score_signal':[float(row['score-signal'])],
                'momentum_zscore':[float(row['momentum-zscore'])],
                'MACD_index':[float(row['MACD-index'])],
                'MACD_cross':[float(row['MACD-cross'])],
                    
                'ema_distance_200':[float(row['ema-distance-200'])],
                'ema_distance_120':[float(row['ema-distance-120'])],
                'ema_distance_60':[float(row['ema-distance-60'])],
                'ema_distance_20':[float(row['ema-distance-20'])],
                
                'support_resistance_index_24':[float(row['support_resistance_index_24'])],
                'support_resistance_index_48':[float(row['support_resistance_index_48'])],
                'support_resistance_index_72':[float(row['support_resistance_index_72'])],
                'support_resistance_index_96':[float(row['support_resistance_index_96'])],
                'support_resistance_index_120':[float(row['support_resistance_index_120'])],
                
                'derivative':[float(row['derivative'])],
            }
            return_data = requests.post("http://ec2-52-79-152-193.ap-northeast-2.compute.amazonaws.com:8083/autobot/v4/ml/1/3/predict",json=request_data_dict).json()
            data_list.append(
                {'datetime':row['datetime'],
                'ticker':ticker,
                'value':return_data['proba'][0],
                'parameter':'ml_probability_v3'
                }
            )
            probability_3 = return_data['proba'][0]
        except Exception as e:
            print("11")
            print(ticker)
            print(e)
            
        # algorithm 13
        try :
            request_data_dict = {
                'datetime':[row['datetime']],
                'score':[float(row['score'])],
                'score_diff':[float(row['score-derivative'])],
                'score_signal':[float(row['score-signal'])],
                'momentum_zscore':[float(row['momentum-zscore'])],
                'MACD_index':[float(row['MACD-index'])],
                
                'ema_distance_20':[float(row['ema-distance-20'])],    
                'ema_distance_60':[float(row['ema-distance-60'])],
                
                'ema_distance_120':[float(row['ema-distance-120'])],
                'ema_distance_240':[float(row['ema-distance-240'])],
                'ema_distance_7':[float(row['ema-distance-7'])],
                
                'support_resistance_index_24':[float(row['support_resistance_index_24'])],
                'support_resistance_index_48':[float(row['support_resistance_index_48'])],
                'support_resistance_index_72':[float(row['support_resistance_index_72'])],
                'support_resistance_index_96':[float(row['support_resistance_index_96'])],
                'support_resistance_index_120':[float(row['support_resistance_index_120'])],
                'support_resistance_index_240':[float(row['support_resistance_index_240'])],
                
                'derivative':[float(row['derivative'])],
            }
            return_data = requests.post("http://ec2-52-79-152-193.ap-northeast-2.compute.amazonaws.com:8083/autobot/v4/ml/1/4/predict",json=request_data_dict).json()
            # return_data = requests.post("http://127.0.0.1:8000/autobot/v4/ml/1/4/predict",json=request_data_dict)
    
            # .json()
            data_list.append(
                {'datetime':row['datetime'],
                'ticker':ticker,
                'value':return_data['proba'][0],
                'parameter':'ml_probability_v4'
                }
            )
            probability_4 = return_data['proba'][0]
        except Exception as e:
            print(e)
        
        # algorithm 14
        try :
            request_data_dict = {
                'datetime':[row['datetime']],
                'score':[float(row['score'])],
                'score_diff':[float(row['score-derivative'])],
                'score_signal':[float(row['score-signal'])],
                'momentum_zscore':[float(row['momentum-zscore'])],
                'MACD_index':[float(row['MACD-index'])],
                
                'ema_distance_20':[float(row['ema-distance-20'])],    
                'ema_distance_60':[float(row['ema-distance-60'])],
                
                'ema_distance_120':[float(row['ema-distance-120'])],
                'ema_distance_240':[float(row['ema-distance-240'])],
                'ema_distance_7':[float(row['ema-distance-7'])],
                
                'support_resistance_index_24':[float(row['support_resistance_index_24'])],
                'support_resistance_index_48':[float(row['support_resistance_index_48'])],
                'support_resistance_index_72':[float(row['support_resistance_index_72'])],
                'support_resistance_index_96':[float(row['support_resistance_index_96'])],
                'support_resistance_index_120':[float(row['support_resistance_index_120'])],
                'support_resistance_index_240':[float(row['support_resistance_index_240'])],
                
                'derivative':[float(row['derivative'])],
                'probability_3':[probability_3],
                'probability_4':[probability_4],
                
            }
            return_data = requests.post("http://ec2-52-79-152-193.ap-northeast-2.compute.amazonaws.com:8083/autobot/v4/ml/1/5/predict",json=request_data_dict).json()
            # return_data = requests.post("http://127.0.0.1:8000/autobot/v4/ml/1/4/predict",json=request_data_dict)
    
            # .json()
            data_list.append(
                {'datetime':row['datetime'],
                'ticker':ticker,
                'value':return_data['proba'][0],
                'parameter':'ml_probability_v5'
                }
            )
        except Exception as e:
            print(e)
        
        
        data_list.append(
            {'datetime':row['datetime'],
             'ticker':ticker,
             'value':row['derivative'],
             'parameter':'price-derivative-rate'
             }
        )
        
        data_list.append(
            {'datetime':row['datetime'],
             'ticker':ticker,
             'value':row['slow_d'],
             'parameter':'stocastic_slow_d_24_5_5'
             }
        )
        
        data_list.append(
            {'datetime':row['datetime'],
             'ticker':ticker,
             'value':row['slow-cross-signal'],
             'parameter':'stocastic-slow-cross-signal'
             }
        )
        
        data_list.append(
            {'datetime':row['datetime'],
             'ticker':ticker,
             'value':row['score-derivative'],
             'parameter':'score-derivative'
             }
        )
        
        data_list.append(
            {'datetime':row['datetime'],
             'ticker':ticker,
             'value':row['obv-signal-zscore'],
             'parameter':'obv-signal-zscore'
             }
        )
        
        data_list.append(
            {'datetime':row['datetime'],
             'ticker':ticker,
             'value':row['momentum-zscore'],
             'parameter':'momentum-zscore'
             }
        )
        
        data_list.append(
            {'datetime':row['datetime'],
             'ticker':ticker,
             'value':row['RSI'],
             'parameter':'RSI'
             }
        )
        
        
        data_list.append(
            {'datetime':row['datetime'],
             'ticker':ticker,
             'value':row['momentum-signal'],
             'parameter':'momentum-signal'
             }
        )
        
        data_list.append(
            {'datetime':row['datetime'],
             'ticker':ticker,
             'value':row['score-signal'],
             'parameter':'score-signal'
             }
        )
        
        data_list.append(
            {'datetime':row['datetime'],
             'ticker':ticker,
             'value':row['MACD'],
             'parameter':'MACD-12-26'
             }
        )
        
        data_list.append(
            {'datetime':row['datetime'],
             'ticker':ticker,
             'value':row['MACD-cross-signal'],
             'parameter':'MACD-cross-signal-12-26'
             }
        )
        
        data_list.append(
            {'datetime':row['datetime'],
             'ticker':ticker,
             'value':row['MACD-index'],
             'parameter':'MACD-index'
             }
        )
        
        # support_resistance
        
        data_list.append(
            {'datetime':row['datetime'],
             'ticker':ticker,
             'value':row['support_resistance_index_240'],
             'parameter':'support_resistance_index_240'
             }
        )
        
        data_list.append(
            {'datetime':row['datetime'],
             'ticker':ticker,
             'value':row['support_resistance_index_120'],
             'parameter':'support_resistance_index_120'
             }
        )
        
        data_list.append(
            {'datetime':row['datetime'],
             'ticker':ticker,
             'value':row['support_resistance_index_96'],
             'parameter':'support_resistance_index_96'
             }
        )
        
        data_list.append(
            {'datetime':row['datetime'],
             'ticker':ticker,
             'value':row['support_resistance_index_72'],
             'parameter':'support_resistance_index_72'
             }
        )
        
        data_list.append(
            {'datetime':row['datetime'],
             'ticker':ticker,
             'value':row['support_resistance_index_48'],
             'parameter':'support_resistance_index_48'
             }
        )
        
        data_list.append(
            {'datetime':row['datetime'],
             'ticker':ticker,
             'value':row['support_resistance_index_24'],
             'parameter':'support_resistance_index_24'
             }
        )
        data_list.append(
            {'datetime':row['datetime'],
             'ticker':ticker,
             'value':row['ema-distance-240'],
             'parameter':'ema-distance-240'
             }
        )
        
        data_list.append(
            {'datetime':row['datetime'],
             'ticker':ticker,
             'value':row['ema-distance-200'],
             'parameter':'ema-distance-200'
             }
        )
        
        data_list.append(
            {'datetime':row['datetime'],
             'ticker':ticker,
             'value':row['ema-distance-120'],
             'parameter':'ema-distance-120'
             }
        )
        
        data_list.append(
            {'datetime':row['datetime'],
             'ticker':ticker,
             'value':row['ema-distance-60'],
             'parameter':'ema-distance-60'
             }
        )
        
        data_list.append(
            {'datetime':row['datetime'],
             'ticker':ticker,
             'value':row['ema-distance-20'],
             'parameter':'ema-distance-20'
             }
        )
        data_list.append(
            {'datetime':row['datetime'],
            'ticker':ticker,
            'value':row['ema-distance-7'],
            'parameter':'ema-distance-7'
            }
        )
        
        try : 
            data_list.append(
                {'datetime':row['datetime'],
                'ticker':ticker,
                'value':row['SAR_ls'],
                'parameter':'SAR_ls'
                }
            )
        except Exception as e : 
            print(e)
        
        
    except Exception as e :
        print(e)
        print(ticker)
    # print(data_list)
    # break

# for data in data_list : 
#     if data['parameter'] == 'RSI' : 
#         print(data)
insert_data_to_db(data_list,conn)