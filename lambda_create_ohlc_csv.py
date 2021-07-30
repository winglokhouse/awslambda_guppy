import yfinance as yf
import pandas as pd
import numpy as np
import talib as TA
from datetime import datetime
from dateutil.relativedelta import relativedelta
import matplotlib.dates as mdates
import boto3
from lambda_new_talib import ta_chip

def ohlc_gen_compute(data, from1):
    ### Check to make sure they are even number
    if (data.shape[0] % 2) == 1:
        data = data.iloc[1:]
    #
    data["Date"]=data.index
    data["Week"]=data["Date"].dt.week
    data["Year"]=data["Date"].dt.year
    #Create weekly data from daily data
    data_weekly=data[data.index>=from1].groupby(["Year","Week"]).agg(
        {"Date":"first",'Open':'first', 'High':'max', 'Low':'min', 'Close':'last','Volume':'sum'})
    #Add TA in daily and weekly data
    data_weekly["Week"]=data_weekly.index
    data_weekly.index=data_weekly["Date"]
    #
    dayofyear=data['Date'].dt.dayofyear
    #
    # new column introduced by Kevin Davey
    data['prevClose'] = data['Close'].shift(1)
    data['prevprevClose'] = data['Close'].shift(2)
    # 0 = Monday, 6 = Sunday
    data['dayofweek'] = data.index.to_series().dt.dayofweek
    #
    data['ATR21'] = TA.ATR(data['High'],data['Low'],data['Close'],timeperiod=21)
    data['prevATR21'] = data['ATR21'].shift(1)
    data['trailing_ATR'] = data['prevClose'] - 3*data['prevATR21']
    #
    data['SAR']=TA.SAR(data['High'], data['Low'])
    data['ADX']=TA.ADX(data['High'], data['Low'], data['Close'])
    data['prevADX']=data['ADX'].shift(1)
    data['ADX-trend']=data['ADX']-data['ADX'].shift(1) # today - previous
    #
    data['firstday'] = (dayofyear<dayofyear.shift(1)) | (dayofyear.shift(1).isnull())
    #
    data["EMA9"]=TA.EMA(data["Close"],9)
    data["EMA19"]=TA.EMA(data["Close"],19)
    data["EMA50"]=TA.EMA(data["Close"],50)
    #
    # Gruppy line short term
    data["EMA3"]=TA.EMA(data["Close"],3)
    data["EMA5"]=TA.EMA(data["Close"],5)
    data["EMA8"]=TA.EMA(data["Close"],8)
    data["EMA10"]=TA.EMA(data["Close"],10)
    data["EMA12"]=TA.EMA(data["Close"],12)
    data["EMA15"]=TA.EMA(data["Close"],15)
    #
    # Gruppy line long term
    data["EMA30"]=TA.EMA(data["Close"],30)
    data["EMA35"]=TA.EMA(data["Close"],35)
    data["EMA40"]=TA.EMA(data["Close"],40)
    data["EMA45"]=TA.EMA(data["Close"],45)
    data["EMA50"]=TA.EMA(data["Close"],50)
    data["EMA60"]=TA.EMA(data["Close"],60)
    data['guppy_long_width'] = data["EMA60"]-data["EMA30"]
    data['guppy_long_width'] = data['guppy_long_width'].abs()
    data['guppy_30over60'] = data["EMA30"] > data["EMA60"]
    #
    ST_ema = ['EMA5', 'EMA8', 'EMA10', 'EMA12', 'EMA15']
    LT_ema = ['EMA35', 'EMA40', 'EMA45', 'EMA50', 'EMA60']
    data['guppy_ShortTermTrend'] = 0
    condition = (data['EMA3'] > data[ST_ema].max(axis=1)) & data['EMA3'].notna() & data['EMA15'].notna()
    data.loc[condition, 'guppy_ShortTermTrend'] = 1
    condition = (data['EMA3'] < data[ST_ema].min(axis=1))  & data['EMA3'].notna() & data['EMA15'].notna()
    data.loc[condition, 'guppy_ShortTermTrend'] = -1
    data['guppy_LongTermTrend'] = 0
    condition = (data['EMA30'] > data[LT_ema].max(axis=1))  & data['EMA30'].notna() & data['EMA60'].notna()
    data.loc[condition, 'guppy_LongTermTrend'] = 1
    condition = (data['EMA30'] < data[LT_ema].min(axis=1)) & data['EMA30'].notna() & data['EMA60'].notna()
    data.loc[condition, 'guppy_LongTermTrend'] = -1
    data['guppy_LongTermTrend_simple'] = 0
    condition = (data['EMA30'] > data['EMA60'])  & data['EMA30'].notna() & data['EMA60'].notna()
    data.loc[condition, 'guppy_LongTermTrend_simple'] = 1
    condition = (data['EMA30'] < data['EMA60']) & data['EMA30'].notna() & data['EMA60'].notna()
    data.loc[condition, 'guppy_LongTermTrend_simple'] = -1
    #
    data['guppy_LongTermBandWiden'] = False
    #
    window = 200
    chipavg = 'CHIP_AVG_{}'.format(window)
    chipscore = 'CHIP_SCORE_{}'.format(window)
    chipema = "EMA{}".format(window)
    data[chipavg], data[chipscore] = ta_chip(data['High'], data['Low'], data['Close'], data['Volume'], window)
    data[chipema]=TA.EMA(data["Close"],window)
    data['CHIP_CRAZY'] = abs(data[chipavg] - data[chipema])
    #    
    window = 1000
    chipavg1 = 'CHIP_AVG_{}'.format(window)
    chipscore1 = 'CHIP_SCORE_{}'.format(window)
    data[chipavg1], data[chipscore1] = ta_chip(data['High'], data['Low'], data['Close'], data['Volume'], window)
    data['CHIP_TREND'] = data[chipavg] - data[chipavg1]
    #
    guppy_window = 5
    guppy_longterm = data['guppy_LongTermTrend']
    guppy_longterm_up = guppy_longterm.rolling(guppy_window).sum()==(guppy_window*1)
    guppy_longterm_down = guppy_longterm.rolling(guppy_window).sum()==(guppy_window*-1)
    #
    for d in guppy_longterm_up[guppy_longterm_up].index:
        prev_longterm = data.loc[:d, 'guppy_LongTermTrend']
        last_diff_date = prev_longterm[prev_longterm != 1].tail(1).index[0]
        first_same_iloc = data.index.get_loc(last_diff_date) + 1
        first_same_loc = data.index[first_same_iloc]
        window_median = data.loc[first_same_loc:d, 'guppy_long_width'].median()
        data.loc[d,'guppy_LongTermBandWiden'] = data.loc[d,'guppy_long_width'] > window_median
    #
    for d in guppy_longterm_down[guppy_longterm_down].index:
        prev_longterm = data.loc[:d, 'guppy_LongTermTrend']
        last_diff_date = prev_longterm[prev_longterm != -1].tail(1).index[0]
        first_same_iloc = data.index.get_loc(last_diff_date) + 1
        first_same_loc = data.index[first_same_iloc]
        window_median = data.loc[first_same_loc:d, 'guppy_long_width'].median()
        data.loc[d,'guppy_LongTermBandWiden'] = data.loc[d,'guppy_long_width'] > window_median        
    #
    data['slowk'], data['slowd'] = TA.STOCH(data['High'], data['Low'], data['Close'])
    # data = generic_peak_trough_projection_ampd(data, 'guppy_long_width', long_change)
    #
    data["RSI9"]=TA.RSI(data["Close"],9)
    data["macd"], data["macdsignal"], data["macdhist"] = TA.MACD(data["Close"], 
                                                           fastperiod=12, 
                                                           slowperiod=26, signalperiod=9)
    #
    data["EMA5"]=TA.EMA(data["Close"],5)
    data["BBupper"], data["BBmid"], data["BBlower"] = TA.BBANDS(data["Close"], 
                                                                timeperiod=5, nbdevup=2, nbdevdn=2, matype=0)
    data['RSI9-75']=data['RSI9'].rolling(200).apply(lambda x: np.percentile(x, 75))
    data['RSI9-9']=TA.EMA(data["RSI9"],9)
    data['RSI9-19']=TA.EMA(data["RSI9"],19)
    data['RSI9-50']=TA.EMA(data["RSI9"],50)
    data['obv']=TA.OBV(data['Close'], data['Volume'])
    data["obv9"]=TA.EMA(data["obv"],9)
    data["obv19"]=TA.EMA(data["obv"],19)
    data["obv50"]=TA.EMA(data["obv"],50)
    data['ADOSC']=TA.ADOSC(data["High"],data["Low"],data["Close"],data["Volume"])
    data['MFI']=TA.MFI(data["High"],data["Low"],data["Close"],data["Volume"])
    data["chaikin"]=TA.ADOSC(data['High'],data['Low'],data['Close'],data['Volume'], fastperiod=3, slowperiod=10)
    data["chaikin9"]=TA.EMA(data["chaikin"],9)
    data["chaikin19"]=TA.EMA(data["chaikin"],19)
    data["chaikin50"]=TA.EMA(data["chaikin"],50)
    #
    data_weekly["EMA9"]=TA.EMA(data_weekly["Close"],9)
    data_weekly["EMA19"]=TA.EMA(data_weekly["Close"],19)
    data_weekly["EMA50"]=TA.EMA(data_weekly["Close"],50)
    #
    data['Date-simple']=data['Date']
    data["Date"] = data["Date"].apply(mdates.date2num)
    #Construct filtered daily and weekly data for chart generation
    #
    return data

def ohlc_gen(stock_code,from1,end,read_allow,no_yfinance):
    #Fetch daily stock data
    start_time = datetime.now()
    print('{} {}'.format(from1, end))
    if from1 is None:
        data = yf.download(stock_code)
    else:
        data = yf.download(stock_code,from1,end)
    data.dropna(axis=0, inplace=True)
    if data.shape[0]==0:
        return None, None
    else:
        print('{} {}'.format(data.index[0], data.index[-1]))
        # data.to_csv(stock_csv, index=True, header=True)
        # print(data)
        data = ohlc_gen_compute(data, from1)
        data.index.names = ['index']
        # data.to_csv(stock_csv, index=True)
        print('Duration = {}'.format(datetime.now() - start_time))
        #
        ohlc_col = ['Date', 'Open', 'High', 'Low', 'Close']
        ohlc_rest = list(set(data.columns.tolist()) - set(ohlc_col))
        ohlc_col = ohlc_col + ohlc_rest
        return  data, ohlc_col

def upload_to_s3(data_csv):
    s3 = boto3.client('s3')
    bucket = 'autoguppy'
    local_file = data_csv
    key = 'daily/{}'.format(data_csv)
    s3.upload_file(Filename=local_file, Bucket=bucket, Key=key)

def send_msg_to_sqs_trenddetect(L1):
    client = boto3.client('sqs')
    sqs_url = 'https://ap-east-1.queue.amazonaws.com/113309370782/auto-guppy-trenddetect'
    response = client.send_message(
        QueueUrl=sqs_url,
        MessageBody=L1
    )
    print(L1)

def lambda_handler(event, context):
    year_span = 6
    goback = year_span * -1
    if 'Records' in event:
        for msg in event['Records']:
            handle = msg['receiptHandle']
            L1 = msg['body']
            sqs = msg['eventSourceARN']
            region = msg['awsRegion']
            #
            read_allow = False
            no_yfinance = False
            end_date = datetime.today()
            end_extend = end_date
            two_year_before = end_extend + relativedelta(years=goback)
            two_year_before_strft = two_year_before.strftime('%Y-%m-%d')
            data, ohlc_col = ohlc_gen(L1,two_year_before_strft,end_extend,read_allow,no_yfinance)
            #
            if data is not None:
                bucket = 'autoguppy'
                s3_folder = 'daily'
                data_csv = '{}.csv'.format(L1[:4])
                s3_path = 's3://{}/{}/{}'.format(bucket, s3_folder, data_csv)
                data.to_csv(s3_path)
                # upload_to_s3(data_csv)
                send_msg_to_sqs_trenddetect(L1)
    return None

event = {'Records': [
            {'messageId': '67df3bef-db40-4efd-8ab3-9a91ac2a8bef',
            'receiptHandle': 'AQICb/KtCVgWXxCZhbDcNki/rtsKyag6EE2gA08aer0eAknp3ceEsiW9hQKxQHuIWps8VPiqmR5+A7xlOo+3etrXGJZ4x1dcIQRYXyeoHq/sdlvw1K4AoAGGEVwAgMjBb3+WR6N3yrDTt4OCOc4lO7SuUyTe1IoFiiSsSzp9T4xUIWmH3FmLDK2QmIdzi8t7GFqbSZ9m46WkunBmSDiAxkc+NMBseygCWcW3a5+SoSYk5hfJsvASzyU5WQBu3hrEubu77sCVALO9vKWvSC4uKp5cThzdSVDXyggf4H3Z8rT8WweSkTFtxiqwRWFkBnqbdS2l3EsNXte6QkKFe1tWmxe3krdi+L6RLCfKrtgMcgRSWqHvq5t8ro8e03Kn3qFJt5wUsm2K6HqeWIN1gwfmyHqLCg==', 
            'body': '0700.HK',
            'attributes': {
                'ApproximateReceiveCount': '1',
                'SentTimestamp': '1619413269680',
                'SenderId': 'AIDAJSE75KHV6KTYM2KOQ',
                'ApproximateFirstReceiveTimestamp': '1619413269681'
            },
            'messageAttributes': {}, 
            'md5OfBody': '5940981fd5bb2a9106cfb4890358b14a',
            'eventSource': 'aws:sqs',
            'eventSourceARN': 'arn:aws:sqs:ap-east-1:113309370782:auto_guppy_sqs',
            'awsRegion': 'ap-east-1'}
            ]
        }
lambda_handler(event, None)
