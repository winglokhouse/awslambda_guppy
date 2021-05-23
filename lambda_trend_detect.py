import boto3
#
import numpy as np
import pandas as pd
import trendet
from scipy.signal import find_peaks, find_peaks_cwt, argrelextrema, argrelmax, argrelmin
from decimal import *
#
def rename_trend_group(row_gp, gp_id):
    if pd.isna(row_gp):
        return row_gp
    else:
        return '{}{}'.format(row_gp, gp_id)

def find_trend(df, column, direction, dir_header):
    trend = df[column].copy()
    more_trend = True
    last_end = df.index[0]
    trend_gp_id = 0
    response = True
    while more_trend:
        input_df = df[last_end:].copy()
        res = trendet.identify_df_trends(df=input_df, column=column, identify=direction)
        if dir_header in res.columns.to_list():
            trend_temp = res[dir_header]
            trend_group_list = trend_temp.dropna().unique()
            if len(trend_group_list)==0:
                more_trend = False
            else:
                trend.loc[last_end:] = trend_temp.apply(rename_trend_group, gp_id=trend_gp_id)
                if trend_group_list[-1] == 'Z':
                    # more trend to be identified
                    last_end = trend_temp[trend_temp=='Z'].index[-1]
                    trend_gp_id = trend_gp_id + 1
                else:
                    more_trend = False
        else:
            # suddenly the trendet.identify_df_trends fails to create column "Up Trend" or "Down Trend"
            trend = pd.Series(np.nan, index=df.index)
            more_trend = False
            response = False
    return trend, response

def trendet_segment(data, col):
    try:
        up_trend, up_response = find_trend(data[[col]].fillna(method='bfill'), col, 'up', 'Up Trend')
    except IndexError:
        up_response = False
    if up_response:
        last_up = up_trend.dropna().index[-1]
    else:
        last_up = None
    #
    try:
        down_trend, down_response = find_trend(data[[col]].fillna(method='bfill'), col, 'down', 'Down Trend')
    except IndexError:
        down_response = False
    if down_response:
        last_down = down_trend.dropna().index[-1]
    else:
        last_down = None
    print(up_response, last_up, down_response, last_down)
    #
    # Detemine 'last', 'selected_trend', 'last_trend_ind'
    last = None
    last_trend_ind = 0
    if up_response and down_response:
        last = max(last_up, last_down)
        if last_up > last_down:
            selected_trend = up_trend
            last_trend_ind = 1
            prev_trend = down_trend
            prev_trend_ind = -1
            prev_last = prev_trend.dropna().index[-1]
        else:
            selected_trend = down_trend
            last_trend_ind = -1
            prev_trend = up_trend
            prev_trend_ind = 1
            prev_last = prev_trend.dropna().index[-1]
    elif up_response:
        last = last_up
        selected_trend = up_trend
        last_trend_ind = 1
    elif down_response:
        last = last_down
        selected_trend = down_trend
        last_trend_ind = -1
    #
    # Determine 'untrend_loc', 'last_trend_loc'
    if last is not None:
        untrend_iloc = data.index.get_loc(last) + 1
        if untrend_iloc < data.shape[0]:
        	# there is no untrend data
        	untrend_loc = data.index[untrend_iloc]
        	last_seg = selected_trend.dropna().unique()[-1]
        	last_trend_loc = data[selected_trend==last_seg].index[0]
        else:
        	# trend_detect covers the entire data
        	# 2nd last trend becomes the last trend
        	untrend_iloc = data.index.get_loc(prev_last) + 1
        	untrend_loc = data.index[untrend_iloc]
        	last_seg = prev_trend.dropna().unique()[-1]
        	last_trend_loc = data[prev_trend==last_seg].index[0]
        	last_trend_ind = prev_trend_ind
    else:
        # no up_response and no down response
        last_trend_loc = None
        untrend_loc = data.index[0]
    #
    return untrend_loc, last_trend_loc, last_trend_ind

def peak_trough_untrend(data, last_trend_loc, untrend_loc, high_col, low_col):
    # 
    data_lasttrend = data.loc[last_trend_loc:].copy()
    High_uptonow = data_lasttrend[high_col]
    Low_uptonow = data_lasttrend[low_col]

    # Find support and resistance using scipy
    peaks2 = argrelmax(High_uptonow.values, order=2)
    peak = pd.Series(False, index=High_uptonow.index)
    peak.iloc[peaks2] = True
    peak_only = peak[peak]
    #
    troughs2 = argrelmin(Low_uptonow.values, order=2)
    trough = pd.Series(False, index=Low_uptonow.index)
    trough.iloc[troughs2] = True
    trough_only = trough[trough]
    #
    high_peak = '{}-peak'.format(high_col)
    data_lasttrend[high_peak] = peak
    low_trough = '{}-trough'.format(low_col)
    data_lasttrend[low_trough] = trough
    #
    ohlc_col = ['Date', 'Open', 'High', 'Low', 'Close']
    ohlc_rest = list(set(data_lasttrend.columns.tolist()) - set(ohlc_col))
    ohlc_col = ohlc_col + ohlc_rest
    ohlc = data_lasttrend.loc[untrend_loc:, ohlc_col]
    #
    return ohlc, high_peak, low_trough

def micro_trend_price_action(ohlc, high_peak, low_trough, high_col, low_col):
    micro_high = ohlc.loc[ohlc[high_peak],high_col]
    micro_low = ohlc.loc[ohlc[low_trough],low_col]
    #
    micro_trend_high = None
    micro_trend_low = None
    if ohlc[high_col].min() == ohlc[high_col].max():
        micro_trend_high = 0
    if ohlc[low_col].min() == ohlc[low_col].max():
        micro_trend_low = 0
    if (micro_trend_high is None) and (ohlc[high_col].tail(1).values[0] >= ohlc[high_col].max()):
        micro_trend_high = 1
        micro_trend_low = 1
        return micro_trend_high, micro_trend_low
    elif (micro_trend_low is None) and (ohlc[low_col].tail(1).values[0] <= ohlc[low_col].min()):
        micro_trend_high = -1
        micro_trend_low = -1
        return micro_trend_high, micro_trend_low
    #
    micro_trend_high = 0
    if len(micro_high) > 1:
        micro_high_max_loc = micro_high.idxmax()
        micro_high_min_loc = micro_high.idxmin()
        # you need more > 1 high point to determine a trend
        if micro_high_max_loc == micro_high.index[-1]:
            # the first High is the max -> downtrend
            micro_trend_high = 1
        elif micro_high_min_loc == micro_high.index[-1]:
             # the last High is the max -> downtrend
            micro_trend_high = -1
        else:
            if micro_high_max_loc > micro_high_min_loc:
                # Since high_max is latest, verify whether it is a downtrend
                tiny_high = micro_high[micro_high_max_loc:]
                tiny_high_nextday = tiny_high.shift(-1)
                if (tiny_high[:-1] > tiny_high_nextday[:-1]).all():
                    micro_trend_high = -1
            elif micro_high_min_loc > micro_high_max_loc:
                # Since high_min is latest, verify whether it is a uptrend
                tiny_high = micro_high[micro_high_min_loc:]
                tiny_high_nextday = tiny_high.shift(-1)
                if (tiny_high[:-1] < tiny_high_nextday[:-1]).all():
                    micro_trend_high = 1
    #
    micro_trend_low = 0
    if len(micro_low) > 1:
        micro_low_max_loc = micro_low.idxmax()
        micro_low_min_loc = micro_low.idxmin()
        if micro_low_max_loc == micro_low.index[-1]:
            # the first High is the max -> downtrend
            micro_trend_low = 1
        elif micro_low_min_loc == micro_low.index[-1]:
            # the last High is the max -> downtrend
            micro_trend_low = -1
        else:
            if micro_low_max_loc > micro_low_min_loc:
                # Since low_max is latest, verify whether it is a downtrend
                tiny_low = micro_low[micro_low_max_loc:]
                tiny_low_nextday = tiny_low.shift(-1)
                if (tiny_low[:-1] > tiny_low_nextday[:-1]).all():
                    micro_trend_low = -1
            elif micro_low_min_loc > micro_low_max_loc:
                # Since low_min is latest, verify whether it is a uptrend
                tiny_low = micro_low[micro_low_min_loc:]
                tiny_low_nextday = tiny_low.shift(-1)
                if (tiny_low[:-1] < tiny_low_nextday[:-1]).all():
                    micro_trend_low = 1
    return micro_trend_high, micro_trend_low
#
def analyze_each_field(df, close_col, high_col, low_col):
	untrend_loc, last_trend_loc, last_trend_ind = trendet_segment(df, close_col)
	ohlc, high_peak, low_trough = peak_trough_untrend(df, last_trend_loc, untrend_loc, high_col, low_col)
	micro_trend_high, micro_trend_low = micro_trend_price_action(ohlc, high_peak, low_trough, high_col, low_col)
	return last_trend_ind, micro_trend_high, micro_trend_low, last_trend_loc, untrend_loc

def true_false_to_pos_neg(x):
    if x:
        return 1
    else:
        return -1

def run_trend_analysis(L1):
	key = 'daily/{}.csv'.format(L1[:4])
	print(key)
	df = pd.read_csv('s3://autoguppy/daily/{}.csv'.format(L1[:4]))
	df['index'] = pd.to_datetime(df['index'])
	df['Date-simple'] = pd.to_datetime(df['Date-simple'])
	df.set_index('index', drop=True, inplace=True)
	# Augment df
	df['guppy_3over30'] = df['EMA3']>df['EMA30']
	df['guppy_15over30'] = df['EMA15']>df['EMA30']
	#
	price_trend_ind, price_micro_high, price_micro_low, _, _ = analyze_each_field(df, 'Close', 'High', 'Low')
	print('price trend {} price micro high {} price micro low {}'.format(price_trend_ind, price_micro_high, price_micro_low))
	mfi_trend_ind, mfi_micro_high, mfi_micro_low, _, _ = analyze_each_field(df, 'MFI', 'MFI', 'MFI')
	print('mfi trend {} mfi micro high {} mfi micro low {}'.format(mfi_trend_ind, mfi_micro_high, mfi_micro_low))
	gwidth_trend_ind, gwidth_micro_high, gwidth_micro_low, last_trend_loc, untrend_loc = analyze_each_field(df, 'guppy_long_width', 'guppy_long_width', 'guppy_long_width')
	print('guppy-width trend {} guppy-width micro high {} guppy-width micro low {}'.format(gwidth_trend_ind, gwidth_micro_high, gwidth_micro_low))
	#
	# compute EMA30 > EMA60 between last_trend_loc and last_trend_last_day_loc
	last_trend_last_day_iloc = df.index.get_loc(untrend_loc) - 1
	last_trend_last_day_loc = df.index[last_trend_last_day_iloc]
	print('first day {} last day {} untrend first day {}'.format(last_trend_loc, last_trend_last_day_loc, untrend_loc))
	last_trend_30over60_sum = df.loc[last_trend_loc:last_trend_last_day_loc ,'guppy_30over60'].apply(true_false_to_pos_neg).sum()
	if last_trend_30over60_sum > 0:
		guppy_30over60_last_ind = 1
	elif last_trend_30over60_sum < 0:
		guppy_30over60_last_ind = -1
	else:
		guppy_30over60_last_ind = 0
	#
	ema3_trend_ind, ema3_micro_high, ema3_micro_low, _, _ = analyze_each_field(df, 'EMA3', 'EMA3', 'EMA3')
	print('EMA3 trend {} EMA3 micro high {} EMA3 micro low {}'.format(ema3_trend_ind, ema3_micro_high, ema3_micro_low))
	ema15_trend_ind, ema15_micro_high, ema15_micro_low, _, _ = analyze_each_field(df, 'EMA15', 'EMA15', 'EMA15')
	print('EMA15 trend {} EMA15 micro high {} EMA15 micro low {}'.format(ema15_trend_ind, ema15_micro_high, ema15_micro_low))
	ema30_trend_ind, ema30_micro_high, ema30_micro_low, _, _ = analyze_each_field(df, 'EMA30', 'EMA30', 'EMA30')
	print('EMA30 trend {} EMA30 micro high {} EMA30 micro low {}'.format(ema30_trend_ind, ema30_micro_high, ema30_micro_low))
	ema60_trend_ind, ema60_micro_high, ema60_micro_low, _, _ = analyze_each_field(df, 'EMA60', 'EMA60', 'EMA60')
	print('EMA60 trend {} EMA60 micro high {} EMA60 micro low {}'.format(ema60_trend_ind, ema60_micro_high, ema60_micro_low))
	chip200_trend_ind, chip200_micro_high, chip200_micro_low, _, _ = analyze_each_field(df, 'CHIP_SCORE_200', 'CHIP_SCORE_200', 'CHIP_SCORE_200')
	print('CHIP200 trend {} CHIP200 micro high {} CHIP200 micro low {}'.format(chip200_trend_ind, chip200_micro_high, chip200_micro_low))
	#
	row_data = {
	    'stock': L1,
	    'last_date' : df.index[-1].strftime("%m-%d-%Y"),
	    'price_last_up': price_trend_ind,
	    'price_micro_high': price_micro_high,
	    'price_micro_low': price_micro_low,
	    'mfi_last_up': mfi_trend_ind,
	    'mfi_micro_high': mfi_micro_high, 
	    'mfi_micro_low': mfi_micro_low,
	    'guppy_30over60_last_ind': guppy_30over60_last_ind,
	    'guppy-width_last_up': gwidth_trend_ind,
	    'guppy-width_micro_high': gwidth_micro_high, 
	    'guppy-width_micro_low': gwidth_micro_low,
	    'guppy-width_untrend_firstday': untrend_loc.strftime("%m-%d-%Y"),
	    'EMA3_last_up': ema3_trend_ind,
	    'EMA3_micro_high': ema3_micro_high,
	    'EMA3_micro_low': ema3_micro_low,
	    'EMA15_last_up': ema15_trend_ind,
	    'EMA15_micro_high': ema15_micro_high,
	    'EMA15_micro_low': ema15_micro_low,
	    'EMA30_last_up': ema30_trend_ind,
	    'EMA30_micro_high': ema30_micro_high,
	    'EMA30_micro_low': ema30_micro_low,
	    'EMA60_last_up': ema60_trend_ind,
	    'EMA60_micro_high': ema60_micro_high,
	    'EMA60_micro_low': ema60_micro_low,
	    'CHIP_AVG_Price': df['CHIP_AVG_200'].tail(1)[0]>df['Close'].tail(1)[0],
	    'CHIP200_last_up': chip200_trend_ind,
	    'CHIP200_micro_high': chip200_micro_high,
	    'CHIP200_micro_low': chip200_micro_low,
	    }
	guppy_col = [x for x in df.columns if 'guppy' in x]
	for col in guppy_col:
	    # print(df[col].dtypes)
	    if df[col].dtypes == 'float64':
	        row_data[col] = Decimal(str(df.tail(1)[col].values[0]))
	    elif df[col].dtypes == 'bool':
	        row_data[col] = (df.tail(1)[col].values[0] * 1).item()
	    elif df[col].dtypes == 'int64':
	        row_data[col] = df.tail(1)[col].values[0].item()
	    else:
	        row_data[col] = df.tail(1)[col].values[0]
	#
	dynamodb = boto3.resource('dynamodb')
	table = dynamodb.Table('guppy_trend')
	table.put_item(Item=row_data)

def lambda_handler(event, handler):
	for msg in event['Records']:
		L1 = msg['body']
		run_trend_analysis(L1)

event = {'Records': [
            {'messageId': '67df3bef-db40-4efd-8ab3-9a91ac2a8bef',
            'receiptHandle': 'AQICb/KtCVgWXxCZhbDcNki/rtsKyag6EE2gA08aer0eAknp3ceEsiW9hQKxQHuIWps8VPiqmR5+A7xlOo+3etrXGJZ4x1dcIQRYXyeoHq/sdlvw1K4AoAGGEVwAgMjBb3+WR6N3yrDTt4OCOc4lO7SuUyTe1IoFiiSsSzp9T4xUIWmH3FmLDK2QmIdzi8t7GFqbSZ9m46WkunBmSDiAxkc+NMBseygCWcW3a5+SoSYk5hfJsvASzyU5WQBu3hrEubu77sCVALO9vKWvSC4uKp5cThzdSVDXyggf4H3Z8rT8WweSkTFtxiqwRWFkBnqbdS2l3EsNXte6QkKFe1tWmxe3krdi+L6RLCfKrtgMcgRSWqHvq5t8ro8e03Kn3qFJt5wUsm2K6HqeWIN1gwfmyHqLCg==', 
            'body': '0656.HK',
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
