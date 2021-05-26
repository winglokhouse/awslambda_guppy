import boto3
#
import pandas as pd
from lambda_analyze_utility import run_trend_analysis
#
def load_csv_run_analysis(L1):
	key = 'daily/{}.csv'.format(L1[:4])
	print(key)
	df = pd.read_csv('s3://autoguppy/daily/{}.csv'.format(L1[:4]))
	df['index'] = pd.to_datetime(df['index'])
	df['Date-simple'] = pd.to_datetime(df['Date-simple'])
	df.set_index('index', drop=True, inplace=True)
	row_data = run_trend_analysis(L1, df, display=True)
	#
	dynamodb = boto3.resource('dynamodb')
	table = dynamodb.Table('guppy_trend')
	table.put_item(Item=row_data)

def lambda_handler(event, handler):
	for msg in event['Records']:
		L1 = msg['body']
		load_csv_run_analysis(L1)

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
