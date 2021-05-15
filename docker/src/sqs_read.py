def lambda_handler(event, context):
	if 'Records' in event:
		for msg in event['Records']:
			handle = msg['receiptHandle']
			body = msg['body']
			sqs = msg['eventSourceARN']
			region = msg['awsRegion']
			print('body : {}'.format(body))
			print('sqs : {}'.format(sqs))
			print('region : {}'.format(region))
	return None
'''
event = {'Records': [
			{'messageId': '67df3bef-db40-4efd-8ab3-9a91ac2a8bef',
			'receiptHandle': 'AQICb/KtCVgWXxCZhbDcNki/rtsKyag6EE2gA08aer0eAknp3ceEsiW9hQKxQHuIWps8VPiqmR5+A7xlOo+3etrXGJZ4x1dcIQRYXyeoHq/sdlvw1K4AoAGGEVwAgMjBb3+WR6N3yrDTt4OCOc4lO7SuUyTe1IoFiiSsSzp9T4xUIWmH3FmLDK2QmIdzi8t7GFqbSZ9m46WkunBmSDiAxkc+NMBseygCWcW3a5+SoSYk5hfJsvASzyU5WQBu3hrEubu77sCVALO9vKWvSC4uKp5cThzdSVDXyggf4H3Z8rT8WweSkTFtxiqwRWFkBnqbdS2l3EsNXte6QkKFe1tWmxe3krdi+L6RLCfKrtgMcgRSWqHvq5t8ro8e03Kn3qFJt5wUsm2K6HqeWIN1gwfmyHqLCg==', 
			'body': 'HK.0700',
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
'''
