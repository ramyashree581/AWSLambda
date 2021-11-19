# ---------------- retrieve data from DynamoDB 

import json
import boto3
from decimal import Decimal

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return json.JSONEncoder.default(self, obj)

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('example')

def lambda_handler(event, context):
    
    order_id = int(event['queryStringParameters']['order_id'])
    response = table.get_item(Key={'order_id': order_id})
    print(response['Item'])
    return {
        'statusCode': 200,
        'body': json.dumps(response['Item'],cls=DecimalEncoder)
    }

# ----------------- parse csv data and put to DynamoDB

import json
import boto3
import urllib
import csv


def lambda_handler(event, context):
    
    bucket = event['Records'][0]['s3']['bucket']['name']
    
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    
    s3_resource = boto3.resource('s3')
    s3_object = s3_resource.Object(bucket, key)
    
    data = s3_object.get()['Body'].read().decode('utf-8').splitlines()
    
    dynamodb = boto3.client('dynamodb')
    
    lines = csv.reader(data)
    headers = next(lines)
    print('headers: %s' %(headers))
    for line in lines:
        
        dynamodb.put_item(TableName='example', Item={'order_id':{'N':line[0]},'name':{'S':line[1]},'item':{'S':line[2]}, 'value':{'S':line[3]}})
        
        #print complete line
        print(line)
