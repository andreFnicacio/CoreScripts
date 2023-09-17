# %%
import boto3
from decimal import Decimal
from provider import *
from numpy import column_stack
import pandas as pd
from datetime import datetime

session = boto3.session.Session()
dynamodb = session.resource('dynamodb')
sqs = boto3.resource('sqs')
provider = Dev_Provider()

columns = list()
response_flow = dict()
data_inicial = datetime(2022, 4, 1, 0, 0, 0, 0)
data_final = datetime(2022, 4, 10, 0, 0, 0, 0)

#inserindo dados no mongo
def created_dispatch(item): 
    client = provider.get_mongodb()
    db = client.sofie
    sofier_info = db.sofier_info   
    dispatch_result = sofier_info.insert_one(item)       
    return dispatch_result  


def buscar_tabela():
    table = dynamodb.Table('table_sofier_info')
    params = dict(
        FilterExpression='NOT #sofier IN (:dtI)',
        ExpressionAttributeNames={"#sofier": "sofier"},
        ExpressionAttributeValues={
            ':dtI': 'false'
        }
    )

    tasks = list()
    finishedTask = list()
    item = dict()

    while True:
        response = table.scan(**params)
        items = response.get("Items")
        if items:
            tasks.extend(items)

        last_key = response.get('LastEvaluatedKey')

        if not last_key:
            break

        params['ExclusiveStartKey'] = last_key

    # CAPTURA DAS QUESTIONS EXECUTION
    
    for data in tasks:
        verify_username = data.get('username', None)
        verify_sofier_id = data.get('sofier_id', None)
        if verify_username != None:
            item = data
            item['_id'] = data['username']
            if verify_sofier_id != None:                
                item['sofier_id']['tasks'] = int(data['sofier_id']['tasks'])

            finishedTask.append(item)

    return finishedTask

sofiers = buscar_tabela()

for sofier in sofiers:
    new_item = created_dispatch(sofier)
print('Sucess')

