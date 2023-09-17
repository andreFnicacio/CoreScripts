# %%
import boto3
import json
from numpy import column_stack
import pandas as pd
from datetime import datetime

session = boto3.session.Session()
dynamodb = session.resource('dynamodb')
sqs = boto3.resource('sqs')

columns = list()
response_flow = dict()
data_inicial = datetime(2022, 5, 11, 0, 0, 0, 0)
data_final = datetime(2022, 5, 13, 0, 0, 0, 0)


def buscar_tabela_execution():
    table = dynamodb.Table('table_micro_task_execution')
    params = dict(
        FilterExpression='#audit.#when BETWEEN :dtI AND :dtF',
        ExpressionAttributeNames={"#audit": "audit", "#when": "when"},
        ExpressionAttributeValues={
            ':dtI': data_inicial.isoformat(),
            ':dtF': data_final.isoformat(),
        }
    )

    tasks = list()
    finishedTask = list()

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
        item = {
            'company': data['company'],
            'task_id': data['task_id'],
            'execution_id': data['execution_id'],
            'stage': 'PROD'
        }

        finishedTask.append(item)

    return finishedTask

execution = buscar_tabela_execution()

# %%
print('INICIANDO SQS')
queue = sqs.get_queue_by_name(QueueName='queue_mapreduce')
i = 0
print(f'total: {len(execution)}\nIniciando')
for exec in execution:
    i+=1
    print(i)
    try:
        body = exec
        queue.send_message(
            MessageBody=json.dumps(body),
            MessageAttributes={
                'aws': {
                    'DataType': 'String',
                    'StringValue': body['stage']
                },
                'context': {
                    'DataType': 'String',
                    'StringValue': 'audit'
                }
            }
        )
    except:
        print(f'erro: {exec}')
print('INFILEIRADO')

# %%