# %%
import boto3
import json
from datetime import datetime, timedelta
from uuid import uuid4
from datetime import datetime
from redis import Redis
#%%
dynamodb = boto3.session.Session(
    region_name='sa-east-1', profile_name='david.soares').resource('dynamodb')
table_dynamo = dynamodb.Table('table_micro_task_in_person')


# %%

def query_apsen(dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.session.Session(
            region_name='sa-east-1', profile_name='david.soares').resource('dynamodb')

    table = dynamodb.Table('table_micro_task_in_person')
    # #task.#name
    scan_kwargs = dict(
        FilterExpression='#status.#status <> :SUCCESS and #status.#status <> :EXECUTED and #company = :c',
        ExpressionAttributeNames={'#status': 'status', '#company':'company'},
        ExpressionAttributeValues={':SUCCESS': 'SUCCESS', ':EXECUTED': 'EXECUTED', ':c': 'apsen'}
    )
    # scan_kwargs = dict(
    #    FilterExpression='#status.#status <> :SUCCESS and #status.#status <> :EXECUTED',
    #    ExpressionAttributeNames={'#status': 'status'},
    #    ExpressionAttributeValues={':SUCCESS': 'SUCCESS', ':EXECUTED': 'EXECUTED'}
    # )
    #scan_kwargs = dict(
    #    FilterExpression='#company = :c and #status.#lot_reference = :d',
    #    ExpressionAttributeNames={'#company': 'company', '#lot_reference': 'lot_reference', '#status': 'status'},
    #    ExpressionAttributeValues={':c': 'apsen', ':d': '2021-09-11T13:56:44.511200'}
    #)

    FINAL_LIST = list()
    done = False
    start_key = None
    while not done:
        if start_key:
            scan_kwargs['ExclusiveStartKey'] = start_key
        response = table.scan(**scan_kwargs)
        for item in (response.get('Items')):
            FINAL_LIST.append((item))
        start_key = response.get('LastEvaluatedKey', None)
        done = start_key is None

    return FINAL_LIST


valor = query_apsen()
print(len(valor))
# %%

update = []

for item in dados:
    for k in list(item['original']):
        if k == "CNPJ ":
            item['original']['CNPJ'] = item['original'][k]
            item['original'].pop(k)

    update.append(item)

print(len(update))
# %%
tasks = list()
for item in valor:
    item['task']['cycle'] = 1
    item['task']['etapa'] = 1
    item['task']['onda'] = 1
    tasks.append(item)

# %%
update = []

for item in valor:
    item['task']['reward'] = 15
    update.append(item)

print(len(update))


# %%

def update_dynamo(task_id, task, table_dynamo):
    try:
        response = table_dynamo.update_item(
            Key={
                'task_id': task_id,
            },
            UpdateExpression='SET #task = :a',
            ExpressionAttributeNames={
                '#task': 'task',
            },
            ExpressionAttributeValues={':a': task},
            ReturnValues="UPDATED_NEW"
        )
    except ClientError as e:
        if e.response['Error']['Code'] == "ConditionalCheckFailedException":
            print(e.response['Error']['Message'])
        else:
            raise
    else:
        return response


for item in update:
    task_id = item['task_id']
    task = item['task']
    update_dynamo(task_id, task, table_dynamo)


