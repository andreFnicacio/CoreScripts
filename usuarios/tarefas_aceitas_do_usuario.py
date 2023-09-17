# %%
import pandas as pd
import boto3

dynamodb = boto3.session.Session().resource('dynamodb')
table = dynamodb.Table('table_micro_task_execution')


params = dict(
    ProjectionExpression='execution_id',
    FilterExpression='#result = :r and #who = :c and #audit.#app = :t',
    ExpressionAttributeValues={
        ':r': 'FINISH',
        ':c': 'carlospesquisa7@gmail.com',
        ':t': True
    },
    ExpressionAttributeNames={
        '#result': 'result',
        '#who': 'who',
        '#audit': 'audit',
        '#app': 'approved'
    }
)

tasks = list()

while True:
    response = table.scan(**params)
    items = response.get("Items")

    if items:
        tasks.extend(items)

    last_pass = response.get('LastEvaluatedKey')
    if not last_pass:
        break

    params.update({'ExclusiveStartKey': last_pass})

print(len(tasks))
# %%