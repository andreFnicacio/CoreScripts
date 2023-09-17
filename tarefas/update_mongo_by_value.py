import boto3
import json
from numpy import column_stack
import pandas as pd
from datetime import datetime
from provider import *

provider = Dev_Provider()
dynamodb = provider.get_dynamodb()

def update_tabela_person(task_id):
        table = dynamodb.Table('table_micro_task_in_person')    
        params = dict(
            Key={
                'task_id': task_id
            },            
            ConditionExpression= 'NOT #status.#state IN (:state) AND #task.#reward = :reward_error',
            UpdateExpression='SET #task.#reward = :reward',
            ExpressionAttributeNames={"#task": "task", '#reward': 'reward', '#status': 'status', '#state': 'state'},            
            ExpressionAttributeValues={
                ':reward': 15,
                ':reward_error': 20,
                ':state': 'FINISHED' 
            }
        )
        response = table.update_item(**params)   

        return response 

def buscar_tabela_person():
        table = dynamodb.Table('table_micro_task_in_person')
        params = dict(
            FilterExpression='NOT #status.#state IN (:state) AND #task.#reward = :reward',
            ExpressionAttributeNames={"#task": "task", '#reward': 'reward', '#status': 'status', '#state': 'state'},
            ExpressionAttributeValues={
                ':state': 'FINISHED',
                ':reward': 15
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

        # CAPTURA DAS QUESTIONS person

        for data in tasks:
           finishedTask.append(data['task_id'])

        return finishedTask

in_person_items = buscar_tabela_person()
print(len(in_person_items))

#for item in in_person_items:
#    items_update = update_tabela_person(item)#

#print('Sucesso')
#int(items_update)

