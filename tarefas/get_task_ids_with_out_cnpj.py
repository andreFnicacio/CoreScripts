# %%
# listagem de tasks finalizadas
from ssl import VerifyFlags
import boto3
from numpy import column_stack
import pandas as pd

ids = list()


session = boto3.session.Session()
dynamodb = session.resource('dynamodb')

company = 'apsen'


# FUNÇOES RESPONSAVEL POR ACESSAR E TRAZER DADOS DO DYNAMO


def buscar_tabela_in_person():
    table = dynamodb.Table('table_micro_task_in_person')
    params = dict(
        FilterExpression='#company = :c',
        ExpressionAttributeNames={'#company': 'company'},
        ExpressionAttributeValues={':c': company}
    )
    tasks = list()
    res = list()

    while True:
        response = table.scan(**params)
        items = response.get("Items")
        if items:
            tasks.extend(items)

        last_key = response.get('LastEvaluatedKey')

        if not last_key:
            break

        params['ExclusiveStartKey'] = last_key

    for data in tasks:
        item = {
                'CNPJ': data['original'].get('CNPJ', None) or data['original'].get('CNPJ ', None),
                'task_id': data['task_id']
            }
        res.append(item) 
                      

    return res 


def buscar_tabela_execution():
    table = dynamodb.Table('table_micro_task_execution')
    params = dict(
        FilterExpression='company = :c',
        ExpressionAttributeValues={
            ':c': company
        }
    )

    # response = table.cam(**params)

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

    # CAPTURA DAS QUESTIONS EXECUTION/PERSON
    
    for data in tasks:
        for itemExec in data['execution']:
            if (itemExec['index'] == 101 and itemExec['response'] == 'Não'):
                finishedTask.append(data['task_id'])        

                           

    return finishedTask


# SEÇAO RESPONSAVEL POR CAPTURAR DADOS DO DYNAMO E ENCAMINHAR PARA O ARQUIVO
execution = buscar_tabela_execution()
print(len(execution))
inperson = buscar_tabela_in_person()
print('executuion carregada')
data = list()
# %%
for exec in execution:
    # data_inperson = list(filter(lambda item: item['task_id'] == task_id, inperson))
    res = [x for x in inperson if x['task_id'] == exec]
    if(res != []):
        data.append(res[0]['task_id'])
        


print('Iniciando DataFrame')        
dataFrame = pd.DataFrame(data)
print(len(data))

dataFrame.to_excel(f'./list_not_found_CNPJ_{company}.xlsx', index=False)
print('Finalizado')

# %%