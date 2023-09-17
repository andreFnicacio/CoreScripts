# %%
# listagem de tasks finalizadas
from ssl import VerifyFlags
from threading import local
import boto3
from numpy import column_stack
import pandas as pd
from datetime import datetime

ids = list()


session = boto3.session.Session()
dynamodb = session.resource('dynamodb')

company = 'apsen'
column_dataframe = list()
columns = list()
data_inicial = '2022-04-01T00:00:00.000000'
data_final = '2022-06-01T23:59:59.005101'

task_id = ''

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
        if(company == 'apsen'):
            item = {
                'SUBCANAL': data['original'].get('SUBCANAL', ''),
                'BANDEIRA': data['original'].get('BANDEIRA', ''),
                'GRUPO': data['original'].get('GRUPO', ''),
                'CNPJ': data['original'].get('CNPJ', '') or data['original'].get('CNPJ ', ''),
                'DESCRICAO_CUP': data['original'].get('DESCRICAO_CUP', ''),
                'task_id': data['task_id']
            }
            res.append(item)
            ids.append(data['task_id'])
        elif(company == 'alelo'):
            item = {
                'RAZAO_SOCIAL': data['original']['RAZAO_SOCIAL'],
                'EC': data['original']['NU_SO_EC'],
                'AUDITORIA': data['status']['when'],
                'ENDERECO': data['address']['formatted_address'],
                'CIDADE': data['address']['city'],
                'ESTADO': data['address']['state'],
                'task_id': data['task_id']
            }
            res.append(item)            

    return res


def buscar_tabela_execution():
    response_flow = dict()
    table = dynamodb.Table('table_micro_task_execution')
    params = dict(
        FilterExpression='company = :c AND #audit.#when BETWEEN :dtI AND :dtF AND #result = :f',
        ExpressionAttributeNames={"#result": "result", "#audit": "audit", "#when": "when"},
        ExpressionAttributeValues={
            ':c': company,
            ':dtI': data_inicial,
            ':dtF': data_final,
            ':f': 'FINISH'
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
    print(len(tasks))
    # CAPTURA DAS QUESTIONS EXECUTION/PERSON
    
    for data in tasks:
        execution = list()
        for itemExec in data['execution']:
            verify = itemExec.get('context', None)
            verify_response = itemExec.get('response', None)
            if verify == 'O que encontrou no local':
                exec = {
                    'task_id': data['task_id'],
                    'O que encontrou no local': verify_response
                }
                execution.append(exec)


                           
                


        if(company == 'apsen'):
            item = dict()

            for execAtual in execution:
                finishedTask.append(execAtual)        

        elif(company == 'alelo'):
            item = {
                'task_id': data['task_id'],
                'LATITUDE': data['task_info']['location']['lng'],
                'LONGITUDE': data['task_info']['location']['lat']
            }

            for execAtual in execution:
                pergunta = execAtual['pergunta']
                item[pergunta] = execAtual['resposta']
            finishedTask.append(item) 
                           

    return finishedTask


# SEÇAO RESPONSAVEL POR CAPTURAR DADOS DO DYNAMO E ENCAMINHAR PARA O ARQUIVO
execution = buscar_tabela_execution()
print('executuion carregada')
print(execution)
print('buscando in person')
inperson = buscar_tabela_in_person()

# %%

data = list()

for exec in range(len(execution)):
    taskid = execution[exec]['task_id']
    # data_inperson = list(filter(lambda item: item['task_id'] == task_id, inperson))
    res = [x for x in inperson if x['task_id'] == taskid]
    if(res != []):
        data.append(res[0] | execution[exec])
    else:
        print(taskid)

print(len(data))
print('Iniciando DataFrame')        
dataFrame = pd.DataFrame(data)
dataFrame.to_excel(f'list_with_out_drugstore.xlsx', index=False)

#dataFrame = dataFrame.reindex(columns=column_dataframe)

#dataFrame.to_excel(f'execucoes_abril_{company}.xlsx', index=False)
print('Finalizado')

# %%
