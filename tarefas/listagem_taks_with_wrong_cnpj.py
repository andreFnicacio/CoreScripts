# %%
# listagem de tasks finalizadas
from dataclasses import replace
from ssl import VerifyFlags
import boto3
from numpy import column_stack
import pandas as pd

ids = list()


session = boto3.session.Session()
dynamodb = session.resource('dynamodb')

company = 'apsen'


data_inicial = '2022-04-01T00:00:00.000000'
data_final = '2022-05-30T23:59:59.005101'


# FUNÇOES RESPONSAVEL POR TRAZER DADOS DO DYNAMO


def buscar_tabela_in_person():
    table = dynamodb.Table('table_micro_task_in_person')
    params = dict(
        FilterExpression='#company = :c and #last_movement BETWEEN :dtI and :dtF',
        ExpressionAttributeNames={'#company': 'company', "#last_movement": "last_movement"},
        ExpressionAttributeValues={
            ':c': company,
            ':dtI': data_inicial,
            ':dtF': data_final}
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
        verify = data['original'].get('CNPJ ', None)
        if verify != None:
            item = {
                'Apsen': data['original'].get('Apsen ', ''),
                'SUBCANAL': data['original'].get('SUBCANAL', ''),
                'BANDEIRA': data['original'].get('BANDEIRA', ''),
                'GRUPO': data['original'].get('GRUPO', ''),
                'CNPJ': verify,
                'DESCRICAO_CUP': data['original'].get('DESCRICAO_CUP', ''),
                'CUP_BAIRRO': data['original'].get('CUP_BAIRRO', ''),
                'CUP_CEP': data['original'].get('CUP_CEP', ''),
                'CUP_CIDADE': data['original'].get('CUP_CIDADE', ''),
                'CUP_ENDERECO': data['original'].get('CUP_ENDERECO', ''),
                'CUP_NUMERO': data['original'].get('CUP_NUMERO', ''),
                'CUP_UF': data['original'].get('CUP_UF', ''),
                'DESCRICAO_CUP': data['original'].get('DESCRICAO_CUP', ''),
                'GRUPO': data['original'].get('GRUPO', ''),
                'Marcas Radar': data['original'].get('Marcas Radar ', ''),
                'Marcas Radar': data['original'].get('Marcas Radar ', ''),
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
        data.append({'cep': res[0]['CUP_CEP'],'cidade': res[0]['CUP_CIDADE'],'bairro': res[0]['CUP_BAIRRO'], 'numero': res[0]['CUP_NUMERO'], 'task_id': res[0]['task_id'], 'bandeira': res[0]['BANDEIRA'],'cnpj': res[0]['CNPJ']})
            
        


print('Iniciando DataFrame')        
dataFrame = pd.DataFrame(data)
print(len(data))

dataFrame.to_excel(f'./list_not_found_CNPJ_{company}.xlsx', index=False)
print('Finalizado')

