# %%
# listagem de tasks finalizadas
from ssl import VerifyFlags
import boto3
from numpy import column_stack
import pandas as pd
from collections import Counter
ids = list()


session = boto3.session.Session()
dynamodb = session.resource('dynamodb')

task_name = 'CPC'
data_inicial = '2022-03-16T00:00:00.000000'
data_final = '2022-03-18T23:59:59.005101'

column_dataframe = ["SUBCANAL",None]

def buscar_tabela_execution():
    table = dynamodb.Table('table_micro_task_execution')
    params = dict(
        FilterExpression='#task_info.#name = :tsk_name AND #audit.#approved = :a AND #audit.#when BETWEEN :dtI AND :dtF',
        ExpressionAttributeNames={"#audit": "audit","#when": "when","#approved": "approved","#task_info": "task_info", "#name": "name"},
        ExpressionAttributeValues={
            ':tsk_name': task_name,
            ':a': True,
            ':dtI': data_inicial,
            ':dtF': data_final,
        }
    )

    # response = table.cam(**params)

    tasks = list()
    get_list_who = list()

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
        get_list_who.append(data['who'])
    return get_list_who



# SEÇAO RESPONSAVEL POR CAPTURAR DADOS DO DYNAMO E ENCAMINHAR PARA O ARQUIVO
execution = buscar_tabela_execution()
# %%
print('executuion carregada')
print('gerando dict com a contagem de repetiçoes')
count = { it: freq for it, freq in Counter(execution).items() }

for value_to_task in count:
    if(count[value_to_task] >= 12):
        value_to_pay =  (count[value_to_task] // 12)*50
        count[value_to_task] = value_to_pay
    else:
        count[value_to_task] = 0

c = {k: v for k, v in sorted(count.items(), key=lambda item: item[1], reverse=True)}

# %%

tarefas = pd.DataFrame.from_dict(c, orient='index').rename(columns={0:'BONUS(R$)'})

tarefas.to_excel('../output/tarefas.xlsx')
print("Finalizado")

# %%