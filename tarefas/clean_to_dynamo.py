# %%
# listagem de tasks finalizadas
import boto3
import simplejson as json
ids = list()

session = boto3.session.Session()
dynamodb = session.resource('dynamodb')



column_dataframe = ["SUBCANAL",None]

def buscar_tabela_execution():
    table = dynamodb.Table('table_micro_task_in_person')
    params = dict(
        FilterExpression='attribute_not_exists(#status)',
        ExpressionAttributeNames={"#status": "status"}
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
        get_list_who.append(data)
    return get_list_who



# SEÇAO RESPONSAVEL POR CAPTURAR DADOS DO DYNAMO E ENCAMINHAR PARA O ARQUIVO
execution = buscar_tabela_execution()
with open('tasks_with_out_status.json', 'w') as f:
    json.dump(execution, f)
print("Sucesso")