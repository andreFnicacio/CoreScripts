#%%
from redis import Redis
import boto3

def query_apsen_in_person(dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.session.Session(
            region_name='sa-east-1', profile_name='davi.aquila').resource('dynamodb')

    table = dynamodb.Table('table_micro_task_in_person')

    scan_kwargs = dict(
        FilterExpression='#company <> :c',
        ExpressionAttributeNames={
            '#company': 'company',
        },
        ExpressionAttributeValues={
            ':c': 'sofie',
        },
    )
    base = []
    done = False
    start_key = None
    while not done:
        if start_key:
            scan_kwargs['ExclusiveStartKey'] = start_key
        response = table.scan(**scan_kwargs)
        for item in (response.get('Items')):
            base.append(item)
        start_key = response.get('LastEvaluatedKey', None)
        done = start_key is None

    return base


valores_in_person = query_apsen_in_person()
# %%
itens_para_reservar = list()

for item in valores_in_person:
    cidades = ['JUNDIAI', 'JUNDIAÍ']
    if item['status']['status'] in ['FAILURE', 'WAITING'] and (
            item['original'].get('CUP_CIDADE') in cidades or item['original'].get('CIDADE') in cidades):
        itens_para_reservar.append(item)

print(len(itens_para_reservar))

# %%
from datetime import datetime


def accept(task_id, sofier, **kwargs) -> tuple:
    """
    Processa a **aceitação** de uma tarefa por parte do _sofier_
    :param task_id:
        ID da tarefa em questão
    :return:
        Booolean indicando o sucesso da operação
    """

    SCRIPT_LUA_ACCEPT = """
    --[[
      **ACEITAÇÃO DE UMA TAREFA POR PARTE DE UM SOFIER**
      Fluxo:
      =====
        - Elimina a chave de exibição da tarefa
        - Cria a chave de "tarefa aceita" pelo _sofier_ com TTL de 2 dias (172800 seg)
    --]]
    local TWO_DAYS = KEYS[1]
    local sofier = KEYS[2]
    local task_id = KEYS[3]
    local when = KEYS[4]
    redis.call('DEL', string.format('SOFIE:MICROTASK:%s:SHOWCASE:%s#', task_id, sofier))
    redis.call('SETEX', string.format('SOFIE:MICROTASK:%s:RESERVED:%s#', task_id, sofier), TWO_DAYS, when)
    """

    conn = Redis(host='54.94.105.153', port=5071)
    SHA_SCRIPT_LUA_ACCEPT = conn.script_load(SCRIPT_LUA_ACCEPT)
    tempo = 60 * 60 * 6

    data = conn.evalsha(
        SHA_SCRIPT_LUA_ACCEPT,
        4,
        tempo if sofier not in ['thiago.filadelfo@gmail.com'] else 43200,
        sofier,
        task_id,
        datetime.utcnow().isoformat()
    )

    return data

# %%
# accept('201ccfc8-e70e-4791-951d-6bd39af52675', 'vanderlei.destak@gmail.com') 
# 172800
# %%
conn = Redis(host='54.94.105.153', port=5071)
items = conn.keys('SOFIE:MICROTASK:*:RESERVED:*')
print(f'total de valores achados na base do REDIS {len(items)}')

# %%
for item in itens_para_reservar:
    task_id = item['task_id']
    sofier = "dantas-bruno@outlook.com"
    accept(task_id, sofier)

# %%
