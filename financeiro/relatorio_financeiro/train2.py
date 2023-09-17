# %%
import boto3

from json import dumps, loads, JSONEncoder
from decimal import Decimal
from uuid import uuid4
import pandas as pd
from datetime import datetime

start = datetime(2022, 3, 1, 0, 0, 0, 0)
finish = datetime(2022, 4, 1, 0, 0, 0, 0)

# try:
#     dynamodb = boto3.resource('dynamodb')
#     clambda = boto3.client('lambda')
# except:
dynamodb = boto3.session.Session().resource('dynamodb')
clambda = boto3.session.Session().client('lambda')


class DecimalEncoder(JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return int(o) if o % 1 == 0 else float(o)
        return super(DecimalEncoder, self).default(o)


def start_cron(event, context):
    # start = finish.replace(month=finish.month - 1)

    balances = __balances(start, finish)

    lots = list()

    for it in balances:
        i = __create_lot(it, balances[it])
        balances[it].update({'_id': i[it]})

        lots.append(i)

    __send_email(balances, start, finish)

    return 200, {'lots': lots, 'start': start.isoformat(), 'finish': finish.isoformat()}


def __value_company(company: str) -> float:
    valor = 0.0
    if company == 'VR':
        valor = 25.0
    if company == 'alelo':
        valor = 36.0
    if company == 'apsen':
        valor = 25.0
    return valor


def __balances(start: datetime, finish: datetime) -> dict:
    """
    Retorna todos os lancamentos em carência de lote

    dict -> { company: { moves, total_reward,total_fiscal, competence } }
    """

    competence = f'{start.year}-{start.month:02}'

    table_db = dynamodb.Table('table_micro_task_execution')
    params = dict(
        ProjectionExpression='company, reward, task_id, execution_id, who, #when.#start',
        FilterExpression='#company <> :c and audit.approved = :ap and #audit.#when BETWEEN :date1 and :date2 and #result = :r ',
        ExpressionAttributeNames={
            '#result': 'result',
            '#company': 'company',
            '#audit': 'audit',
            '#when': 'when',
            '#start': 'start'
        },
        ExpressionAttributeValues={
            ':date1': start.isoformat(),
            ':date2': finish.isoformat(),
            ':r': 'FINISH',
            ':ap': True,
            ':c': 'sofie',
        }
    )

    items = list()

    while True:
        response = table_db.scan(**params)
        for item in response['Items']:
            items.append(item)

        last_key = response.get('LastEvaluatedKey')

        if not last_key:
            break

        params['ExclusiveStartKey'] = last_key

    print(len(items))
    a = 0
    for i in items:
        if i['company'] == 'apsen':
            a+=1
    print(a)

    valor = list()
    table_db_2 = dynamodb.Table('table_micro_task_in_person')
    params_2 = dict(
        ProjectionExpression='task_id, #task',
        FilterExpression='#company <> :c',
        ExpressionAttributeNames={
            '#company': 'company',
            '#task': 'task',
        },
        ExpressionAttributeValues={
            ':c': 'sofie'
        }
    )

    while True:
        response_2 = table_db_2.scan(**params_2)
        for item in response_2['Items']:
            valor.append(item)

        last_key = response_2.get('LastEvaluatedKey')

        if not last_key:
            break

        params_2['ExclusiveStartKey'] = last_key


    for item in items:
        task_id_execution = item['task_id']
        for dado in valor:
            task_id_in_person = dado['task_id']
            if task_id_execution == task_id_in_person:
                item['reward'] = dado['task']['reward']
                index = items.index(item)
                items[index].update(item)

    balances = dict()

    for item in items:
        company = item['company']
        company_data = balances.get(company, {})

        reward = item.get('reward', 10.0)

        # MOVIMENTACOES
        moves = company_data.get('moves', [])
        moves.append({
            'reward': Decimal(reward),
            'task_id': item['task_id'],
            'execution_id': item['execution_id'],
            # 'sofier': item['who'],
            # 'when': item['when']['start']
        })

        # TOTAL DO PREMIO
        total_reward = Decimal(float(company_data.get(
            'total_reward', 0.0)) + float(reward))

        # TOTAL DO NOTA FISCAL
        total_fiscal = Decimal(float(company_data.get('total_fiscal', 0.0)) +
                               (__value_company(company) - float(reward)))

        # JUNTANDO TUDO
        company_data.update(dict(moves=moves, total_reward=total_reward,
                                 total_fiscal=total_fiscal, competence=competence))

        balances[company] = company_data

    return balances


def __create_lot(company: str, charge: dict) -> dict:
    _id = str(uuid4())

    financial_db = dynamodb.Table('table_financial')
    item = {'company': company, '_id': _id}
    item.update(**charge)
    # COMENDTANDO PARA NAO INSERIR NO BANCO DURANTE TESTES
    # financial_db.put_item(Item=item)

    return {company: _id}


def __text_to_email(balances: dict, start: datetime, finish: datetime):
    """
    Formata o email à partir dos dados processados
    """

    data = [
        f'<h3>Lote para enviar ao financeiro:</h3><br/>'
        f'<table border="1">'
        f'<tr>'
        f'<th><strong>Empresa</strong></th>'
        f'<th><strong>Competência</strong></th>'
        f'<th><strong>Quantidade de tarefas</strong></th>'
        f'<th><strong>Nota de Débito</strong></th>'
        f'<th><strong>Nota Fiscal</strong></th>'
        f'<th><strong>Total</strong></th>'
        f'</tr>'
    ]

    def format_currency(value) -> str:
        return f'{value:0,.2f}'.replace('.', '|').replace(',', '.').replace('|', ',')

    for i in balances:
        balance = balances[i]

        data.append(
            f'<tr>'
            f'<td>{i.upper()}</td>'
            f'<td>{balance["competence"]}</td>'
            f'<td>{len(balance["moves"])}</td>'
            f'<td>R$ {format_currency(balance["total_reward"])}</td>'
            f'<td>R$ {format_currency(balance["total_fiscal"])}</td>'
            f'<td>R$ {format_currency(balance["total_reward"] + balance["total_fiscal"])}</td>'
            f'</tr>'
        )

    data.append('</table>')

    data.append('<br/>')

    data.append(
        f'<em>Foram processadas tarefas de {start.strftime("%d/%m/%Y %H:%M:%S")} até {finish.strftime("%d/%m/%Y %H:%M:%S")}</em>')
    data.append('<br/>')

    data.append('<p>Acesse o portal de administração do Sofie</p>')
    data.append(
        '<p><a href="https://mysofie.com/financial">https://mysofie.com/financial</a></p>')
    data.append('<p>após clique em Enviar Lote</p>')
    return ''.join(data)


def __send_email(balances: dict, start: datetime, finish: datetime):
    competence = f'{start.month:02}-{start.year}'

    data_to_email = {
        'subject': f'ESPELHO DO FATURAMENTO - {competence}',
        #  'to': ['erik@mysofie.com', 'sonia@mysofie.com', 'davi.aquila@mysofie.com', 'developer@mysofie.com'],
        'to': ['davi.aquila@mysofie.com'],
        'message': __text_to_email(balances, start, finish)
    }

    data = dumps(data_to_email, cls=DecimalEncoder)
    print(data)
    clambda.invoke(
        FunctionName='arn:aws:lambda:{}:{}:function:{}'.format(
            'sa-east-1', '971419184909', 'handler_sofie_mf_sender-email'),
        InvocationType='Event',
        Payload=data

    )

# %%

print('Iniciando')
start_cron(None, None)
print('fim')

# %%

