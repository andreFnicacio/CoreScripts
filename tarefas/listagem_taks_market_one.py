# %%
from provider import *
from ssl import VerifyFlags
from numpy import column_stack
import pandas as pd

provider = Dev_Provider()
dynamodb = provider.get_dynamodb()
company = 'marketOne'

def fetch_items_marketOne():
    mongodb = provider.get_mongodb()
    db = mongodb['marketone']
    collection = db.warehouse
    cursor = collection.find()
    
    items = list()
    for item in cursor:
        items.append(item)
    return items

def get_all_items_marketOne():
    data = fetch_items_marketOne()
    return data

def format_data(items):
    result = []

    for item in items:
        execution = {}
        temp = []
        vinhos = item['Marcas de vinhos estão disponíveis no estabelecimento']
        for vinho in vinhos:
            temp.append({'NOME': vinho})
        
        del item['Marcas de vinhos estão disponíveis no estabelecimento']


        for key in item:
            if '#' in key:
                question_name = key.split(':')[0].replace('#', '')
                name = key.split(':')[1]
                for vinho in temp:
                    if vinho['NOME'] == name:
                        vinho[question_name] = item[key]
            elif '$' in key:
                for tipo in item[key]:
                    for i in temp:
                        if i['NOME'] == tipo:
                            if 'Sinal' in key:
                                i['TEM_PROMO'] = 1
                            elif 'Material' in key:
                                i['TEM_COMUNICACAO'] = 1
            else:
                execution[key] = item[key]
        
        for i in temp:
            preco = i['PRECO']

            if preco < 100:
                i['CATEGORIA'] = 'ENTRADA'
            elif preco >= 100 and preco < 200:
                i['CATEGORIA'] = 'MAINSTREAM'
            elif preco >= 200:
                i['CATEGORIA'] = 'PREMIUM'
        
        execution['items'] = temp
        result.append(execution)

    
    return result

# GERANDO RELATORIO
warehouse = get_all_items_marketOne()
print('Iniciando DataFrame')        
dataFrame = pd.DataFrame(warehouse)

dataFrame.to_excel(f'./Task_List_{company}.xlsx', index=False)
print('Finalizado')

# GERANDO RELATORIO FORMATADO
format_data = format_data(warehouse)
print('Iniciando DataFrame')        
dataFrame = pd.DataFrame(format_data)

dataFrame.to_excel(f'./Task_List_Format_{company}.xlsx', index=False)
print('Finalizado')
# %%
