from itertools import groupby
from numpy import column_stack
import pandas as pd
from datetime import datetime
from provider import *

#update_origianl
provider = Dev_Provider()
dynamodb = provider.get_dynamodb()

def my_function(x):
  return list(dict.fromkeys(x))



def update_tabela_person(data):
        table = dynamodb.Table('table_micro_task_in_person')    
        params = dict(
            Key={
                'task_id': data['task_id']
            },            
            ConditionExpression= '#task_id = :task_id',
            UpdateExpression='SET #original.#CNPJ = :cnpj',
            ExpressionAttributeNames={'#task_id': 'task_id','#original': 'original', '#CNPJ': 'CNPJ '},            
            ExpressionAttributeValues={
                ':cnpj': data['cnpj'],
                ':task_id': data['task_id']
            }
        )
        response = table.update_item(**params)   

        return response 



#in_person_items = buscar_tabela_person()
dataframe_not_cnpj = pd.read_excel('list_not_found_CNPJ_apsen.xlsx',dtype=str)
dataframe_original = pd.read_excel('farmacias_apsen.xlsx')


#SOFIER
bandeira_up = dataframe_not_cnpj['bandeira'].tolist()
cnpj_up = dataframe_not_cnpj['cnpj'].tolist()
cep_up = dataframe_not_cnpj['cep'].tolist()
task_id_up = dataframe_not_cnpj['task_id'].tolist()
numero_up = dataframe_not_cnpj['numero'].tolist()
district_up = dataframe_not_cnpj['bairro'].tolist()
cidade = dataframe_not_cnpj['cidade'].tolist()


#ORIGINAL
bandeira_original = dataframe_original['BANDEIRA'].tolist()
cnpj_original = dataframe_original['CNPJ'].tolist()
cep_original = dataframe_original['CEP'].tolist()
numero_original = dataframe_original['NUMERO'].tolist()
distric_original = dataframe_original['BAIRRO'].tolist()
cidade_original = dataframe_original['cidade'].tolist()


data_up = list()
data_original = list()
data_finish = list()

for index,data in enumerate(zip(bandeira_up,task_id_up,numero_up,district_up, cnpj_up, cidade, cep_up)):
    bandeira,task_id,numero,district, cnpj, cidade, cep_up = data
    data_up.append({'cep': cep_up,'bandeira': bandeira, 'task_id': task_id, 'numero': numero,'distrito': district, 'cnpj': cnpj, 'cidade':cidade})


for index,data in enumerate(zip(bandeira_original,numero_original,cnpj_original,distric_original,cidade_original, cep_original)):
    bandeira,numero,cnpj,district,cidade,cep_original = data
    data_original.append({'bandeira': bandeira,'numero': numero, 'cnpj': cnpj, 'distrito': district, 'cidade':cidade, 'cep': cep_original})


for index,data in enumerate(zip(cnpj_up,data_up)):
    cnpj_up, data_get = data
    #data_inperson = list(filter(lambda item: item['task_id'] == task_id, inperson))
    resposta = [x for x in data_original if x['cnpj'] != cnpj_up]
    #print(resposta)
    if resposta != []:
        for res in resposta:
            if data_get['bandeira'] == res['bandeira'] and data_get['cidade'] == res['cidade'] and data_get['numero'] == res['numero'] and data_get['cep'] == res['cep'] and data_get['distrito'] == res['distrito']:
                data_finish.append({'task_id': data_get['task_id'], 'cnpj': res['cnpj']})   
            else:
                continue

mylist_unique = [{'task_id': key, 'cnpj': max(item['cnpj'] for item in values)}
                 for key, values in groupby(data_finish, lambda dct: dct['task_id'])]       

print(mylist_unique)                   



print(len(data_finish))
print(len(mylist_unique))
for item in mylist_unique:
    items_update = update_tabela_person(item)
    print('Sucesso')
