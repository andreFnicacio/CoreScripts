# %%
# listagem de tasks finalizadas
from ssl import VerifyFlags
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
data_inicial = '2022-06-01T00:00:00.000000'
data_final = '2022-06-14T00:00:00.000000'

final_dataframe = list()

if(company == 'apsen'):
    # column_dataframe = ["SUBCANAL",
    #     "task_id",
    #       "BANDEIRA",
    #       "GRUPO",
    #       "CNPJ",
    #       "",
    #       "",
    #       "",
    #       "",
    #       "",
    #       "DESCRICAO_CUP",
    #       "Local encontrado",
    #       "Encontrou Lactosil Flora",
    #       "Diferença entre Lactosil Flora e concorrentes",
    #       "Posicionamento das caixas na prateleira Lactosil Flora",
    #       "Tinha mensagem LEVE +8 CAPSULAS Lactosil Flora",
    #       "Localizou o preço do Lactosil Flora na prateleira",
    #       "Havia promoção sinalizada na etiqueta Lactosil",
    #       "Localizou o preço do Lactosil na prateleira",
    #       "Encontrou Lactosil 10,000 FCC 30tbl",
    #       "Posicionamento das caixas na prateleira Lactosil",
    #       "Tinha mensagem LEVE +8 CAPSULAS Lactosil",
    #       "Localizou o preço do Lactosil na prateleira",
    #       "Havia promoção sinalizada na etiqueta Lactosil",
    #       "Preço unitário Lactosil",
    #       "Foto Lactosil ou prateleira",
    #       "Encontrou Inilok 30cpr",
    #       "Preço unitário Inilok",
    #       "Medicamento oferecido como opção mais barata ao Inilok",
    #       "Medicamento oferecido na troca do Inilok",
    #       "Medicamento oferecido como opção mais barata ao Inilok",
    #       "Preço unitário do produto indicado"]
    column_dataframe = [
            'task_id',
            'BANDEIRA',
            'GRUPO',
            'CNPJ',
            'DESCRICAO_CUP',
            'O que encontrou no local',
            'Encontrou Motilex HA com 30 cápsulas na prateleira',
            'Encontrou preço do Motilex HA na prateleira',            
            'Preço informado do Motilex HA na prateleira',
            'Posicionamento da(as) caixa(s) do Motilex HA na prateleira',
            'Mensagem de Leve + 12 Dias',
            'Marca sugerida colageno para articulações',
            'Preço do Motilex HA de acordo com o balconista',
            'Foi informado sobre programa de desconto do laboratório Sou Mais Vida',
            'Encontrou alguma comunicação sobre o programa de desconto do laboratório Sou Mais Vida',
            'Foto prateleira Motilex HA',
            'Indicação de remédio para enjoo',
            'Opção que dá menos sonolência e age mais rápido',
            'Entao qual a marca indicada?',
            'Opção mais barato que não causa sonolência',
            'Meclin é a mesma coisa']
elif(company == 'alelo'):
    column_dataframe = ["RAZAO_SOCIAL",
              "EC",
              "AUDITORIA",
              "ENDERECO",
              "CIDADE",
              "ESTADO",
              "EXISTE_1",        
              "RESPONSAVEL",
              "DIA_FUNCIONAMENTO",
              "HORA_FUNCIONAMENTO",
              "ANTECIPACAO",      
              "FOTO_1",                                       
              "FOTO_2",                                            
              "ERRADO_ALIMENTACAO_1", 
              "ERRADO_ALIMENTACAO_2",                                
              "FOTO_3",             
              "LATITUDE",
              "LONGITUDE",             
              "FOTO_4"
              ]  

task_id = ''

# FUNÇOES RESPONSAVEL POR ACESSAR E TRAZER DADOS DO DYNAMO


def buscar_tabela_in_person():
    table = dynamodb.Table('table_micro_task_in_person')
    params = dict(
        FilterExpression='#company = :c and #status.#status = :s and #status.#state = :f',
        ExpressionAttributeNames={'#company': 'company','#status': 'status', '#state': 'state'},
        ExpressionAttributeValues={':c': company,
                                   ':s': 'SUCCESS', 
                                   ':f': 'FINISHED'}
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
                'CNPJ': data['original'].get('CNPJ', ''),
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
    count = 0
    table = dynamodb.Table('table_micro_task_execution')
    params = dict(
        FilterExpression='company = :c AND audit.approved = :a AND #audit.#when BETWEEN :dtI AND :dtF AND #result = :f',
        ExpressionAttributeNames={"#audit": "audit", "#when": "when", "#result": "result"},
        ExpressionAttributeValues={
            ':c': company,
            ':a': True,
            ':dtI': data_inicial,
            ':dtF': data_final,
            ':f': 'FINISH'
        }
    )

    # response = table.cam(**params)

    tasks = list()

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
    
    return tasks

# SEÇAO RESPONSAVEL POR CAPTURAR DADOS DO DYNAMO E ENCAMINHAR PARA O ARQUIVO
all_execution = buscar_tabela_execution()

# %%
def parse_execution(tasks):
    finishedTask = list()
    for data in tasks:
        execution = list()
        for itemExec in data['execution']:
            verify_column = itemExec.get('context', None)     
            verify_response = itemExec.get('response', None)     
            style = itemExec.get('style', None)
            if(company == 'apsen'):
                for columns in column_dataframe:
                    if style != 'photo':
                        if verify_column == columns and verify_column != 'O que encontrou no local':
                                response_flow = {
                                    'resposta': itemExec.get('response', None),
                                    'pergunta': verify_column
                                }
                                execution.append(response_flow)  
                        elif verify_column == 'O que encontrou no local' and verify_response != None:
                                response_flow = {
                                    'resposta': itemExec.get('response', None),
                                    'pergunta': verify_column
                                }
                                execution.append(response_flow)  
                        else:
                            response_flow = {
                                'resposta': '',
                                'pergunta': ''
                            }
                            execution.append(response_flow)          
                    else:
                        if verify_column != 'Foto prateleira Probians' and  verify_column != 'Foto Extima Lata na prateleira' and verify_column != None:
                            try:
                                final_dataframe.remove(verify_column)
                            except:
                                print()
                            response_photos = itemExec.get('response', "vazio")
                            for i, foto in enumerate(response_photos):
                                name = f'{verify_column}_{i+1}'
                                if name not in final_dataframe:
                                    final_dataframe.append(name)
                                response_flow = {
                                        'resposta': foto,
                                        'pergunta': name
                                    }                                  
                                execution.append(response_flow)
                            
                                                           
            else:
                for columns in column_dataframe:
                    if verify_column == columns:
                        exec = {
                            'resposta': str(itemExec.get('response', None)),
                            'pergunta': verify_column
                        }
                        execution.append(exec)


        if(company == 'apsen'):
            item = {
                'Start': data['when']['start'],
                'Finish': data['audit']['when'],
                'task_id': data['task_id'],
                'execution_id': data['execution_id']
            }

            for execAtual in execution:
                pergunta = execAtual.get('pergunta', None)
                resposta = execAtual.get('resposta',None)
                item[pergunta] = resposta
            finishedTask.append(item)        

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

final_dataframe = list(column_dataframe)
execution = parse_execution(all_execution)
print(final_dataframe)
print(1)

# %%
print('executuion carregada')
print('buscando in person')
inperson = buscar_tabela_in_person()

# %%

data = list()
print(final_dataframe)

for exec in range(len(execution)):
    taskid = execution[exec]['task_id']
    #data_inperson = list(filter(lambda item: item['task_id'] == task_id, inperson))
    res = [x for x in inperson if x['task_id'] == taskid]
    if(res != []):
        data.append(res[0] | execution[exec])
    else:
        continue

print(len(data))
print('Iniciando DataFrame')        
dataFrame = pd.DataFrame(data)
dataFrame.to_excel(f'cru.xlsx', index=False)

dataFrame = dataFrame.reindex(columns=final_dataframe)

dataFrame.to_excel(f'execucoes_junho_{company}.xlsx', index=False)
print('Finalizado')

# %%