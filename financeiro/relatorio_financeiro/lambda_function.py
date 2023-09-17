# %%
import json
from train2 import start_cron


def lambda_handler(event, context):
    statusCode, body = 500, {}
    statusCode, body = start_cron(event, context)

    return {
        'statusCode': statusCode,
        'body': json.dumps(body),
        'headers': {
            'Access-Control-Allow-Origin': '*'
        }
    }


lambda_handler(None, None)




#%%
# from uuid import uuid4
# from datetime import datetime
#
# print(datetime.now())
#
# print(uuid4())
#
