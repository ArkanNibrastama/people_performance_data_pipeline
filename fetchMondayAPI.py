import requests, json, os
from dotenv import load_dotenv
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timedelta, timezone

# utc+7 (follow indonesia timezone)
date_yesterday = (datetime.now().astimezone(timezone(timedelta(hours=7)))-timedelta(days=1)).strftime("%Y-%m-%d")


load_dotenv()

apiUrl = "https://api.monday.com/v2"
headers = {
    "Authorization" : os.getenv("TOKEN"), 
    "API-Version" : "2024-04"
    }

query = '''
            query { 
                boards(ids: 1896672101){
                    items_page (limit: 2){
                        items{
                            name
                            column_values{
                                column{
                                    title
                                }
                                value
                                text   
                            }
                        }
                    }
                }
            }
        '''

data = {'query' : query}

res = requests.post(url=apiUrl, json=data, headers=headers)
json_res = json.loads(res.text)
# print(json_res)

final_res = []
for i in json_res['data']['boards'][0]['items_page']['items']:

    tmp = {
        'task': i['name'],
        'person' : i['column_values'][0]['text'],
        'status' : i['column_values'][1]['text'],
        'date' : i['column_values'][2]['text'],
        'note' : i['column_values'][3]['text'],
    }

    final_res.append(tmp)

df = pd.DataFrame(final_res)
df_filtered = df.loc[df['date'] == date_yesterday]
table = pa.Table.from_pandas(df_filtered)
pq.write_table(table, f'datalake/bronze/{datetime.now()}-api.parquet')