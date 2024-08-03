import requests, json, os
from dotenv import load_dotenv
import pandas as pd
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
                    items_page{
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
        'date' : i['column_values'][2]['text'],
        'person' : i['column_values'][0]['text'],
        'role' : i['column_values'][1]['text'],
        'task': i['name'],
        'level' : i['column_values'][3]['text'],
        'note' : i['column_values'][4]['text'],
        'progress (%)' : i['column_values'][5]['text']
    }

    final_res.append(tmp)

df = pd.DataFrame(final_res)
df_filtered = df.loc[df['date'] == date_yesterday]

# print(df_filtered)

df_filtered.to_parquet(f"../datalake/bronze/{datetime.now().strftime('%Y-%m-%d')}-api.parquet", index=False)