import os
import psycopg2
import os
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime, timedelta, timezone

load_dotenv()

# utc+7 (follow indonesia timezone)
date_yesterday = (datetime.now().astimezone(timezone(timedelta(hours=7)))-timedelta(days=1)).strftime("%Y-%m-%d")

database_url = "postgresql://rahasia:"+os.getenv("DB_PASSWORD")+"@db-task-7445.6xw.aws-ap-southeast-1.cockroachlabs.cloud:26257/defaultdb?sslmode=verify-full"

conn = psycopg2.connect(database_url)

with conn.cursor() as cur:
    cur.execute('''SELECT CAST(date as VARCHAR(50)) as date, name, role, task, level, note, "progress (%)" FROM defaultdb.task.daily_task''')
    res = cur.fetchall()    
    conn.commit()
 
df = pd.DataFrame(res, columns=["date", "name", "role", "task", "level", "note", "progress (%)"])
# print(df)
filtered_df = df.loc[df['date']==date_yesterday]
# print(filtered_df)

filtered_df.to_parquet(f"../datalake/bronze/{datetime.now().strftime('%Y-%m-%d')}-db.parquet", index=False)




