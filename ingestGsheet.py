import pandas as pd
from datetime import datetime, timedelta, timezone
import os
from dotenv import load_dotenv
import pyarrow as pa
import pyarrow.parquet as pq

load_dotenv()

df = pd.read_csv(os.getenv("GSHEET_URL"))

# utc+7 (follow indonesia timezone)
date_yesterday = (datetime.now().astimezone(timezone(timedelta(hours=7)))-timedelta(days=1)).strftime("%Y-%m-%d")

df_filtered = df.loc[df['date'] == date_yesterday]

table = pa.Table.from_pandas(df_filtered)
pq.write_table(table, f"datalake/bronze/{datetime.now()}-gsheet.parquet")  