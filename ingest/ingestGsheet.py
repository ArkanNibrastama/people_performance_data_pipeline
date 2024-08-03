import pandas as pd
from datetime import datetime, timedelta, timezone
import os
from dotenv import load_dotenv

load_dotenv()

df = pd.read_csv(os.getenv("GSHEET_URL"))

# utc+7 (follow indonesia timezone)
date_yesterday = (datetime.now().astimezone(timezone(timedelta(hours=7)))-timedelta(days=1)).strftime("%Y-%m-%d")

df_filtered = df.loc[df['date'] == date_yesterday]

df_filtered.to_parquet(f"../datalake/bronze/{datetime.now().strftime('%Y-%m-%d')}-gsheet.parquet", index=False)