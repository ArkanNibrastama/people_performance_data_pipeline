import pandas as pd
import pyarrow.parquet as pq

table = pq.read_table('datalake/bronze/01-08-2024-gsheet.parquet')
df = table.to_pandas()

print(df)