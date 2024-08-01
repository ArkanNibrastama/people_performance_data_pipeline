import pandas as pd
import pyarrow.parquet as pq

table = pq.read_table('dataalke/bronze/01-08-2024-api.parquet')
df = table.to_pandas()

print(df)