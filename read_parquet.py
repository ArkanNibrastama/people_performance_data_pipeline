import pandas as pd

df = pd.read_parquet('datalake/bronze/2024-08-05-db.parquet')

print(df)