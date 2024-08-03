import pandas as pd

df = pd.read_parquet('datalake/bronze/2024-08-03-api.parquet')

print(df)