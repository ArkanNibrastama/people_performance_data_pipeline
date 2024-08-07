from pyspark.sql.functions import lower
from read import read_data
from transformation import JoinPoints, UnionDataframe
from datetime import datetime

# read data
df_api = read_data("bronze", "api")
df_db = read_data("bronze", "db")
df_gsheet = read_data("bronze", "gsheet")

df_points_api = read_data("points", "api")
df_points_db = read_data("points", "db")
df_points_gsheet = read_data("points", "gsheet")

# rename column
df_api = df_api.withColumnRenamed('person','name')

# join
cond_api = [
    lower(df_api.role) == df_points_api.role,
    lower(df_api.level) == df_points_api.level,
    df_api["progress (%)"].cast('int').between(df_points_api.min_progress, df_points_api.max_progress)
]
joined_api = JoinPoints().transform(
                main_table=df_api, 
                point_table=df_points_api, 
                condition=cond_api,
                division="IT"
            )

cond_db = [
    lower(df_db.role) == df_points_db.role,
    lower(df_db.level) == df_points_db.level,
    df_db["progress (%)"].cast('int').between(df_points_db.min_progress, df_points_db.max_progress)
]
joined_db = JoinPoints().transform(
                main_table=df_db, 
                point_table=df_points_db, 
                condition=cond_db,
                division="RnD"
            )

cond_gsheet = [
    lower(df_gsheet.type) == df_points_gsheet.type,
    df_gsheet.value.between(df_points_gsheet.min_value, df_points_gsheet.max_value)
]
joined_gsheet = JoinPoints().transform(
                main_table=df_gsheet, 
                point_table=df_points_gsheet, 
                condition=cond_gsheet,
                division="RnD"
            )

# union
dfs = [joined_api, joined_db, joined_gsheet]
df = UnionDataframe().transform(dfs)

# df.show()

df.write.\
   format("parquet").\
   mode("Overwrite").\
   save(f"../datalake/silver/{datetime.now().strftime('%Y-%m-%d')}/")
