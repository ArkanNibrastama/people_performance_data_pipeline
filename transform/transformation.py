from pyspark.sql.functions import lit, lower
from read import read_data
from datetime import datetime

# read data
df_api = read_data("bronze", "api")
df_db = read_data("bronze", "db")
df_gsheet = read_data("bronze", "gsheet")

df_points_api = read_data("points", "api")
df_points_db = read_data("points", "db")
df_points_gsheet = read_data("points", "gsheet")

# join point in IT division
cond_api = [
    lower(df_api.role) == df_points_api.role,
    lower(df_api.level) == df_points_api.level,
    df_api["progress (%)"].cast('int').between(df_points_api.min_progress, df_points_api.max_progress)
]
join_api = df_api.join(df_points_api, cond_api, 'inner').select(
    df_api.date,
    df_api.person.alias("name"),
    df_points_api.points
).withColumn("division", lit("IT"))
# join_api.show()

# join point in RnD division
cond_db = [
    lower(df_db.role) == df_points_db.role,
    lower(df_db.level) == df_points_db.level,
    df_db["progress (%)"].cast('int').between(df_points_db.min_progress, df_points_db.max_progress)
]
join_db = df_db.join(df_points_db, cond_db, 'inner').select(
    df_db.date,
    df_db.name,
    df_points_db.points
).withColumn("division", lit("RnD"))
# join_db.show()

# join point in sales division
cond_gsheet = [
    lower(df_gsheet.type) == df_points_gsheet.type,
    df_gsheet.value.between(df_points_gsheet.min_value, df_points_gsheet.max_value)
]
join_gsheet = df_gsheet.join(df_points_gsheet, cond_gsheet, 'inner').select(
    df_gsheet.date,
    df_gsheet.name,
    df_points_gsheet.points
)   
join_gsheet = join_gsheet.withColumn("divison", lit("sales"))
# join_gsheet.show() 

# gether all dataframe
df = join_api.union(join_db.union(join_gsheet))
# df.show()

df.write.\
   format("parquet").\
   mode("Overwrite").\
   save(f"../datalake/silver/{datetime.now().strftime('%Y-%m-%d')}/")
