import pyspark
from read import read_data

df_api = read_data("bronze", "api")
df_api.show()

df_db = read_data("bronze", "db")
df_db.show()

df_gsheet = read_data("bronze", "gsheet")
df_gsheet.show()

df_points_gsheet = read_data("points", "gsheet")
df_points_gsheet.show()
