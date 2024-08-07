from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

class Transformation:
    def __init__(self):
        self.spark = SparkSession.builder.\
                     master("spark://192.168.1.9:7077").\
                     appName("join dataframe").\
                     getOrCreate()

    def transform(self):
        pass


class JoinPoints(Transformation):
    
    def transform(self, main_table, point_table, condition, division):
        
        joined_df = main_table.join(point_table, condition, 'inner').select(
            main_table.date,
            main_table.name,
            point_table.points
        ).withColumn("division", lit(division))

        return joined_df


class UnionDataframe(Transformation):

    def transform(self, dfs):
        
        union_df = dfs[0]

        for df in dfs[1:]:
            
            union_df = union_df.union(df)

        return union_df