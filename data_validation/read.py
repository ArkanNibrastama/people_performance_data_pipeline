from pyspark.sql import SparkSession
from datetime import datetime

class ReadData:

    def __init__(self):
        self.spark = SparkSession.builder.\
                                  master("spark://192.168.1.9:7077").\
                                  appName("get data for validation").\
                                  getOrCreate()
        
        # self.date = datetime.now().strftime("%Y-%m-%d")
        self.date = "2024-08-05"


    def read(self):
        
        df = self.spark.read.\
                        format("parquet").\
                        load(f"../datalake/silver/{self.date}/")
        
        return df