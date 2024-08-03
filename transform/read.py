from pyspark.sql import SparkSession
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

class ReadData:

    def __init__(self, type):
        # no need when in databricks
        self.spark = SparkSession.builder.\
                     master("spark://172.20.10.10:7077").\
                     appName("read data from bronze datalake").\
                     getOrCreate()
        
        self.type = type
        self.date = datetime.now().strftime("%Y-%m-%d")
        
    def readFromBronze(self):
        df = self.spark.read.\
                  format("parquet").\
                  load(f"datalake/bronze/{self.date}-{self.type}.parquet")
        
        return df
    
    def readPoints(self):
        df = self.spark.read.\
                  format("csv").\
                  option("header", "True").\
                  load(f"delta_live/points-{self.type}.csv")
        
        return df
    
def read_data(type_source, type_data):

    if type_source == "bronze":
        return ReadData(type_data).readFromBronze()
    elif type_source == "points":
        return ReadData(type_data).readPoints()
    else:
        raise ValueError(f"Unknown type_source of :{type_source}")
    

    




