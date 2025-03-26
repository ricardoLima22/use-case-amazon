import pyspark.sql.functions as F
from pyspark.sql import SparkSession

class loadSilver():
    def __init__(self,spark):
        self.spark = spark

    def loadSilver(self,path_read_silver):
        
        df_sales_amazon_silver = self.spark.read\
                             .format("parquet")\
                             .load(path_read_silver)\
        
        
        df_sales_amazon_silver.show(20, False)

        return df_sales_amazon_silver