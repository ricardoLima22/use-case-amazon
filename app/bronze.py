from pyspark.sql import SparkSession
import pyspark.sql.functions as F


class loadFile():
    def __init__(self,spark):
        self.spark = spark
    
    def bronze_file(self,path_read,path_write):
            
        df_sales_amazon = self.spark.read\
                                  .format("csv")\
                                  .option("header", True)\
                                  .option("inferSchema", True)\
                                  .option("sep", ",")\
                                  .load(path_read)


        df_sales_amazon.write\
                       .format("parquet")\
                       .mode("overwrite")\
                       .save(path_write)

        return True
        














