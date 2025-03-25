from pyspark.sql import SparkSession
import pyspark.sql.functions as F


class selectColumn():
    def __init__(self,spark):
        self.spark = spark

    def transformselect(self,path_read_bronze):

        df_sales_amazon = self.spark.read\
                             .format("parquet")\
                             .load(path_read_bronze)  



 
        df_select = ( 
                df_sales_amazon.select(

                       F.col("Order ID").alias("ID"),
                       F.col("Date").alias("data"),
                       F.col('Status').alias("status"),
                       F.col('Sales Channel').alias("vendas"),
                       F.col('Category').alias("categoria"),
                       F.col('Size').alias("tamanho"),
                       F.col('Qty').alias("qtd"),
                       F.col('Amount').alias("valor"),
                       F.lower(F.col('ship-city').alias("cidade")),
                       F.lower(F.col('ship-state').alias("estado")),
                       
                        )
                    )
        

        df_transform = (
                df_select
                    .withColumn("data", F.to_date(F.col("data"), "yyyy-MM-dd"))
                    .withColumn("valor", F.coalesce(F.col("valor"), F.lit("Cancelled")))
                )

        df_transform.show()

        return df_transform