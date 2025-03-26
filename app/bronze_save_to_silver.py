from pyspark.sql import SparkSession
import pyspark.sql.functions as F


class selectColumn():
    def __init__(self,spark):
        self.spark = spark

    def transformselect(self,path_read_bronze):

        df_sales_amazon = self.spark.read\
                             .format("parquet")\
                             .load(path_read_bronze)\

 
        df_select = (
                df_sales_amazon.select(
                    F.col("Order ID").alias("ID"),
                    F.col("Date").alias("data_venda"),
                    F.col("Status").alias("status"),
                    F.col("Sales Channel").alias("vendas"),
                    F.col("Category").alias("categoria"),
                    F.col("Size").alias("tamanho"),
                    F.col("Qty").alias("qtd"),
                    F.col("Amount").alias("valor"),
                    F.lower(F.col("ship-city")).alias("cidade"),
                    F.lower(F.col("ship-state")).alias("estado"),
                )
                .withColumn(
                    "valor", F.coalesce(F.col("valor"), F.lit(0.00))  # Colocando valores nulos como 0.00
                )
                .withColumn(
                    "estado", F.coalesce(F.col("estado"), F.lit("não informado"))
                )
                .withColumn(
                    "cidade", F.coalesce(F.col("cidade"), F.lit("não informado"))  # Colocando valores nulos como "não informado"
                )
            )



        df_transform = df_select.withColumn(
                "data_venda", 
                F.when(F.col("data_venda").rlike(r"^\d{2}-\d{2}-\d{4}$"), F.to_date(F.col("data_venda"), "dd-MM-yyyy"))
                .when(F.col("data_venda").rlike(r"^\d{2}-\d{2}-\d{2}$"), F.to_date(F.col("data_venda"), "MM-dd-yy"))
                .otherwise(None)
            )


        df_transform.write\
                    .format("parquet")\
                    .mode("overwrite")\
                    .save(r".\arqSilver")
        

        return df_transform