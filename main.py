from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import app.bronze as bronze
import app.bronze_save_to_silver as bronzeToSilver
import app.silver as silver

    
def main():
    spark = SparkSession.builder.master("local[*]").appName("bronzemain").config('spark.sql.repl.eagerEval.enabled', True).getOrCreate()
    
    path_doc_amazon = r".\docs\AmazonSaleReport.csv"
    path_doc_amazon_write = r".\arqBronze"

    processando_arquivo = bronze.loadFile(spark)
    processando_arquivo.bronze_file(path_doc_amazon,path_doc_amazon_write)


    path_doc_bronze = r".\arqbronze\*"

    select_bronze_to_silver = bronzeToSilver.selectColumn(spark)
    select_bronze_to_silver.transformselect(path_doc_bronze)

    path_doc_silver = r".\arqSilver\*"

    processando_arquivo_silver = silver.loadSilver(spark)
    processando_arquivo_silver.loadSilver(path_doc_silver)

if __name__ == "__main__":
    main()
