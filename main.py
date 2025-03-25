from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import app.bronze as bronze
import app.bronze_save_to_silver as bronzeToSilver

    
def main():
    spark = SparkSession.builder.master("local[*]").appName("bronzemain").config('spark.sql.repl.eagerEval.enabled', True).getOrCreate()
    
    path_doc_amazon = r".\docs\AmazonSaleReport.csv"
    path_doc_amazon_write = r".\arqBronze"

    processando_arquivo = bronze.loadFile(spark)
    processando_arquivo.bronze_file(path_doc_amazon,path_doc_amazon_write)


    path_doc_bronze = r".\arqbronze\*"

    teste = bronzeToSilver.selectColumn(spark)
    teste.transformselect(path_doc_bronze)


if __name__ == "__main__":
    main()
