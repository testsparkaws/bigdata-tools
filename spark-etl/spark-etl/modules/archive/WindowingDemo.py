from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as f



import sys 
sys.path.insert(1, '/Users/rajsingh/work/PYSPARK/pyspark-repo/')

from lib.logger import Log4J
from lib.utils import get_spark_app_config

if __name__ == '__main__':
    print("Starting Hello Spark")
    conf = get_spark_app_config()
    spark = SparkSession.builder \
         .config(conf=conf) \
         .getOrCreate()
    conf_out = spark.sparkContext.getConf()
    print(f"CONF : {conf_out.toDebugString()}")
    logger = Log4J(spark)
    
    logger.info("Starting HelloSpark")

    invoice_df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("./data/invoices.csv")
    
    invoice_df.show()

    summary_df = spark.read.parquet("./output/")
    summary_df.show()

    running_total_window = Window.partitionBy("Country") \
                 .orderBy("WeekNumber") \
                 .rowsBetween(-2,Window.currentRow)
    
    summary_df.withColumn("RunningTotal", f.sum("InvoiceValue").over(running_total_window)).show()