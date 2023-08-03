from pyspark.sql import SparkSession
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

    #### Very very Imortant ###
    NumInvoices = f.countDistinct("InvoiceNo").alias("NumInvoices")
    TotalQuantity = f.sum("Quantity").alias("TotalQuantity")
    InvoiceValue = f.expr("round(sum(Quantity*UnitPrice),2) as InvoiceValue")

    exSummary_df = invoice_df \
                 .withColumn("InvoiceDate", f.to_date(f.col("InvoiceDate"),"dd-MM-yyyy H.mm")) \
                 .where("year(InvoiceDate) == 2010") \
                 .withColumn("WeekNumber", f.weekofyear(f.col("InvoiceDate"))) \
                 .groupBy("Country","WeekNumber") \
                 .agg(NumInvoices,TotalQuantity,InvoiceValue)


    exSummary_df.coalesce(1) \
       .write \
       .format("parquet") \
       .mode("overwrite") \
       .save("output")
    
    exSummary_df.sort("Country","WeekNumber").show()

    # InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country