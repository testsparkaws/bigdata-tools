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

    ##### Approach1 ######
    invoice_df.select(
         f.count("*").alias("Count *"),
         f.sum("Quantity").alias("TotalQuantity"),
         f.avg("UnitPrice").alias("AvgPrice"),
         f.countDistinct("InvoiceNo").alias("CountDistinct")
    ).show()

    ##### Approach2 ######
    invoice_df.selectExpr(
       "count(1) as `count 1`",
       "count(StockCode) as `Count field`",
       "sum(Quantity) as TotalQuantity", 
       "avg(unitPrice) as AvgPrice"
    ).show()


    ####### Approach 1 #########
    invoice_df.createOrReplaceTempView("sales")
    summary_sql = spark.sql("""
         Select Country, InvoiceNo,
           sum(Quantity) as TotalQuantity, 
           round(sum(Quantity * UnitPrice), 2) as InvoiceValue                    
         FROM sales 
         Group BY 1,2
    """)
    summary_sql.show()

    ####### Approach 2 #########
    summary_df = invoice_df \
        .groupBy("Country", "InvoiceNo") \
        .agg(f.sum("Quantity").alias("TotalQuantity"),
             f.round(f.sum(f.expr("Quantity * UnitPrice")), 2).alias("InvoiceValue"),
             f.expr("round(sum(Quantity * UnitPrice),2) as InvoiceValueExpr")
             )
    summary_df.show()
    # InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country