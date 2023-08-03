from pyspark.sql import SparkSession
from pyspark.sql import * 
from source import Source
from transformation import Transformation

import sys 
sys.path.insert(1, '/Users/rajsingh/work/PYSPARK/pyspark-repo/')

from lib.logger import Log4J
from lib.utils import get_spark_app_config

if __name__ == '__main__':
    print("Starting Hello Spark")
    conf = get_spark_app_config()
    spark = SparkSession.builder \
         .config(conf=conf) \
         .enableHiveSupport() \
         .getOrCreate()
    conf_out = spark.sparkContext.getConf()
    print(f"CONF : {conf_out.toDebugString()}")
    logger = Log4J(spark)
    # if len(sys.argv) != 2:
    #     logger.error("Usage: HelloSpark <filename>")
    #     sys.exit(-1)
    
    flightTimeParquetDF = spark.read \
        .format("parquet") \
        .load("./data/flight*.parquet")
    flightTimeParquetDF.show()

    ###### spark is database also, so save as table rather then saving file ####
    spark.sql("CREATE DATABASE IF NOT EXISTS AIRLINE_DB")
    spark.catalog.setCurrentDatabase("AIRLINE_DB")
    # flightTimeParquetDF.write \
    #    .mode("overwrite") \
    #    .saveAsTable("flight_data_tbl")  
    

    # flightTimeParquetDF.write \
    #    .mode("overwrite") \
    #    .partitionBy("ORIGIN","OP_CARRIER") \
    #    .saveAsTable("flight_data_tbl")  

    # flightTimeParquetDF.write \
    #    .mode("overwrite") \
    #    .bucketBy(5,"OP_CARRIER", "ORIGIN") \
    #    .saveAsTable("flight_data_tbl")  

    ### if data is sorted it will be easliy accessible 
    flightTimeParquetDF.write \
      .format("csv") \
       .mode("overwrite") \
       .bucketBy(5,"OP_CARRIER", "ORIGIN") \
       .sortBy("OP_CARRIER", "ORIGIN") \
       .saveAsTable("flight_data_tbl")  
        
    print(spark.catalog.listTables("AIRLINE_DB"))
    # by default create table in default database
