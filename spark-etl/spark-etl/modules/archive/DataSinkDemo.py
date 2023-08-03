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

    ### spark.jars.packages org.apache.spark:spark-avro_2.11:2.4.5 [ spark.defaults.conf]

    from pyspark.sql.functions import spark_partition_id

    print("Num Paritioned Before : "+str(flightTimeParquetDF.rdd.getNumPartitions()))
    flightTimeParquetDF.groupBy(spark_partition_id()).count().show()

    partitionedDF = flightTimeParquetDF.repartition(5)
    print("Num Paritioned After : "+str(partitionedDF.rdd.getNumPartitions()))
    partitionedDF.groupBy(spark_partition_id()).count().show()
    
    #### AVRO #########
    partitionedDF.write \
        .format("avro") \
        .mode("overwrite") \
        .option("path","datasink/avro/") \
        .save()
    
    ###### JSON  => why ? bczo avro created lot of files, so we paritioned ###
    flightTimeParquetDF.write \
         .format("json") \
         .mode("overwrite") \
         .option("path", "datasink/json/") \
         .partitionBy("OP_CARRIER","ORIGIN") \
         .option("maxRecordsPerFile",10000) \
         .save()




