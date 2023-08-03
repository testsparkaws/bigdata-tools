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
    
    logger.info("Starting HelloSpark")

    ### Read CSV ###
    from pyspark.sql.types import StringType,StructField,StructType, DateType, IntegerType
    flightSchemaStruct = StructType([
        StructField("FL_DATE", DateType()),
        StructField("OP_CARRIER", StringType()),
        StructField("OP_CARRIER_FL_NUM", IntegerType()),
        StructField("ORIGIN", StringType()),
        StructField("ORIGIN_CITY_NAME", StringType()),
        StructField("DEST", StringType()),
        StructField("DEST_CITY_NAME", StringType()),
        StructField("CRS_DEP_TIME", IntegerType()),
        StructField("DEP_TIME", IntegerType()),
        StructField("WHEELS_ON", IntegerType()),
        StructField("TAXI_IN", IntegerType()),
        StructField("CRS_ARR_TIME", IntegerType()),
        StructField("ARR_TIME", IntegerType()),
        StructField("CANCELLED", IntegerType()),
        StructField("DISTANCE", IntegerType())
    ])

    # FL_DATE,OP_CARRIER,OP_CARRIER_FL_NUM,ORIGIN,ORIGIN_CITY_NAME,DEST,DEST_CITY_NAME,CRS_DEP_TIME,DEP_TIME,
    # WHEELS_ON,TAXI_IN,CRS_ARR_TIME,ARR_TIME,CANCELLED,DISTANCE

    flightTimeCSVDF = spark.read \
          .format("csv") \
          .option("header","true") \
          .schema(flightSchemaStruct) \
          .option("mode", "FAILFAST") \
          .option("dateFormat","M/d/y") \
          .load("./data/flight*.csv")
          #.option("inferSchema","true") \
    
    flightTimeCSVDF.show(5)
    print("CSV Schema: "+flightTimeCSVDF.schema.simpleString())

    flightSchemaDDL = """FL_DATE DATE, OP_CARRIER STRING, OP_CARRIER_FL_NUM INT, ORIGIN STRING, 
          ORIGIN_CITY_NAME STRING, DEST STRING, DEST_CITY_NAME STRING, CRS_DEP_TIME INT, DEP_TIME INT, 
          WHEELS_ON INT, TAXI_IN INT, CRS_ARR_TIME INT, ARR_TIME INT, CANCELLED INT, DISTANCE INT"""
    
   ### READ JSON
    flightTimeJSON = spark.read \
       .format("json") \
       .schema(flightSchemaDDL) \
       .option("dateFormat","M/d/y") \
       .load("./data/flight*.json")
    
    flightTimeJSON.show(5)
    print("JSON Schema: "+flightTimeJSON.schema.simpleString()) 


    ### Read Parquet 
    flightTimeParquet = spark.read \
       .format("parquet") \
       .load("./data/flight*.parquet")
    
    flightTimeParquet.show(5)
    print("Parquet Schema: "+flightTimeParquet.schema.simpleString()) 

