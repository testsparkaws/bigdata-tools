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
    if len(sys.argv) != 2:
        logger.error("Usage: HelloSpark <filename>")
        sys.exit(-1)
    
    logger.info("Starting HelloSpark")

    ################## Source ###########################
    source = Source()
    survey_df = source.read_csv(spark, sys.argv[1])
    #survey_df.show()

    survey_df.createOrReplaceTempView("survey_tbl")
    countDF = spark.sql("select Country, count(1) from survey_tbl where age<40 group by Country")
    countDF.show()
