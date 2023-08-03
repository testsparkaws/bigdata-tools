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
    survey_df.show()

    ############ Transformation #######################
    partitioned_survey_df = survey_df.repartition(2)
    transformation = Transformation()
    count_df = transformation.count_by_country(partitioned_survey_df)
    count_df.show()
    print(f"Collect : {count_df.collect()}")

    ########### SINK ###########################

    #input("press here , to debug in UI")
    logger.info("Finished HelloSpark")
    
