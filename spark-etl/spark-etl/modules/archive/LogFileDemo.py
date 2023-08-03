from pyspark.sql import SparkSession
from pyspark.sql import * 

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

    file_df = spark.read.text("./data/apache_logs.txt")
    file_df.printSchema()

    # 11 Fields 
    log_reg = r'^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+) "(\S+)" "([^"]*)'

    from pyspark.sql.functions import regexp_extract, substring_index
    logs_df = file_df.select(
        regexp_extract('value', log_reg, 1).alias('ip'),
        regexp_extract('value', log_reg, 4).alias('date'),
        regexp_extract('value', log_reg, 6).alias('request'),
        regexp_extract('value', log_reg, 10).alias('referrer')
    )
    logs_df.show()

    #################  
    logs_df \
        .where("trim(referrer) != '-' ") \
        .withColumn("referrer", substring_index("referrer", "/",3)) \
        .groupBy('referrer') \
        .count() \
        .show(100, truncate= False)