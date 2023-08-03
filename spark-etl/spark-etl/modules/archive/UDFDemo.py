from pyspark.sql import SparkSession
from pyspark.sql import * 

import sys , re 
sys.path.insert(1, '/Users/rajsingh/work/PYSPARK/pyspark-repo/')

from lib.logger import Log4J
from lib.utils import get_spark_app_config

def parse_gender(gender):
    female_pattern= r"^f$|f.m|w.m"
    male_pattern = r"^m$|ma|m.l"
    if re.search(female_pattern,gender.lower()):
        return "Female"
    elif re.search(male_pattern, gender.lower()):
        return "Male"
    else:
        return "Unknown"

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

    survey_df = spark.read \
        .option("header","true") \
        .option("inferSchema", "true") \
        .csv("./data/survey.csv")
    #survey_df.show(100, truncate = False )

    ## Register the UDF for column obect 
    from pyspark.sql.functions import udf ,expr
    from pyspark.sql.types import StringType

    parse_gender_udf = udf(parse_gender, returnType=StringType())
    print("Catalog Entry: ")
    [print(f) for f in spark.catalog.listFunctions() if "parse_gender" in f.name ]

    survey_df2 = survey_df.withColumn("Gender", parse_gender_udf("Gender"))
    survey_df2.show()

    ###### Register the UDF for SQL statemtn or sparkSQL 
    spark.udf.register("parse_gender_udf", parse_gender, StringType())
    print("Catalog Entry: ")
    [print(f) for f in spark.catalog.listFunctions() if "parse_gender" in f.name ]

    survey_df3 = survey_df.withColumn("Gender", expr("parse_gender_udf(Gender)"))
    survey_df3.show()

