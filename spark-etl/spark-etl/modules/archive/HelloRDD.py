from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

import sys 
sys.path.insert(1, '/Users/rajsingh/work/PYSPARK/pyspark-repo/')

from lib.logger import Log4J
from lib.utils import get_spark_app_config
from collections import namedtuple

SurveyRecord = namedtuple("SurveyRecord",["Age","Gender","Country","State"])

if __name__ == '__main__':
    print("Starting Hello Spark")
    conf = get_spark_app_config()
    spark = SparkSession.builder \
         .config(conf=conf) \
         .getOrCreate()
    
    #sc = SparkContext(conf=conf)
    #print(sc)
    sc = spark.sparkContext
    logger = Log4J(spark)

    if len(sys.argv)!=2:
        print("usage: HelloRDD <FileName>")
        sys.exit(-1)
    
    linesRDD = sc.textFile(sys.argv[1])
    partitionedRDD = linesRDD.repartition(2)
    #print(linesRDD.take(10))

    colsRDD = linesRDD.map(lambda line: line.split(",")) # list of list 
    #print(colsRDD.take(1))

    selectRDD = colsRDD.map(lambda cols: SurveyRecord(int(cols[1]), cols[2], cols[3], cols[4]))
    #print(selectRDD.take(1))
    # [SurveyRecord(Age=37, Gender='"Female"', Country='"United States"', State='"IL"')]

    filteredRDD = selectRDD.filter(lambda r: r.Age < 40 )

    ### Need to Group By 
    kvRDD = filteredRDD.map(lambda r: (r.Country,1))
    #print(kvRDD.take(1))

    ### Now we can reduce it ###
    countRDD = kvRDD.reduceByKey(lambda v1,v2: v1+v2)
    print(countRDD.take(1))

    colsList = countRDD.collect()
    for x in colsList:
        print(x, len(x[0]))



