###### Tested with Spark 3.0.3 ######
####### Section 2: Installing and Using Apache Spark ####
#8. Setup Your Databricks Community Cloud Enviorment 
=> Complete free envrionment -> Community Edition 

# 
https://community.cloud.databricks.com/

######### Section 3: Spark Execution Model and Architecture ###############
## Working with pyspark shell - DEMO ##


####### Section 4: Spark Programming Model and Developer Experience ##########
# Creating spark Project Build Configuration  

#28.  DataFrame Introduction 
# Read > Process > Write 
https://spark.apache.org/docs/latest/api/python/index.html
https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.csv.html


Spark Action 
------------
1. Read 
2. Write 
3. Collect 
4. Show 


######### Section 5: Spark Structured API Foundation #########
RDD -> Resilient Distributed Dataset 

#37. Working with Spark SQL 
#38. Spark SQL and Catalyst Optimizer 
1. Analysis 
2. Logical Optimization 
3. Pysical Planning 
4. Code Generation

######## Section 6: Spark Data Sources and Sinks ############
#Spark Data Sources and SInks *** Very Very Important to Understand 

Internal
========
- HDFS 
- Amazon S3 
- Azure Blob 
- Google Cloud 

1. CSV / JSON/PARQUET / AVRO / Plain TEXT 
1. Spark SQL Tables 
2. Delta Lake 

External
======= 
1. JDBC Data Sources 
2. No SQL Data Systems
   - Cssandra, MongoDB
3. Cloud Warehoused 
  - Snowflake, redshift 
4. Stream Integrators 
  - Kafka , Kinesis
  
#41. Spark DataFrameReader API 
DataFrameReader
    .format(...)
    .option("key","value")
    .schema(...)
    .load()

## 
spark.read
   .format("csv")
   .option("header","true")
   .option("path","/data/mycsvfiles")
   .option("mode","FAILFAST")
   .schema(myschema)
   .load()

# Instead of Load used csv() method , dont use this 
# Built in Format : csv/json/parquet/orc/jdbc 
# Community Formats: Cassandra,MonogDB,AVRO,XML,HBase,Redshift 
# Read Mode: PERMISSIVE, DROPMALFORMED,FAILFAST
# Shcmea[Optional]: Explicit, Infer Schema, Implicit 

#42. Reading CSV,JSON and Parquet files 
# https://spark.apache.org/docs/3.0.1/api/python/pyspark.sql.html#pyspark.sql.DataFrame


### 43. Creating Spark DataFrame Schema 
=> explicity setting schema for dataframe 
=> Spark Data Types [ pyspark.sql.types ]
=> Spark has their own Data Types 
SparkType      / Python Type 
1. IntegerType / Int 
2. LongType   / long 
3. FloatType / Float 
4. DoubleType / Float 
5. StringTYpe / String 

-- Spark Schema defined 2 ways 
=> programmtically
** StructType([StructField()])

=> using DDL String 
flightSchemaDDL = """FL_DATE DATE, OP_CARRIER STRING, OP_CARRIER_FL_NUM INT, ORIGIN STRING, 
          ORIGIN_CITY_NAME STRING, DEST STRING, DEST_CITY_NAME STRING, CRS_DEP_TIME INT, DEP_TIME INT, 
          WHEELS_ON INT, TAXI_IN INT, CRS_ARR_TIME INT, ARR_TIME INT, CANCELLED INT, DISTANCE INT"""

# 44. Spark DataframeWriter API / Data SInk 

Genearlal Strucutre 
-------------------
DataFrameWriter 
   .format(...)
   .option(...)
   .partitionBy(...)
   .bucketBy(...)
   .sortBy(...)
   .save()

Indicative Example 
------------------
dataframe.write
    .format("parquet")
    .mode(saveMode) 
    .option("path","/data/flights")
    .save()

Spark File Layout 
------------------
1. Number of files and file size 
2. Paritions and Buckets 
3. Storing sorted Data 

=> Dataframe.repartition(n) : control number of output files 
=> dataframe.paritionBy(col1,col2)
=> dataframe.bucketBy(n, col1, col2) : only managed table 
=> sortBy()
=> maxRecordsPerFile() 

# Built in Format : csv/json/parquet-> default/orc/jdbc 
# Community Formats: Cassandra,MonogDB,AVRO,XML,HBase,Redshift 
# Save Mode: append , overwrite, errorIfExists, ignore 
# Option : according to 


#45. Writing your data and managing Layout *** very very important 
1. Partition Elimination 
2. 
https://spark.apache.org/docs/3.0.0-preview/sql-data-sources-avro.html

# 46 . Spark databases and Tables 
- Database 
   * Tables [ Table Data -> spark Warehouse , Table Metdata => Catalog Metastore ]
   * Views [ Table Metdata => Catalog Metastore ]

- Spark Tables 
   * Managed Tables 
     dataframe.write.saveAsTable("your_table_name") # location managed by spark
   * Unmanaged Tables ( External tables )
    create table table_name(col1, col2, col3 )
    LOCATION '<>' # location mentioned by user not managed in spark

# 47. Working with Spark SQL Tables 
 - if we used paritionBy() and parition by column has lot of values 100 thousand values then dont use it  instead use bucketBy() 
 - it create metastore_db & spark-warehouse
 - BucketBy()
   - hash() # mod(5)

################# Section 7: Spark Dataframe and Dataset Transformation #################
# Transformation 
 1. Combining Data Frames 
 2. Aggregating And Summarizing 
 3. Applying functions and built-in Transformation 
 4. Using Built-in and column-level functions 
 5. Creating and using UDFs 
 6. Referencing Rows/Columns 
 7. Creating Column Expressions

 #49. Working with DataFrame rows 
 -- test notebook in databricks cluster 

#51. Dataframw Rows and Unstructured data 
 - spark.read.text()
 - regexp_extract() , substring_index()

#52. Working with Dataframe Columns 
 - What is column and how to reference it 
 - Columns String 
 - Column Object 
 - # How to create column Expression [ String ]
    * String Expression or SQL Expression 
 -  DataFrame/Built-in Functions 
  -  Check Column class Documentaiton ** from here only they created Column class [ HARRY]

# 53. UDF : User Defined Functions 

# 54. Misc 
1. Quick Method to create dataframes 
2. Adding Monotonically Increasing ID 
3. Using case when Then transformation 
4. Casting Your columns 
5. Adding Columns to Dataframes 
6. Dropping Columns 
7. Dropping duplicate rows 
8. sorting Dataframes 

python3 modules/RowDemo.py 


######### Section 8: Aggregation in Apache spark ########
#55. Aggregating Dataframes 
 1. Simple Aggregation 
 2. Grouping Aggregation 
 3. Windowing Aggregations 

 Aggreting Funtion 
 - avg(), count(), max(), min(),sum() 
 Windows functions 
  - lead(), lag() , rank(), dense_ran(), cume_dist()

# 57. Window Aggregtation 
1. Identify Your Partitioning Columns 
2. Identify your ordering requirements
3. Define your window starnt and end .

######### Section 9: Spark DataFrame Joins  ########
# 58. Dataframe Joins and Column name ambiguity 
  -- 1. Join Condition & Expression : 
  -- 2. Join type 

# 59. Outer Join in Dataframe 
   -- Left Outer, Right outer, Full Outer 

# 60. Internal of Spark Join and shuffle 
  -- ShuffleJoin 
  -- BroadcastJoin 
  
# 61. Optimizing Your Joins 
# 62. Implementing Bucket Joins 
