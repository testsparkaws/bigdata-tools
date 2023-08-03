
# Creating DataFrames
df = spark.read.load("s3://${BUCKET_NAME}/input/lab2/sample.csv", 
                          format="csv", 
                          sep=",", 
                          inferSchema="true",
                          header="true")

type(df)

#DataFrame Operations
df.printSchema()
df.columnname or by indexing df['columnname']
df.count()
df.select('country')


####### Transformations #####
df.select('country').show()
df.select('country').show(10,truncate=False)
dfselect = df.select(df['Country'], df['ItemType'], df['SalesChannel'],df['TotalRevenue'])
dfselect.filter(df['Country'] == 'United Kingdom').show(10,truncate=False)

dfselectfilter = dfselect.filter((df['Country'] == 'United Kingdom') & (df['TotalRevenue'] <= 200000.00))



#Spark groupBy() function is used to group idential item in DataFram.
dfselectfiltergroupby = dfselectfilter.groupBy("ItemType").sum("TotalRevenue")

#Spark orderBy() function is used to sort one or more column in DataFrame.
dfselectfiltergroupbyorderby = dfselectfiltergroupby.orderBy("sum(TotalRevenue)", ascending=False)
dfselectfiltergroupbyorderby.show(10,truncate=False)




 