class Source:
    def __init__(self):
        pass 

    def read_csv(self,spark, path):
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(path)
        
        return df 