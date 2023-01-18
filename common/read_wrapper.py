
def read_from_csv(spark,path):
     df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
     return df
