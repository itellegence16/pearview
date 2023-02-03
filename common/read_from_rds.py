import json
from pyspark.sql import SparkSession,SQLContext
config_file = open("cdm_rds.json")
json_file_config_details=json.load(config_file)
def read_from_rds(table_name):
        spark = SparkSession.builder.master('local').appName('RDS connectivity').getOrCreate()
        df = spark.read.format("jdbc").option("url",json_file_config_details["host"])\
         .option("user",json_file_config_details["user"])\
         .option("password", json_file_config_details["password"])\
         .option("driver",json_file_config_details["driver"])\
         .option("query",json_file_config_details["query"]+table_name).load()
        df.show()
        #df.toPandas().to_csv(source_dir+json_file_config_details["rds"][i]["table_name"]+"_rds.csv",index=False)
        return df
