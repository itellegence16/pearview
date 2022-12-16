import json
import pandas as pd
import numpy as np
from pyspark import SparkContext
from pyspark.sql import SparkSession,SQLContext
import mysql.connector
from mysql.connector import Error
import getpass
db_name="pearview"
#table_details=str(input("Enter the table name: e.g. student, course, fee: ")).lower()
table_details="student"
config_dir=r"C:\Users\Sana Mahajan\Documents\git_practice\pearview\config\\"
source_dir=r"C:\Users\Sana Mahajan\Documents\git_practice\pearview\\"+table_details+"\\"
config_file_mysql_table = open(config_dir+"mysql_config_read.json")
json_file_mysql_table=json.load(config_file_mysql_table)

def read_from_mysql():
  for i in range(0,len(json_file_mysql_table["config"])):
   try:
    spark = SparkSession.builder.config \
          ("spark.driver.extraClassPath",json_file_mysql_table["config"][i]["spark_driver_path"] ) \
          .master("local").appName("Mysql connectivity").getOrCreate()
    password = getpass.getpass("Enter the password for MYSQL workbench user "+json_file_mysql_table["config"][i]["user"]+ ": ")

    df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:"+json_file_mysql_table["config"][i]["mysql_host_url"]\
                +":"+json_file_mysql_table["config"][i]["mysql_port"]+"/"+json_file_mysql_table["config"][i]["db_name"]) \
        .option("driver", json_file_mysql_table["config"][i]["driver"]) \
        .option("db_table", json_file_mysql_table["config"][i]["table_name"]) \
        .option("user", json_file_mysql_table["config"][i]["user"]) \
        .option("password", password)\
        .option("query",json_file_mysql_table["config"][i]["query"]+json_file_mysql_table["config"][i]["table_name"]).load()
    df.toPandas().to_csv(source_dir+json_file_mysql_table["config"][i]["table_name"]+str(i)+".csv",index=False)
   except:
       print("Please enter correct password for MYSQL workbench user "+json_file_mysql_table["config"][i]["user"])


read_from_mysql()