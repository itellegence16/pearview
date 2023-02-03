import json
import pandas as pd
import numpy as np
from pyspark import SparkContext
from pyspark.sql import SparkSession,SQLContext
from mysql.connector import Error
import getpass
import sys
import boto3
from io import StringIO
# from mapping_data import table_details
from settings import *
from dotenv import load_dotenv
from common.read_wrapper import read_from_csv

table_details = sys.argv[1]
bucket=os.getenv('bucket')
key=os.getenv('key')
# table_details = str(input("Enter the table name: e.g. student, course, college, admission: ")).lower()
# table_details="student"
print(table_details)
# config_dir=r"C:\Users\Sana Mahajan\Documents\git_practice\pearview\config\\"+table_details+"\\"
# source_dir=r"C:\Users\Sana Mahajan\Documents\git_practice\pearview\\source_input\\"+table_details+"\\"

source_dir = source_dir + "\\" + table_details + "\\"
config_dir = dir_name + "\\" + table_details + "\\"
config_file = open(config_dir+"read_source_"+table_details+"_config.json")
json_file_config_details=json.load(config_file)

def configure():
    load_dotenv()


def read_from_mysql():
  try:
   for i in range(0,len(json_file_config_details["mysql"])):
    try:
     spark = SparkSession.builder.config \
          ("spark.driver.extraClassPath",json_file_config_details["mysql"][i]["spark_driver_path"] ) \
          .master("local").appName("Mysql connectivity").getOrCreate()
     #password = getpass.getpass("Enter the password for MYSQL workbench user "+json_file_config_details["mysql"][i]["user"]+ ": ")

     df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:"+json_file_config_details["mysql"][i]["mysql_host_url"]\
                +":"+json_file_config_details["mysql"][i]["mysql_port"]+"/"+json_file_config_details["mysql"][i]["db_name"]) \
        .option("driver", json_file_config_details["mysql"][i]["driver"]) \
        .option("db_table", json_file_config_details["mysql"][i]["table_name"]) \
        .option("user", json_file_config_details["mysql"][i]["user"]) \
        .option("password", json_file_config_details["mysql"][i]["password"])\
        .option("query",json_file_config_details["mysql"][i]["query"]+json_file_config_details["mysql"][i]["table_name"]).load()
     df.toPandas().to_csv(source_dir+json_file_config_details["mysql"][i]["table_name"]+str(i)+".csv",index=False)
    except:
       print("Please enter correct password for MYSQL workbench user "+json_file_config_details["mysql"][i]["user"])
  except:
      print("No MYSQL instance has been configured")
'''
def read_from_rds():
   try:
    for i in range(0, len(json_file_config_details["rds"])):
     try:
        spark = SparkSession.builder.master('local').appName('RDS connectivity').getOrCreate()
        df = spark.read.format("jdbc").option("url",json_file_config_details["rds"][i]["host"] ) \
         .option("user",json_file_config_details["rds"][i]["user"] )\
         .option("password", json_file_config_details["rds"][i]["password"]) \
         .option("dbtable", json_file_config_details["rds"][i]["table_name"]) \
         .option("driver",json_file_config_details["rds"][i]["driver"]).load()
        df.toPandas().to_csv(source_dir+json_file_config_details["rds"][i]["table_name"]+str(i)+"_rds.csv",index=False)
        return df
     except:
        print("Issue while connecting to RDS instance")
   except:
       print("No RDS instance has been configured")'''

def read_from_rds():
    for i in range(0, len(json_file_config_details["rds"])):
        spark = SparkSession.builder.master('local').appName('RDS connectivity').getOrCreate()
        df = spark.read.format("jdbc").option("url",json_file_config_details["rds"][i]["host"])\
         .option("user",json_file_config_details["rds"][i]["user"])\
         .option("password", json_file_config_details["rds"][i]["password"])\
         .option("dbtable", json_file_config_details["rds"][i]["table_name"])\
         .option("driver",json_file_config_details["rds"][i]["driver"]).load()
        #df.show()
        print(json_file_config_details["rds"][i]["table_name"])
        df.toPandas().to_csv(source_dir+json_file_config_details["rds"][i]["table_name"]+"_rds.csv",index=False)
    return df


def write_to_s3(df):
    spark = SparkSession.builder.master("local").appName("test").getOrCreate()
    s3 = boto3.client("s3")
    df_pandas = df.toPandas() #converting df to pandas
    csv_buf = StringIO()  # keep data in memory
    df_pandas.to_csv(csv_buf, header=True, index=False)
    csv_buf.seek(0)
    print(bucket)
    s3.put_object(Bucket=bucket, Body=csv_buf.getvalue(), Key=key + json_file_config_details["rds"][1]["table_name"] + ".csv")
    print("data uploaded to s3 successfully....")



configure()
#df = read_from_mysql()
#df = read_from_rds()
from common.spark_utility import create_spark_session
spark = create_spark_session()
df = read_from_csv(spark, "../output/course/target_mapped_data_course.csv")
write_to_s3(df)
