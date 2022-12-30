import getpass_asterisk.getpass_asterisk
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import when,split
import os,json,getpass
from pyspark.sql import functions as F
table_details=str(input("Enter the table name: e.g. student, course, college: ")).lower()
source_dir=r"C:\Users\Sana Mahajan\Documents\git_practice\pearview\\source_input\\"+table_details+"\\"
config_dir=r"C:\Users\Sana Mahajan\Documents\git_practice\pearview\config\\"+table_details+"\\"
output_dir=r"C:\Users\Sana Mahajan\Documents\git_practice\pearview\output\\"
source_dir_length=len(os.listdir(source_dir))
target_file=output_dir+"target_mapped_data_"+table_details+".csv"
col_csv=output_dir+"col.csv"
pd.set_option('display.max_columns', None)
config_file=open(config_dir+table_details+"_config.json")
column_mapping = json.load(config_file)
config_file = open(config_dir+"write_source_"+table_details+"_config.json")
json_file_config_details=json.load(config_file)
i=0
#password=getpass.getpass("Enter the password for MYSQL workbench user "+json_file_mysql_table["config"][i]["user"]+ ": ")
def create_spark_session():
    spark=SparkSession.builder.config\
    ("spark.driver.extraClassPath",json_file_config_details["mysql"][i]["spark_driver_path"])\
            .appName('Read source data into dataframe').getOrCreate()
    return spark

def consolidated_data_to_sql(df_target):
        df_target.select(df_target.columns).write.format("jdbc").option("url", "jdbc:"+json_file_config_details["mysql"][i]["mysql_host_url"]\
                +":"+json_file_config_details["mysql"][i]["mysql_port"]+"/"+json_file_config_details["mysql"][i]["db_name"]) \
            .option("driver", json_file_config_details["mysql"][i]["driver"]).option("dbtable", json_file_config_details["mysql"][i]["table_name"]) \
            .option("user", json_file_config_details["mysql"][i]["user"]).option("password", json_file_config_details["mysql"][i]["password"]).mode('overwrite').save()
def read_source_data():
    df_col= pd.DataFrame(columns=list(column_mapping.keys()))
    csv_col=df_col.to_csv(col_csv,index=False)
    df_target = spark.read.csv(col_csv,header=True)
    ###To read csv files generated from mysql tables as well and normal csv's as well###
    for file in os.listdir(source_dir):
        df_source = spark.read.csv(source_dir+file,header=True)
        print("Source: "+file)
        df_target=transform_source_data(df_source,df_target)
    ### This is to store output data in csv for future use##
    df_target.toPandas().to_csv(target_file,index=False)
    print(df_target.columns)
    print("Target table: ")
    ####Load consolidated data into MySql Table###
    consolidated_data_to_sql(df_target)


def transform_source_data(df_source,df_target):
    source_column_names = list(df_source.columns)
    source_column_names=list(map(str.lower, source_column_names))
    for key, value in column_mapping.items():
        #### To check if any source column value is mapped to custom defined columns ########
        if (set(source_column_names) & set(value)):
            common = list(set(source_column_names) & set(value))
            if (len(common) == 1):
                common_column=""
                ###Joining and converting common column list to a string######
                common_column = common_column.join(common)
            ####Renaming the column names from source target to common target df#######
            df_source=df_source.withColumnRenamed(common_column,key)
            #print(df_source.columns)
            if(key=="Gender"):
                df_source=df_source.withColumn("Gender", when((df_source.Gender == "M") | (df_source.Gender == "Male") | (df_source.Gender == "Men"), "Male")\
                                               .when((df_source.Gender == "F") | (df_source.Gender == "Female") | (df_source.Gender == "Women"), "Female")\
                        .otherwise(df_source.Gender))
            if (key == "EnrollmentDate"):
                df_source = convert_dates(df_source)
    #### To input source column data into target columns####
    result=df_source.unionByName(df_target,allowMissingColumns=True)
    ##
    # if "first_name" in result.columns:
    #     result = result.withColumn("first_name", split(result["first_name"], " ").getField(0))\
    #         .withColumn("last_name", split(result["first_name"], " ").getField(1)).drop(result["first_name"])
    # print(result.show())
    ##
    return result

def convert_dates(ds_with_dates):
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
    sdf = ds_with_dates.withColumn("yyyy/MM/dd", F.to_date(F.unix_timestamp(ds_with_dates.EnrollmentDate, 'yyyy/MM/dd').cast('timestamp'))) \
        .withColumn("yyyy-MM-dd", F.to_date(F.unix_timestamp(ds_with_dates.EnrollmentDate, 'yyyy-MM-dd').cast('timestamp'))) \
        .withColumn("MM/dd/yyyy", F.to_date(F.unix_timestamp(ds_with_dates.EnrollmentDate, 'MM/dd/yyyy').cast('timestamp'))) \
        .withColumn("MM-dd-yyyy", F.to_date(F.unix_timestamp(ds_with_dates.EnrollmentDate, 'MM-dd-yyyy').cast('timestamp'))) \
        .withColumn("dd/MM/yy", F.to_date(F.unix_timestamp(ds_with_dates.EnrollmentDate, 'dd/MM/yy').cast('timestamp'))) \
        .withColumn("dd-MM-yy", F.to_date(F.unix_timestamp(ds_with_dates.EnrollmentDate, 'dd-MM-yy').cast('timestamp'))) \
        .withColumn("STANDARD_DATE",
                    F.coalesce("yyyy/MM/dd", "yyyy-MM-dd", "MM/dd/yyyy", "MM-dd-yyyy", 'dd/MM/yy', 'dd-MM-yy'))
    sdf1 = sdf.drop('yyyy/MM/dd',"yyyy-MM-dd", "MM/dd/yyyy", "MM-dd-yyyy", 'dd/MM/yy', 'dd-MM-yy')
    return sdf1


if __name__ == '__main__':
    spark = create_spark_session()
    if(source_dir_length>=1):
      read_source_data()
    else:
      exit()
