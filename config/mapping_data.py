import pandas as pd
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import when,split
import os,json
#table_details=str(input("Enter the table name: e.g. student, course, fee: ")).lower()
table_details="student"
source_dir=r"C:\Users\Sana Mahajan\Documents\git_practice\pearview\\"+table_details+"\\"
config_dir=r"C:\Users\Sana Mahajan\Documents\git_practice\pearview\config\\"
source_dir_length=len(os.listdir(   source_dir))
target_file=config_dir+"target_mapped_data.csv"
col_csv=config_dir+"col.csv"
pd.set_option('display.max_columns', None)
config_file=open(config_dir+table_details+"_config.json")
column_mapping = json.load(config_file)
def create_spark_session():
    spark=SparkSession.builder.appName('Read source data into dataframe').getOrCreate()
    return spark
def read_source_data():
    df_col= pd.DataFrame(columns=list(column_mapping.keys()))
    csv_col=df_col.to_csv(col_csv,index=False)
    df_target = spark.read.csv(col_csv,header=True)
    for file in os.listdir(source_dir):
        df_source = spark.read.csv(source_dir+file,header=True)
        print("Source: "+file)
        print(df_source.show(50))
        df_target=transform_source_data(df_source,df_target)
    ### This is to store output data in csv for future use##
    df_target.toPandas().to_csv(target_file,index=False)
    print(df_target.columns)
    print("Target table: ")
    print(df_target.show(50))


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
            if(key=="gender"):
                df_source=df_source.withColumn("gender", when((df_source.gender == "M") | (df_source.gender == "Male") | (df_source.gender == "Men"), "Male")\
                                               .when((df_source.gender == "F") | (df_source.gender == "Female") | (df_source.gender == "Women"), "Female")\
                        .otherwise(df_source.gender))
    #### To input source column data into target columns####
    result=df_source.unionByName(df_target,allowMissingColumns=True)
    ##
    # if "first_name" in result.columns:
    #     result = result.withColumn("first_name", split(result["first_name"], " ").getField(0))\
    #         .withColumn("last_name", split(result["first_name"], " ").getField(1)).drop(result["first_name"])
    # print(result.show())
    ##
    return result

if __name__ == '__main__':
    spark = create_spark_session()
    if(source_dir_length>=1):
      read_source_data()
    else:
      exit()
