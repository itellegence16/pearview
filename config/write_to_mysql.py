from pyspark.sql import SparkSession
from mysql.connector import Error


def write_to_mysql(df, table_name):
    url = "jdbc:mysql://localhost:3306/pearview"
    username = "root"
    password = "admin"
    df.write.format("jdbc").option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", table_name).option(
        "url", url).option("user", username).option("password", password).mode("overwrite").save()


def read_from_csv(spark, path):
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
    return df


def create_spark_session():
    spark = SparkSession.builder.appName('write data into d-mart').getOrCreate()
    return spark


spark = create_spark_session()
df_college = read_from_csv(spark,"output/college/target_mapped_data_college.csv")
df_college.show(10)
df_college_dim = df_college.select("CollegeId", "CollegeName", "UniversityId", "CollegeAddress", "City", "ZipCode")
df_college_dim.show(10)
write_to_mysql(df_college_dim,"college_dim1")

if __name__ == '__main__':
    spark = create_spark_session()