from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, monotonically_increasing_id
from common.read_wrapper import read_from_csv
from common.spark_utility import create_spark_session
from common.write_redshift import write_to_redshift, write_to_mysql

spark = create_spark_session()
df=read_from_csv(spark,"../output/student/target_mapped_data_student.csv")
df1 = df.select("StudentId", "FirstName", "LastName", "Gender", "PermanentAddress",col("College_id").alias("CollegeId"))
#df1.show()
print(df1.count())
write_to_mysql(df1,"student_dim")


