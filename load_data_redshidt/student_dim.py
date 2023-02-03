from pyspark.sql.functions import col
from common.spark_utility import create_spark_session
from common.write_redshift import write_to_redshift, write_to_mysql
from common.read_from_rds import read_from_rds
spark = create_spark_session()

#read data from CDM(rds)
df_student = read_from_rds("student")

#df=read_from_csv(spark,"../output/student/target_mapped_data_student.csv")
df1 = df_student.select("StudentId", "FirstName", "LastName", "Gender", "PermanentAddress",col("College_id").alias("CollegeId"))
#df1.show()
print(df1.count())

#write to redshift
write_to_redshift(df1,"student_dim")


