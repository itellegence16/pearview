from pyspark.sql.functions import col, count,monotonically_increasing_id,to_date
from common.read_wrapper import read_from_csv
from common.spark_utility import create_spark_session
from common.write_redshift import write_to_redshift, write_to_mysql

spark = create_spark_session()
df_admission=read_from_csv(spark, "../output/admission/target_mapped_data_admission.csv")
df_admission.show(10)
df_admission_dim = df_admission.select("AdmissionId",to_date( "CourseEnrollmentDate").alias("CourseEnrollmentDate"), to_date("CourseEndDate").alias("CourseEndDate"),"AdmissionFee","CourseId","StudentId")

df1 = df_admission_dim.dropDuplicates(['AdmissionId'])
#print(df1.count())
write_to_mysql(df1,"admission_dim")

