from pyspark.sql.functions import col, count,monotonically_increasing_id
from common.read_wrapper import read_from_csv
from common.spark_utility import create_spark_session
from common.write_redshift import write_to_redshift, write_to_mysql

spark = create_spark_session()
df_admission=read_from_csv(spark, "../output/admission/target_mapped_data_admission.csv")
df_admission.show(10)
df_admission_dim = df_admission.select("AdmissionId", "CourseEnrollmentDate","CourseEndDate","AdmissionFee","CourseId","StudentId")
df_admission_dim.show(10)
write_to_mysql(df_admission_dim,"admission_dim")
#write_to_redshift(df_college, "admission_dim")
