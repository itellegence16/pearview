
from pyspark.sql.functions import *
from common.read_wrapper import read_from_csv
from common.spark_utility import create_spark_session
from common.write_redshift import write_to_redshift, write_to_mysql

spark = create_spark_session()
df_student = read_from_csv(spark, "../output/student/target_mapped_data_student.csv")
df_student.show(10)
df_admission = read_from_csv(spark, "../output/admission/target_mapped_data_admission.csv")
df_admission1= df_admission.dropDuplicates(['AdmissionId'])
print(df_admission1.count())
#df_admission.show()
df = df_student.join(df_admission1, df_student.StudentId == df_admission1.StudentId, how="inner")
df.show(10)

lst=[]
df_cols=df.columns

for i in df_cols:
    if df_cols.count(i)==2:
        ind=df_cols.index(i)
        lst.append(ind)


lst1=list(set(lst))
for i in lst1:
    df_cols[i]=df_cols[i]+'_0'

admission_factdf=df.toDF(*df_cols)
#admission_factdf.show()
df1=admission_factdf.select("StudentId",col("College_id").alias('CollegeId'),col("Course_id").alias("CourseId"),"AdmissionId").show()
#df2=df1.dropDuplicates(['StudentId'])
print(df1.count())
write_to_mysql(df1,"admission_fact")
#write_to_redshift(df1, "admission_fact")

