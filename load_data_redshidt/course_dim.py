from common.spark_utility import create_spark_session
from common.write_redshift import write_to_redshift
from common.read_from_rds import read_from_rds

spark = create_spark_session()
#df_course = read_from_csv(spark,"../output/course/target_mapped_data_course.csv")
#read data from CDM(rds)
df_course = read_from_rds("course")
df_course.show(10)
df_course_dim = df_course.select("CourseId","CourseName","TotalSeats","AllocatedSeats")
df_course_dim.show()
write_to_redshift(df_course_dim,"course_dim")

