from pyspark.sql.functions import col, count,monotonically_increasing_id
from common.read_wrapper import read_from_csv
from common.spark_utility import create_spark_session
from common.write_redshift import write_to_redshift, write_to_mysql

spark = create_spark_session()
df_course = read_from_csv(spark,"../output/course/target_mapped_data_course.csv")
df_course.show(10)
df_course_dim = df_course.select("CourseId","CourseName","TotalSeats","AllocatedSeats")
df_course_dim.show()
write_to_mysql(df_course_dim,"course_dim")
#write_to_redshift(df_course_dim, "course_dim")
