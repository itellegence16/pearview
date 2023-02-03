from common.spark_utility import create_spark_session
from common.write_redshift import write_to_redshift
from common.read_from_rds import read_from_rds

spark = create_spark_session()
#print(spark)

#read data from CDM(rds)
df_college = read_from_rds("college")
df_college_dim = df_college.select("CollegeId", "CollegeName", "UniversityId", "CollegeAddress", "City", "ZipCode")
df_college_dim.show()

#write_to_mysql(df_college_dim,"college_dim")

#write to Fact and dimension table
write_to_redshift(df_college_dim,"college_dim1")