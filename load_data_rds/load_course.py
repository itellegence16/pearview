import pandas as pd
import pymysql
import config.settings as rds

conn = pymysql.connect(
    host=rds.host,
    port=rds.port,
    user=rds.user,
    password=rds.password,
    db=rds.db,

)
print('opened database successfully')
tbl_name="course"
cursor=conn.cursor()

# drop table with same name
cursor.execute("drop table if exists %s;" % (tbl_name))

#Table Creation
create_table="""
create table course (CourseId int not null,CourseName varchar(45),CourseDuration int ,NoOfSemesters int,TotalSeats int,AllocatedSeats int,CreatedDate datetime,UpdateDate datetime

 )
"""
cursor.execute(create_table)

df_course=pd.read_csv("../output/course/target_mapped_data_course.csv")
df_course.head(10)

#loop through the data frame and  insert values to table
for i,row in df_course.iterrows():
    #here %S means string values
    sql = "INSERT INTO course VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
    print(tuple(row))
    cursor.execute(sql, tuple(row))
    #print("Record inserted")
    # the connection is not auto committed by default, so we must commit to save our changes
    conn.commit()
print("upload sucessfully")
