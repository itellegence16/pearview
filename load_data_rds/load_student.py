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
tbl_name="student"
cursor=conn.cursor()

# drop table with same name
cursor.execute("drop table if exists %s;" % (tbl_name))

#Table Creation
create_table="""
create table student (StudentId int,FirstName varchar(45),LastName varchar(45),DateOfBirth datetime,Gender varchar(45),CurrentAddress varchar(145),PermanentAddress varchar(115),EmailId varchar(45),ContactNumber int,CollegeName varchar(45),EnrollmentDate datetime,College_id int,Course_id int,CreatedDate datetime,UpdateDate datetime,MiddleName varchar(45),AlternateNumber int,EnrollmentNumber int

 )
"""
cursor.execute(create_table)

df_student=pd.read_csv("../output/student/target_mapped_data_student.csv")
df_student=df_student.fillna(0)
df_student.head()

#loop through the data frame and  insert values to table
for i,row in df_student.iterrows():
    #here %S means string values
    sql = "INSERT INTO student VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    print(tuple(row))
    cursor.execute(sql, tuple(row))
    #print("Record inserted")
    # the connection is not auto committed by default, so we must commit to save our changes
    conn.commit()
print("upload sucessfully")
