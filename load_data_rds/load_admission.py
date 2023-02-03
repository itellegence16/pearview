import pandas as pd
import psycopg2
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
tbl_name="admission"
cursor=conn.cursor()

# drop table with same name
cursor.execute("drop table if exists %s;" % (tbl_name))

#Table Creation
create_table="""
create table admission (AdmissionID int not null,CourseAppliedDate datetime ,CourseEnrollmentDate datetime,CourseEndDate datetime,AdmissionFee int,CourseId int,StudentId int,CreatedDate datetime,UpdateDate datetime

 )
"""
cursor.execute(create_table)

df_admission=pd.read_csv("../output/admission/target_mapped_data_admission.csv")
df_admission.head(10)

#loop through the data frame and  insert values to table
for i,row in df_admission.iterrows():
    #here %S means string values
    sql = "INSERT INTO admission VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    cursor.execute(sql, tuple(row))
    #print("Record inserted")
    # the connection is not auto committed by default, so we must commit to save our changes
    conn.commit()
print("upload sucessfully")
