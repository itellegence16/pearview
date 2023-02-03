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
tbl_name="college"
cursor=conn.cursor()

# drop table with same name
cursor.execute("drop table if exists %s;" % (tbl_name))

#Table Creation
create_table="""
create table college (CollegeId int,UniversityId int,CollegeName varchar(45),CollegeAddress varchar(100),City varchar(45),State varchar(45),Country varchar(45),ZipCode int,ContactNumber int ,AlternateNumber int,CreatedDate datetime,UpdateDate datetime
 
 )
"""
cursor.execute(create_table)

df_college=pd.read_csv("../output/college/target_mapped_data_college.csv")
df_college.head(10)

#loop through the data frame and  insert values to table
for i,row in df_college.iterrows():
    #here %S means string values
    sql = "INSERT INTO college VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    cursor.execute(sql, tuple(row))
    #print("Record inserted")
    # the connection is not auto committed by default, so we must commit to save our changes
    conn.commit()
print("upload sucessfully")
