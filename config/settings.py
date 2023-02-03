import os
dir_name = os.getcwd()
parent_dir = os.path.abspath(os.path.join(dir_name, os.pardir))
output_dir = os.path.join(parent_dir, "output")
source_dir = os.path.join(parent_dir, "source_input")
port=3306
db="pearview"
user="admin"
password="Admin123"
host="common-datamart.c3tne9nwnjmy.ap-northeast-1.rds.amazonaws.com"
