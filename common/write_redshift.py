import json
config_file = open("C:\\Users\\Swati P Kamble\\Desktop\\pearview\\common\\redshit.json")
json_file_config_details=json.load(config_file)
def write_to_redshift(df, table_name):
    df.write.format("jdbc").option("driver", json_file_config_details["driver"]).option("dbtable", table_name).option(
        "url", json_file_config_details["host"]).option("user",json_file_config_details["user"]).option("password", json_file_config_details["password"]).mode("append").save()


def write_to_mysql(df, table_name):
    url = "jdbc:mysql://localhost:3306/pearmart"
    username = "root"
    password = "root123"
    df.write.format("jdbc").option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", table_name).option(
        "url", url).option("user", username).option("password", password).mode("overwrite").save()