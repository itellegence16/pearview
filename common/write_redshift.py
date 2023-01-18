def write_to_redshift(df, table_name):
    url = "jdbc:redshift://redshift-cluster-1.cag974yfyey1.us-east-1.redshift.amazonaws.com:5439/finaldb"
    username = "awsuser"
    password = "Vidya#123"
    df.write.format("jdbc").option("driver", "com.amazon.redshift.jdbc42.Driver").option("dbtable", table_name).option(
        "url", url).option("user", username).option("password", password).mode("overwrite").save()


def write_to_mysql(df, table_name):
    url = "jdbc:mysql://localhost:3306/pearmart"
    username = "root"
    password = "root123"
    df.write.format("jdbc").option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", table_name).option(
        "url", url).option("user", username).option("password", password).mode("overwrite").save()
