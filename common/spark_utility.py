from pyspark.sql import SparkSession
def create_spark_session():
    spark = SparkSession \
        .builder.config('spark.hadoop.fs.s3a.aws.credentials.provider',
                        'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
        .config('spark.hadoop.fs.s3a.access.key', 'AKIAQSJSMOQUAA63I4MS') \
        .config('spark.hadoop.fs.s3a.secret.key', '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') \
        .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem') \
        .config('spark.jars.packages',
                'org.apache.hadoop:hadoop-aws:3.2.2,''com.amazon.redshift:redshift-jdbc42:2.1.0.9,'
                'software.aws.rds:aws-mysql-jdbc:1.1.1') \
        .master("local").appName("Preview data pipeline") \
        .getOrCreate()

    return spark
