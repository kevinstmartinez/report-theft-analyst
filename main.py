from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import boto3
import os
from dotenv import load_dotenv


load_dotenv()


spark = SparkSession.builder\
.master('local')\
.appName('reportehurtopormodalidades')\
.config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.4,org.apache.hadoop:hadoop-common:3.3.3')\
.config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")\
.getOrCreate()
sql_context = SQLContext(spark)



sc = spark.sparkContext

sc._jsc.hadoopConfiguration().set('fs.s3a.access.key', os.getenv('aws_access_key_id'))
sc._jsc.hadoopConfiguration().set('fs.s3a.secret.key', os.getenv('aws_secret_access_key'))
sc._jsc.hadoopConfiguration().set('fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')

spark=SparkSession(sc)


