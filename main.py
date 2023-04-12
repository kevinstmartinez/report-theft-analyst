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

s3 = boto3.client(
    's3',
    aws_access_key_id = os.getenv('aws_access_key_id'),
    aws_secret_access_key = os.getenv('aws_secret_access_key'),
    region_name = os.getenv('region')
)

bucket = s3.list_buckets()['Buckets'][0]['Name']
files = s3.list_objects_v2(Bucket = bucket)

Key = files['Contents'][0]['Key']

path = f"s3a://{bucket}/{Key}"

s3_df = spark.read.option('header', True).csv(path)
s3_df.show()
