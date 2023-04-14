from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType, StructField, IntegerType, StringType,FloatType
from pyspark.sql.functions import col, udf
import boto3
import os
import pandas as pd
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


s3_df.printSchema()


def clean_values(value):
  return value.split(' ')[0]


clean_values_udf = udf(lambda z : clean_values(z), StringType())
sql_context.udf.register('clean_values', clean_values_udf)

s3_df = s3_df.select('DEPARTAMENTO', clean_values_udf('MUNICIPIO').alias('MUNICIPIO'), 'CODIGO DANE', 'ARMAS MEDIOS', 'FECHA HECHO', 'GENERO', 'GRUPO ETARIO', 'TIPO DE HURTO', 'CANTIDAD')



s3_df.toPandas().describe().transpose()

# Department with higher theft
#s3_df.groupBy('DEPARTAMENTO').count().sort(col('count').desc()).show()

# Township with higher theft
#s3_df.groupBy('MUNICIPIO').count().orderBy('count', ascending=False).show()
# Weapon more frecuency

# Type theft with more frecuency

# Anything with date