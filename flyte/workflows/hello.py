from pyspark.sql import SparkSession
from pyspark import SparkConf
import os

os.environ['PYSPARK_SUBMIT_ARGS'] = ' --packages org.apache.hadoop:hadoop-aws:3.2.0,com.amazonaws:aws-java-sdk-bundle:1.11.375,org.apache.hudi:hudi-spark3.1-bundle_2.12:0.11.1,org.apache.spark:spark-avro_2.12:3.1.2 pyspark-shell'

conf = SparkConf()
conf.set('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
conf.set('spark.hadoop.fs.s3a.access.key', 'AKIAZZQUVIBN74RMCUXK')
conf.set('spark.hadoop.fs.s3a.secret.key', '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')


spark = SparkSession.builder.config(conf=conf).getOrCreate()
tripsSnapshotDF = spark.read.format('hudi').load('s3a://hudi-table/logs_mor/*')
tripsSnapshotDF.show()
