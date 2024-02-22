from pyspark.context import SparkContext
from pyspark.sql import SparkSession
import sys
from pyspark.sql.functions import pandas_udf
import os
from urllib.parse import urlparse
from pyspark.sql.functions import udf
import boto3
from urllib.parse import urlparse, parse_qs,unquote
from pyspark.sql.types import *
import time
import re
from datetime import datetime, timedelta


aws_region = ""
sql_path = ""
create_table = "false"
job_name = "emr-container-sql.py"
if len(sys.argv) == 3:
    aws_region = sys.argv[1]
    sql_path = sys.argv[2]
else:
    print("Job failed. Please provided params aws_region")
    sys.exit(1)

spark = (SparkSession.builder.config("spark.hadoop.hive.metastore.client.factory.class",
                                     "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory").enableHiveSupport().getOrCreate())
sc = spark.sparkContext
log4j = sc._jvm.org.apache.log4j
logger = log4j.LogManager.getLogger(__name__)

logger.info(job_name + " - my_log - {0}".format('starting'))

def read_sql_content_from_s3(s3_path, aws_region):
    o = urlparse(s3_path, allow_fragments=False)
    s3 = boto3.resource('s3', region_name=aws_region)
    # s3 = boto3.client('s3', region_name=aws_region)
    key = o.path.lstrip('/')
    bucket_name = o.netloc
    bucket = s3.Bucket(bucket_name)
    sqls_array = []
    if s3_path.endswith('.sql'):
        obj = s3.Object(bucket_name, key)
        body = obj.get()['Body'].read()
        contents = body.decode('utf-8')
        sqls_array.append(contents)
    else:
        for obj in bucket.objects.filter(Prefix=key):
            if obj.key.endswith('.sql'):
                body = obj.get()['Body'].read()
                contents = body.decode('utf-8')
                sqls_array.append(contents)
    return sqls_array

sqls = read_sql_content_from_s3(sql_path, aws_region)

for idx, sql in enumerate(sqls):
    sql_list = sql.split(";")
    sql_list_trim = [sql.strip() for sql in sql_list if sql.strip() != ""]
    for exec_sql in sql_list_trim:
        msg = f"exec sql: {exec_sql}"
        print("*********** sql := " + msg)
        logger.info(msg)
        start_time = time.time()
        spark.sql(exec_sql).show(20, False)
        end_time = time.time()
        duration = end_time - start_time
        exec_time_msg = f'*********** execution time: {duration} seconds'
        print(exec_time_msg)
        logger.info(exec_time_msg)

spark.stop()
