from pyspark.sql import SparkSession
import sys, os, time, logging
from urllib.parse import urlparse
import boto3
from urllib.parse import urlparse, parse_qs,unquote
from pyspark.sql.types import *
from datetime import datetime, timedelta
import concurrent.futures

class Task:
    def __init__(self, name, sql=None):
        self.name = name
        self.sql = sql
        spark = (SparkSession.builder.config("spark.hadoop.hive.metastore.client.factory.class",
                                             "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory").enableHiveSupport().getOrCreate())
        sc = spark.sparkContext
        log4j = sc._jvm.org.apache.log4j
        logger = log4j.LogManager.getLogger(self.name)
        self.spark = spark
        self.logger = logger
    def _run_sql_with_result(self, sql_str):
        sql_list = sql_str.split(";")
        sql_list_trim = [sql.strip() for sql in sql_list if sql.strip() != ""]
        for exec_sql in sql_list_trim:
            print("************** exec_sql := " + exec_sql)
            self.spark.sql(exec_sql)
    def run(self, sql=None):
        if not sql:
            sql = self.sql
        run_res = {"sql": self.sql, "name": self.name}
        self.logger.info(f'Starting {self.name}')
        start_time = time.time()
        try:
            self._run_sql_with_result(sql)
        except Exception as e:
            self.logger.error(f'{self.name}, execution error {e}')
        finally:
            end_time = time.time()
            duration = end_time - start_time
            run_res["duration"] = duration
            self.logger.info(f'{self.name} ,execution time: {duration} seconds')
            self.spark.stop()
        return run_res

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

def main():
    aws_region = "us-east-1"
    sql_path = "s3://emr-eks-demo1-551831295244/sql/"
    parallel = 2

    if len(sys.argv) == 4:
        aws_region = sys.argv[1]
        sql_path = sys.argv[2]
        parallel = sys.argv[3]
    else:
        print("Job failed. Please provided params aws_region")
        sys.exit(1)

    sqls = read_sql_content_from_s3(sql_path, aws_region)
    tasks = []
    for idx, sql in enumerate(sqls):
        print('****** sql : ' + sql)
        task = Task(f'job: {idx}', sql)
        tasks.append(task)

    with concurrent.futures.ThreadPoolExecutor(max_workers=int(parallel)) as executor:
        futures = []
        task_res_list = []
        for task in tasks:
            future = executor.submit(task.run, None)
            futures.append(future)
        for future in concurrent.futures.as_completed(futures):
            try:
                res = future.result(timeout=500)
                task_res_list.append(res)
            except Exception as exc:
                logging.info(f'generated an exception: {exc}')

if __name__ == '__main__':
    main()


