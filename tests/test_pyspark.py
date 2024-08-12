import boto3
import os
import pytest
import signal
import subprocess

from moto import mock_aws
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql import Row

@pytest.fixture(autouse=True, scope="module")
def handle_server():
    print(" Set Up Moto Server")
    process = subprocess.Popen("moto_server",
                               stdout=subprocess.PIPE,
                               shell=True,
                               preexec_fn=os.setsid)

    yield
    print("Tear Down Moto Server")
    os.killpg(os.getpgid(process.pid), signal.SIGTERM)


@mock_aws
def test_clean_user(handle_server):

    os.environ["SPARK_LOCAL_IP"] = "localhost"
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--packages "org.apache.hadoop:hadoop-aws:3.3.4" pyspark-shell'
    )
    s3_connection = boto3.resource('s3',
                                   region_name='us-east-1',
                                   endpoint_url="http://127.0.0.1:5000")
    s3_connection.create_bucket(Bucket='twitter-user')

    spark = SparkSession.builder.config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem").getOrCreate()

    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set(
        "fs.s3a.endpoint",
        f"http://127.0.0.1:5000"
    )
    hadoop_conf.set(
        "fs.s3a.access.key",
        ""
    )
    hadoop_conf.set(
        "fs.s3a.secret.key",
        ""
    )

    data = [Row(name="Alice", age=30), Row(name="Bob", age=25), Row(name="Cathy", age=27)]
    df = spark.createDataFrame(data)
    df.write.csv("s3a://twitter-user/test_user_data.csv")
    df.write.parquet("s3://twitter-user/test_user_data.parquet")