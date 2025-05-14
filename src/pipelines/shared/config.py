from minio import Minio
from pyspark import SparkContext
from pyspark.sql import SparkSession
from binance import Client
import os
from dotenv import load_dotenv, dotenv_values
import logging
from utils import get_data, spark_df, upload_minio
import typing

load_dotenv(dotenv_path="./main.env", override=True)

API_KEY = os.getenv("API_KEY")
SECRET_KEY = os.getenv("SECRET_KEY")
MINIO_USER = os.getenv("MINIO_ROOT_USER")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")
client_binance = Client(API_KEY, SECRET_KEY)

def spark_ingest():
    spark = SparkSession.builder.appName("CryptoIngest").getOrCreate()
    return spark

def spark_transform():
    spark = SparkSession.builder \
    .appName("CryptoTransform") \
    .config("spark.jars", "/usr/local/lib/python3.9/site-packages/pyspark/jars/postgresql-42.7.5.jar") \
    .getOrCreate()
    return spark

def configure_s3a(spark:SparkSession):
    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set(
        "fs.s3a.access.key", MINIO_USER
    )  # turn into access key in the future
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", MINIO_PASSWORD)
    sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://minio:9000")
    sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "true")
    sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
    sc._jsc.hadoopConfiguration().set("fs.s3a.attempts.maximum", "1")
    sc._jsc.hadoopConfiguration().set("fs.s3a.connection.establish.timeout", "5000")
    sc._jsc.hadoopConfiguration().set("fs.s3a.connection.timeout", "10000")

def custom_logger():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG) #set to debug to capture all levels
    if logger.hasHandlers():
        logger.handlers.clear()
    logger.propagate = False
    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    