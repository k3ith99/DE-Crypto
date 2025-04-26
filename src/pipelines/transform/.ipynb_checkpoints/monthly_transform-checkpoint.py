from minio import Minio
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, month, year,monotonically_increasing_id,row_number,concat, udf,lit
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, LongType,TimestampNTZType,StringType,DecimalType
from pyspark.sql.window import Window
from pyspark.conf import SparkConf
import requests
import json
import pandas as pd
from binance import Client
from datetime import datetime, date
import os
from dotenv import load_dotenv, dotenv_values
from binance.helpers import date_to_milliseconds, interval_to_milliseconds
from binance.exceptions import BinanceRequestException, BinanceAPIException
from dateutil.relativedelta import relativedelta
from dateutil.parser import parse
import time
import logging
import psycopg2
load_dotenv()

API_KEY = os.getenv("API_KEY")
SECRET_KEY = os.getenv("SECRET_KEY")
MINIO_USER = os.getenv("MINIO_ROOT_USER")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")
client_binance = Client(API_KEY, SECRET_KEY)

spark = SparkSession.builder \
    .appName("CryptoETL") \
    .getOrCreate()
# Get the SparkContext from the SparkSession
sc = spark.sparkContext
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", MINIO_USER)#turn into access key in the future 
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", MINIO_PASSWORD)
sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://localhost:9000")
sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "true")
sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
sc._jsc.hadoopConfiguration().set("fs.s3a.attempts.maximum", "1")
sc._jsc.hadoopConfiguration().set("fs.s3a.connection.establish.timeout", "5000")
sc._jsc.hadoopConfiguration().set("fs.s3a.connection.timeout", "10000")

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG) #set to debug to capture all levels
if logger.hasHandlers():
    logger.handlers.clear()
logger.propagate = False
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
logger.addHandler(handler)

client_minio = Minio(
        "localhost:9000",  # Make sure you're using port 9000 for the S3 API
        #minio_url,
        access_key = MINIO_USER,
        secret_key = MINIO_PASSWORD,
        secure=False  # Disable SSL if you're not using SSL certificates
        )

schema = StructType([\
                    StructField(name = 'datetime',dataType = TimestampNTZType(),nullable = False), \
                    StructField(name= 'Open Price',dataType = DecimalType(),nullable =False), \
                    StructField(name= 'Close Price',dataType = DecimalType(),nullable =False), \
                    StructField(name= 'Volume',dataType = DecimalType(),nullable = False)\
                                ])

def parquet_to_df(client,crypto,schema):
    #read from parquet from minio and combines into dataframe
    try:
        objects = client.list_objects("binancedata", prefix=crypto, recursive=True)
        filenames = [obj.object_name for obj in objects]
        filenames = [f for f in filenames if "_SUCCESS" not in f]
        df = spark.createDataFrame(data = [],schema = schema)
        for file in filenames:
            df_parquet = spark.read.parquet(f"s3a://binancedata/{file}")
            df = df.union(df_parquet)
        return df
    except Exception as e:
        logger.error(f"error reading parquet from minio: {e}", stack_info=True, exc_info=True)

def data_cleaning(df):
    try:
        df_null = df.na.drop(how = 'any',subset = ['datetime'])
        df_renamed = df_null.withColumnsRenamed({'Open Price':'open',
                                'Close Price':'close',
                                'Volume':'volume'})
        df_duplicated = df_renamed.dropDuplicates()
        for column in ["open", "close", "volume"]:
            df_duplicated = df_duplicated.withColumn(column, col(column).cast(IntegerType()))
        return df_duplicated
    except Exception as e:
        logger.error(f"Error cleaning data:{e}",stack_info=True,exc_info=True)
        
read_sql = "SELECT * FROM crypto"
df_crypto = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/crypto") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .option("query", read_sql)\
    .option("driver", "org.postgresql.Driver")\
    .load()

def add_crypto_id(df,df_crypto,crypto,currency):
    try:
        df_crypto = df_crypto.withColumn("trading pair", concat(df_crypto.ticker, lit(currency)))
        #obtain the crypto from concatenating currency and ticker
        df_crypto = df_crypto.filter(col("trading pair") == crypto)
        crypto_id = df_crypto.collect()[0]['id']
        df_id = df.withColumn("crypto_id",lit(crypto_id))
        return df_id
    except Exception as e:
        logger.error(f"Error adding crypto id:{e}",stack_info=True,exc_info=True)
    return df_id

def generate_time_id(dt_value):
    #hard coded
    ts = dt_value.strftime("%Y%m")
    return int(ts)

def add_time_id(generate_time_id,df):
    try:
        dt_udf = udf(generate_time_id,IntegerType())
        df_with_udf = df.withColumn("time_id", dt_udf(df["datetime"]))
        #df_time = df_time.withColumnRenamed('time_id','id')
        return df_with_udf
    except Exception as e:
        logger.error(f"Error adding time id:{e}",stack_info=True,exc_info=True)

def upload_time(df):
    #need to futureproof
    df_year = df.withColumn("year", year(df["datetime"]))
    df_month = df_year.withColumn("month", month(df_year["datetime"]))
    df_time_filtered = df_month.select(['time_id','datetime','year','month'])
    df_time = df_time_filtered.withColumnRenamed('time_id','id')
    try:
        df_time.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/crypto") \
        .option("dbtable", "time") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append")\
        .save()
        logger.info("Successfully uploaded to time table")
    except Exception as e:
        logger.error(f"Error uploading to time table:{e}",stack_info=True,exc_info=True)

def upload_price(df):
    df_filtered = df.select(['crypto_id','time_id','open','close','volume'])
    try:
        df_filtered.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/crypto") \
        .option("dbtable", "price") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append")\
        .save()
        logger.info("Successfully uploaded to price table")
    except Exception as e:
        logger.error(f"Error uploading to price table:{e}",stack_info=True,exc_info=True)
        
def monthly_transform(symbol,currency):
    df = parquet_to_df(client = client_minio,crypto = symbol,schema = schema)
    df_cleaned = data_cleaning(df)
    df_id = add_crypto_id(df_cleaned,df_crypto,symbol,currency)
    df_time_id =add_time_id(generate_time_id,df_id)
    upload_time(df_time_id)
    upload_price(df_time_id)
    logger.info(f"Data successfully transformed and loaded for {symbol}")
def main():
    cryptos = ['BTCUSDT','ETHUSDT','LTCUSDT','BNBUSDT','DOGEUSDT']
    for symbol in cryptos:
        try:
            monthly_transform(symbol = symbol,currency = "USDT")
        except Exception as e:
            logger.error(f"Error transforming {symbol}:{e}",stack_info=True,exc_info=True)
            break
if __name__ == "__main__":
    main()