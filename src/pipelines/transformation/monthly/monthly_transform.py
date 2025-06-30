from minio import Minio
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, TimestampNTZType, DecimalType
from utils import (
    parquet_to_df,
    data_cleaning,
    add_crypto_id,
    add_time_id,
    upload_time,
    upload_price,
    calculations,
    upload_calculations
)
from binance import Client
import os
from dotenv import load_dotenv, dotenv_values
import logging
import pyspark.pandas as ps

load_dotenv(dotenv_path="./main.env", override=True)

API_KEY = os.getenv("API_KEY")
SECRET_KEY = os.getenv("SECRET_KEY")
MINIO_USER = os.getenv("MINIO_ROOT_USER")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")
client_binance = Client(API_KEY, SECRET_KEY)

spark = (
    SparkSession.builder.appName("CryptoETL")
    .config(
        "spark.jars",
        "/usr/local/lib/python3.9/site-packages/pyspark/jars/postgresql-42.7.5.jar",
    )
    .getOrCreate()
)
# Get the SparkContext from the SparkSession
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

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # set to debug to capture all levels
if logger.hasHandlers():
    logger.handlers.clear()
logger.propagate = False
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
logger.addHandler(handler)

client_minio = Minio(
    "minio:9000",  # Make sure you're using port 9000 for the S3 API
    # minio_url,
    access_key=MINIO_USER,
    secret_key=MINIO_PASSWORD,
    secure=False,  # Disable SSL if you're not using SSL certificates
)

schema = StructType(
    [
        StructField(name="datetime", dataType=TimestampNTZType(), nullable=False),
        StructField(name="Open Price", dataType=DecimalType(), nullable=False),
        StructField(name="Close Price", dataType=DecimalType(), nullable=False),
        StructField(name="Volume", dataType=DecimalType(), nullable=False),
    ]
)
# doesnt work for parquet files, schema inferred from that instead


read_sql = "SELECT * FROM crypto"
df_crypto = (
    spark.read.format("jdbc")
    .option("url", "jdbc:postgresql://postgres1:5432/crypto")
    .option("user", "postgres")
    .option("password", "postgres")
    .option("query", read_sql)
    .option("driver", "org.postgresql.Driver")
    .load()
)


def monthly_transform(symbol, currency):
    df = parquet_to_df(
        client_minio=client_minio, timeframe="monthly", crypto=symbol, schema=schema
    )
    df_cleaned = data_cleaning(df)
    df_calculations = calculations("monthly",df_cleaned)
    df_id = add_crypto_id(df_calculations, df_crypto, symbol, currency)
    logger.info(df_id.show())
    df_time_id = add_time_id(df_id, "monthly")
    logger.info(df_time_id.show())
    # logger.info(df_time_id.show())
    upload_time(df_time_id, "monthly")
    upload_price(df_time_id)
    upload_calculations(df_time_id)


def main():
    cryptos = ["BTCUSDT", "ETHUSDT", "LTCUSDT", "BNBUSDT", "XRPUSDT"]

    for symbol in cryptos:
        try:
            monthly_transform(symbol=symbol, currency="USDT")
            logger.info(f"Data successfully transformed and loaded for {symbol}")
        except Exception as e:
            logger.error(
                f"Error transforming {symbol}:{e}", stack_info=True, exc_info=True
            )
            break


if __name__ == "__main__":
    main()
