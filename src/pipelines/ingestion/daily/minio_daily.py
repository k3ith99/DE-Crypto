from minio import Minio
from pyspark import SparkContext
from pyspark.sql import SparkSession
from binance import Client
import os
from dotenv import load_dotenv, dotenv_values
import logging
from utils import get_data, spark_df, upload_minio

load_dotenv(dotenv_path="./main.env", override=True)

# load_dotenv()
API_KEY = os.getenv("API_KEY")
SECRET_KEY = os.getenv("SECRET_KEY")
MINIO_USER = os.getenv("MINIO_ROOT_USER")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")
client_binance = Client(API_KEY, SECRET_KEY)

"""client_minio = Minio(
        #"localhost:9000",  # Make sure you're using port 9000 for the S3 API
        minio_url, #change this
        access_key = MINIO_USER,
        secret_key = MINIO_PASSWORD,
        secure=False  # Disable SSL if you're not using SSL certificates
        )
"""
spark = SparkSession.builder.appName("CryptoETL").getOrCreate()


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
# tickers = client.get_all_tickers()
# client_binance.get_exchange_info() #has rate limit info
# client_binance.response.headers #has info on current usage
"""[
  [
    1499040000000,      // Kline open time
    "0.01634790",       // Open price
    "0.80000000",       // High price
    "0.01575800",       // Low price
    "0.01577100",       // Close price
    "148976.11427815",  // Volume
    1499644799999,      // Kline Close time
    "2434.19055334",    // Quote asset volume
    308,                // Number of trades
    "1756.87402397",    // Taker buy base asset volume
    "28.46694368",      // Taker buy quote asset volume
    "0"                 // Unused field, ignore.
  ]
]"""


def minio_pipeline_daily(symbol):
    columns = [
        "Kline open time",
        "Open Price",
        "High price",
        "Low price",
        "Close Price",
        "Volume",
        "Kline Close time",
        "Quote asset volume",
        "Number of trades",
        "Taker buy base asset volume",
        "Taker buy quote asset volume",
        "Ignore",
    ]
    data = get_data(
        client_binance=client_binance,
        symbol=symbol,
        frequency="daily",
        start_date="01-01-2019",
        end_date="31-12-2024",
    )
    df_daily = spark_df(data, columns)
    upload_minio(
        symbol=symbol, bucket_name="binancedata", timeframe="daily", df=df_daily
    )
    return "Successfully ran daily pipeline to minio"


def main():
    cryptos = ["BTCUSDT", "ETHUSDT", "LTCUSDT", "BNBUSDT", "XRPUSDT"]
    for symbol in cryptos:
        try:
            minio_pipeline_daily(symbol)
        except Exception as e:
            logger.error(
                f"Error processing minio daily pipeline: {e}",
                stack_info=True,
                exc_info=True,
            )


if __name__ == "__main__":
    main()
