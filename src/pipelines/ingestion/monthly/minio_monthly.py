from minio import Minio
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, month, year
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, LongType
import requests
import json
import pandas as pd
from binance import Client
from datetime import datetime, date
import os
from dotenv import load_dotenv, dotenv_values
from binance.helpers import date_to_milliseconds, interval_to_milliseconds
from dateutil.relativedelta import relativedelta
from dateutil.parser import parse
import time
load_dotenv(dotenv_path="./main.env", override=True)

#load_dotenv()
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
sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://minio:9000")
sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "true")
sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
sc._jsc.hadoopConfiguration().set("fs.s3a.attempts.maximum", "1")
sc._jsc.hadoopConfiguration().set("fs.s3a.connection.establish.timeout", "5000")
sc._jsc.hadoopConfiguration().set("fs.s3a.connection.timeout", "10000")

# tickers = client.get_all_tickers()
#client_binance.get_exchange_info() #has rate limit info
#client_binance.response.headers #has info on current usage
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


cryptos = ['BTCUSDT','ETHUSDT','LTCUSDT','BNBUSDT','DOGEUSDT']

def get_data_monthly(symbol,interval,start_date,end_date):
    output = []
    try:
        while end_date > start_date:
            data = client_binance.get_historical_klines(
            symbol=symbol, interval=interval, start_str=start_date, end_str=end_date, limit=1000 
            )
            output +=data
            #timeframe = interval_to_milliseconds(interval)
            last_date = data[-1][0]
            #get month and year from last date and compare with end_date month and year
            last_datetime = datetime.utcfromtimestamp(last_date / 1000)
            end_datetime = datetime.strptime(end_date, '%d-%m-%Y')
            #the below is to check last data point
            if last_datetime.year == end_datetime.year and last_datetime.month == end_datetime.month:
                break
            start_datetime =  last_datetime + relativedelta(months=1)
            start_date = str(start_datetime)
            print(client_binance.response.headers['x-mbx-used-weight'])
            #for daily add sleep function
            if client_binance.response.headers['x-mbx-used-weight'] == 5000:
                time.sleep(10)
        return output
    except Exception as e:
        print("An exception occurred:", type(e).__name__, "–", e)
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

def spark_df_monthly(output,columns):
    #implement schema check
    dataframe = spark.createDataFrame(output, columns)
    df = dataframe.withColumn("Kline open time", to_timestamp(col("Kline open time") / 1000))
    df = df.withColumnRenamed("Kline open time","datetime")
    df = df.select('datetime','Open Price','Close Price','Volume')
    df = df.withColumn("year", year(df["datetime"])) \
       .withColumn("month", month(df["datetime"]))
    return df

def upload_minio(minio_url,minio_user,minio_password,symbol,bucket_name,timeframe,df):
    pass
    client_minio = Minio(
    #"localhost:9000",  # Make sure you're using port 9000 for the S3 API
    minio_url,
    access_key = minio_user,
    secret_key = minio_password,
    secure=False  # Disable SSL if you're not using SSL certificates
    )
    df.write.mode("append") \
    .partitionBy("year", "month") \
    .parquet(f"s3a://{bucket_name}/{symbol}/{timeframe}")
    return f"Successfully {symbol} {timeframe} uploaded to minio"

def minio_pipeline_monthly(symbol):
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
    data = get_data_monthly(symbol = symbol,interval = '1M',start_date = "01-01-2019",end_date = "31-12-2024")
    df_monthly = spark_df_monthly(data,columns)
    upload_minio(minio_url = 'localhost:9000',minio_user = MINIO_USER ,minio_password = MINIO_PASSWORD ,symbol = symbol ,bucket_name = 'binancedata',timeframe='Monthly',df = df_monthly)
    return "Successfully ran monthly pipeline to minio"
    #implement try and except, api codes from minio and binance
    #implement schema check
def main():
    cryptos = ['BTCUSDT','ETHUSDT','LTCUSDT','BNBUSDT','DOGEUSDT']
    for symbol in cryptos:
        try:
            minio_pipeline_monthly(symbol)
        except Exception as e:
            print(f"Error processing {symbol}: {e}")
if __name__ == "__main__":
    main()