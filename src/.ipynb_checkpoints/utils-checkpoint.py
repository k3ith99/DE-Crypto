from minio import Minio
from pyspark import SparkContext
from pyspark.sql import SparkSession,DataFrame as SparkDataFrame
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
from binance.exceptions import BinanceRequestException, BinanceAPIException
from dateutil.relativedelta import relativedelta
from dateutil.parser import parse
import time
import logging
import typing
load_dotenv(dotenv_path="./main.env", override=True)

#parquet to df 
def get_data(frequency:str,symbol:str,interval:str,start_date:str,end_date:str) -> Optional[list]:
    """Calls binance api for given frequency and stores data in list
    Args:
    frequency: data frequency, options are either daily or monthly
    symbol: currency pair
    interval: binance interval, options are : 1d or 1M at the moment
    start_date:
    end_date:
    """
    #start_date,end_date can be str or int form of utc
    start_date_dt = datetime.strptime(start_date, '%d-%m-%Y')
    start_date_utc = int(start_date_dt.timestamp()* 1000)
    end_date_dt = datetime.strptime(end_date, '%d-%m-%Y')
    end_date_utc = int(end_date_dt.timestamp()* 1000)
    output = []
    try:
        while end_date_utc > start_date_utc:
            data = client_binance.get_historical_klines(
            symbol=symbol, interval=interval, start_str=start_date_utc, end_str=end_date_utc, limit=1000 
            )
            output +=data
            #timeframe = interval_to_milliseconds(interval)
            if not data:
                logger.error(f"No data returned for {symbol} from {start_date} to {end_date}")
                return None
            last_date = data[-1][0]
            #get day,month and year from last date and compare with end_date day,month and year
            last_datetime = datetime.utcfromtimestamp(last_date / 1000)
            #the below is to check last data point
            if last_date >= end_date_utc:
                break
            if frequency == 'monthly':
                start_date_new = last_datetime + relativedelta(months=1)
                start_date_utc = int(start_date_new.timestamp()* 1000)
            elif frequency == 'daily':
                start_date_new = last_datetime + relativedelta(days=1)
                start_date_utc = int(start_date_new.timestamp()* 1000)
            else:
                logger.error(f"Unrecognised data frequency")
            logger.info(client_binance.response.headers['x-mbx-used-weight'])
            #for daily add sleep function
            if client_binance.response.headers['x-mbx-used-weight'] == 5000:
                time.sleep(10)
                logger.info("sleeping due to hitting rate limit")
        return output
    except BinanceAPIException as e:
        logger.error(f"order issue:{e}", stack_info=True, exc_info=True)
        return None
    except BinanceRequestException as e:
        logger.error(f"Network issue: {e}", stack_info=True, exc_info=True)
        return None
    except Exception as e:
        logger.error(e,stack_info=True,exc_info = True)
        return None


def spark_df_daily(output:list,columns:list) -> [SparkDataFrame]:
    """Converts output into spark dataframe
    output: list of data from binance
    columns: keys in binance data that will be columns in our dataframe
    """
    try:
        dataframe = spark.createDataFrame(output, columns)
        df = dataframe.withColumn("Kline open time", to_timestamp(col("Kline open time") / 1000))
        df = df.withColumnRenamed("Kline open time","datetime")
        df = df.select('datetime','Open Price','Close Price','Volume')
        df = df.withColumn("year", year(df["datetime"])) \
           .withColumn("month", month(df["datetime"]))
        logger.info(df.head(5))
        return df
    except Exception as e:
        logger.error(e,stack_info = True, exc_info = True)
        return None

def upload_minio(symbol:str,bucket_name:str,timeframe:str,df:SparkDataFrame):
    """Uploads spark dataframe as parquet files to minio
    Args:
        bucket_name:
        timeframe: daily or monthly at the moment
    """
    try:
        df.write.mode("append") \
        .partitionBy("year", "month") \
        .parquet(f"s3a://{bucket_name}/{symbol}/{timeframe}")
        return f"Successfully {symbol} {timeframe} uploaded to minio"
    except Exception as e:
        logger.error(e,stack_info = True, exc_info = True)