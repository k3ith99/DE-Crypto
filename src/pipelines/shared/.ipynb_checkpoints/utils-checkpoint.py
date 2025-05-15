from minio import Minio
from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
from pyspark.sql.functions import col, to_timestamp, month, year
from pyspark.sql.types import (
    IntegerType,
    FloatType,
    LongType,
    StructType,
    StructField,
    DecimalType,
    TimestampNTZType,
)
from pyspark.sql.functions import (
    col,
    to_timestamp,
    month,
    year,
    day,
    monotonically_increasing_id,
    row_number,
    concat,
    udf,
    lit,
)
from binance import Client
from datetime import datetime, date
from binance.helpers import date_to_milliseconds, interval_to_milliseconds
from binance.exceptions import BinanceRequestException, BinanceAPIException
from dateutil.relativedelta import relativedelta
from dateutil.parser import parse
import time
from datetime import datetime, date
import logging
from typing import Optional

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # set to debug to capture all levels
if logger.hasHandlers():
    logger.handlers.clear()
logger.propagate = False
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
logger.addHandler(handler)

spark = SparkSession.builder.appName("MyApp").getOrCreate()


def get_data(
    client_binance: Client,
    frequency: str,
    symbol: str,
    start_date: str,
    end_date: str,
) -> Optional[list]:
    """Calls binance api for given frequency and stores data in list
    Args:
    frequency: data frequency, options are either daily or monthly
    symbol: currency pair
    interval: binance interval, options are : 1d or 1M at the moment
    start_date:
    end_date:
    client: binance client function
    """
    # start_date,end_date can be str or int form of utc
    start_date_dt = datetime.strptime(start_date, "%d-%m-%Y")
    start_date_utc = int(start_date_dt.timestamp() * 1000)
    end_date_dt = datetime.strptime(end_date, "%d-%m-%Y")
    end_date_utc = int(end_date_dt.timestamp() * 1000)
    output = []
    try:
        while end_date_utc > start_date_utc:
            if frequency == "monthly":
                data = client_binance.get_historical_klines(
                    symbol=symbol,
                    interval="1M",
                    start_str=start_date_utc,
                    end_str=end_date_utc,
                    limit=1000,
                )
            elif frequency == "daily":
                data = client_binance.get_historical_klines(
                    symbol=symbol,
                    interval="1d",
                    start_str=start_date_utc,
                    end_str=end_date_utc,
                    limit=1000,
                )
            else:
                logger.error(f"Unrecognised data frequency")
            output += data
            # timeframe = interval_to_milliseconds(interval)
            if not data:
                logger.error(
                    f"No data returned for {symbol} from {start_date} to {end_date}"
                )
                return None
            last_date = data[-1][0]
            # get day,month and year from last date and compare with end_date day,month and year
            last_datetime = datetime.utcfromtimestamp(last_date / 1000)
            # the below is to check last data point
            if last_date >= end_date_utc:
                break
            if frequency == "monthly":
                start_date_new = last_datetime + relativedelta(months=1)
                start_date_utc = int(start_date_new.timestamp() * 1000)
            elif frequency == "daily":
                start_date_new = last_datetime + relativedelta(days=1)
                start_date_utc = int(start_date_new.timestamp() * 1000)
            else:
                logger.error(f"Unrecognised data frequency")
            logger.info(client_binance.response.headers["x-mbx-used-weight"])
            # for daily add sleep function
            if client_binance.response.headers["x-mbx-used-weight"] == 5000:
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
        logger.error(e, stack_info=True, exc_info=True)
        return None


def spark_df(output: list, columns: list) -> [SparkDataFrame]:
    """Converts output into spark dataframe
    output: list of data from binance
    columns: keys in binance data that will be columns in our dataframe
    """
    try:
        dataframe = spark.createDataFrame(output, columns)
        df = dataframe.withColumn(
            "Kline open time", to_timestamp(col("Kline open time") / 1000)
        )
        df = df.withColumnRenamed("Kline open time", "datetime")
        df = df.select("datetime", "Open Price", "Close Price", "Volume")
        df = df.withColumn("year", year(df["datetime"])).withColumn(
            "month", month(df["datetime"])
        )
        logger.info(df.head(5))
        return df
    except Exception as e:
        logger.error(e, stack_info=True, exc_info=True)
        return None


def upload_minio(symbol: str, bucket_name: str, timeframe: str, df: SparkDataFrame):
    """Uploads spark dataframe as parquet files to minio
    Args:
        bucket_name:
        timeframe: daily or monthly at the moment
    """
    try:
        df.write.mode("append").partitionBy("year", "month").parquet(
            f"s3a://{bucket_name}/{symbol}/{timeframe}"
        )
        return f"Successfully {symbol} {timeframe} uploaded to minio"
    except Exception as e:
        logger.error(e, stack_info=True, exc_info=True)


def parquet_to_df(
    client_minio: Minio, timeframe: str, crypto: str, schema: StructType
) -> [SparkDataFrame]:
    """
    Converts parquet file from our minio object store to spark dataframe
    Args:
        crypto:cryptocurrency pair
        schema:schema defined using StrucType from pyspark
        timeframe: daily or monthly
    """
    try:
        objects = client_minio.list_objects(
            "binancedata", prefix=f"{crypto}/{timeframe}", recursive=True
        )
        filenames = [obj.object_name for obj in objects]
        filenames = [f for f in filenames if "_SUCCESS" not in f]
        df = spark.createDataFrame(data=[], schema=schema)
        for file in filenames:
            df_parquet = spark.read.parquet(f"s3a://binancedata/{file}")
            df = df.union(df_parquet)
        return df
    except Exception as e:
        logger.error(
            f"error reading parquet from minio: {e}", stack_info=True, exc_info=True
        )


def data_cleaning(df: SparkDataFrame) -> [SparkDataFrame]:
    """
    drops nulls, renames and deduplicates dataframe
    """
    try:
        df_null = df.na.drop(how="any", subset=["datetime"])
        df_renamed = df_null.withColumnsRenamed(
            {"Open Price": "open", "Close Price": "close", "Volume": "volume"}
        )
        df_duplicated = df_renamed.dropDuplicates()
        df_duplicated = (
            df_duplicated.withColumn("open", col("open").cast(DecimalType(10, 5)))
            .withColumn("close", col("close").cast(DecimalType(10, 5)))
            .withColumn("volume", col("volume").cast(DecimalType(20, 5)))
        )

        return df_duplicated
    except Exception as e:
        logger.error(f"Error cleaning data:{e}", stack_info=True, exc_info=True)


def add_crypto_id(
    df: SparkDataFrame, df_crypto: SparkDataFrame, crypto: str, currency: str
):
    """
    Adds the crypto id from our crypto table in postgres to our spark dataframe as it is required in price table
    Args:
        df: cleaned spark dataframe
        df_crypto: dataframe version of crypto table
        crypto: cryptocurrency pair such as XRPUSDT
        currency: only USDT at the moment
    """
    try:
        df_crypto = df_crypto.withColumn(
            "trading pair", concat(df_crypto.ticker, lit(currency))
        )
        # obtain the crypto from concatenating currency and ticker
        df_crypto = df_crypto.filter(col("trading pair") == crypto)
        crypto_id = df_crypto.collect()[0]["id"]
        df_id = df.withColumn("crypto_id", lit(crypto_id))
        return df_id
    except Exception as e:
        logger.error(f"Error adding crypto id:{e}", stack_info=True, exc_info=True)
    return df_id


def add_time_id(df: SparkDataFrame, timeframe: str) -> SparkDataFrame:
    """
    Adds a 'time_id' column to the DataFrame based on the 'datetime' column and given timeframe.

    Args:
        df (DataFrame): Spark DataFrame that includes a 'datetime' column of TimestampType.
        timeframe (str): Either 'daily' or 'monthly'. Used to format the datetime value.

    Returns:
        DataFrame: Spark DataFrame with a new 'time_id' column.
    """

    def generate_time_id(dt_value: datetime) -> int:
        if dt_value is None:
            return None
        if timeframe == "daily":
            return int(dt_value.strftime("%Y%m%d"))
        elif timeframe == "monthly":
            return int(dt_value.strftime("%Y%m"))
        else:
            raise ValueError(f"Unsupported timeframe: {timeframe}")

    try:
        time_id_udf = udf(generate_time_id, IntegerType())
        return df.withColumn("time_id", time_id_udf(df["datetime"]))
    except Exception as e:
        logger.error(f"Error adding time_id column: {e}", exc_info=True)
        return df


def upload_time(df: SparkDataFrame, timeframe: str):
    """
    Uploads time to postgres
    """
    # need to futureproof
    df_year = df.withColumn("year", year(df["datetime"]))
    df_month = df_year.withColumn("month", month(df_year["datetime"]))
    if timeframe == "daily":
        df_day = df_month.withColumn("day", day(df_month["datetime"]))
        df_time_filtered = df_day.select(
            ["time_id", "datetime", "year", "month", "day"]
        )
    elif timeframe == "monthly":
        df_time_filtered = df_month.select(["time_id", "datetime", "year", "month"])
    else:
        raise ValueError(f"Unsupported timeframe: {timeframe}")
    df_time = df_time_filtered.withColumnRenamed("time_id", "id")
    try:
        df_time.write.format("jdbc").option(
            "url", "jdbc:postgresql://postgres1:5432/crypto"
        ).option("dbtable", "time").option("user", "postgres").option(
            "password", "postgres"
        ).option(
            "driver", "org.postgresql.Driver"
        ).mode(
            "append"
        ).save()
        logger.info("Successfully uploaded to time table")
    except Exception as e:
        logger.error(
            f"Error uploading to time table:{e}", stack_info=True, exc_info=True
        )


def upload_price(df):
    """
    Uploads dataframe to price table

    """
    df_filtered = df.select(["crypto_id", "time_id", "open", "close", "volume"])
    # logger.info(df_filtered.show(5))
    try:
        df_filtered.write.format("jdbc").option(
            "url", "jdbc:postgresql://postgres1:5432/crypto"
        ).option("dbtable", "price").option("user", "postgres").option(
            "password", "postgres"
        ).option(
            "driver", "org.postgresql.Driver"
        ).mode(
            "append"
        ).save()
        logger.info("Successfully uploaded to price table")
    except Exception as e:
        logger.error(
            f"Error uploading to price table:{e}", stack_info=True, exc_info=True
        )
