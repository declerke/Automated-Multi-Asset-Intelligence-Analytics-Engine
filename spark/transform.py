import sys
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, from_unixtime, lit, 
    round as spark_round, lag, when, abs as spark_abs,
    timestamp_millis, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, 
    DoubleType, IntegerType, TimestampType
)
from pyspark.sql.window import Window

def create_spark_session(app_name="CryptoLake-Processing"):
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.32.0") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MILLIS") \
        .config("spark.sql.parquet.writeLegacyFormat", "false") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def define_raw_schema():
    return StructType([
        StructField("open_time", LongType(), False),
        StructField("open", DoubleType(), False),
        StructField("high", DoubleType(), False),
        StructField("low", DoubleType(), False),
        StructField("close", DoubleType(), False),
        StructField("volume", DoubleType(), False),
        StructField("close_time", LongType(), False),
        StructField("quote_volume", DoubleType(), False),
        StructField("trades_count", IntegerType(), False),
        StructField("taker_buy_base_volume", DoubleType(), False),
        StructField("taker_buy_quote_volume", DoubleType(), False),
        StructField("ignore", StringType(), True)
    ])

def load_raw_data(spark, input_path, symbol):
    schema = define_raw_schema()
    
    df = spark.read \
        .option("header", "false") \
        .option("mode", "DROPMALFORMED") \
        .schema(schema) \
        .csv(input_path)
    
    df = df.withColumn("symbol", lit(symbol))
    
    return df

def transform_data(df):
    df = df.withColumn(
        "open_time",
        timestamp_millis((col("open_time") / 1000).cast("long"))
    )
    
    df = df.withColumn(
        "close_time",
        timestamp_millis((col("close_time") / 1000).cast("long"))
    )
    
    df = df.filter(
        (col("open") > 0) & 
        (col("high") > 0) & 
        (col("low") > 0) & 
        (col("close") > 0) & 
        (col("volume") >= 0)
    )
    
    df = df.filter(col("high") >= col("low"))
    df = df.filter((col("high") >= col("open")) & (col("high") >= col("close")))
    df = df.filter((col("low") <= col("open")) & (col("low") <= col("close")))
    
    window_spec = Window.partitionBy("symbol").orderBy("open_time")
    
    df = df.withColumn("prev_close", lag("close", 1).over(window_spec))
    
    df = df.withColumn(
        "daily_return_pct",
        when(
            col("prev_close").isNotNull() & (col("prev_close") > 0),
            spark_round(((col("close") - col("prev_close")) / col("prev_close")) * 100, 4)
        ).otherwise(None)
    )
    
    df = df.withColumn(
        "price_range_pct",
        when(
            col("low") > 0,
            spark_round(((col("high") - col("low")) / col("low")) * 100, 4)
        ).otherwise(None)
    )
    
    df = df.drop("prev_close", "ignore")
    
    df = df.withColumn("ingestion_timestamp", current_timestamp())
    
    df = df.dropDuplicates(["symbol", "open_time"])
    
    df = df.select(
        "symbol",
        "open_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_time",
        "quote_volume",
        "trades_count",
        "taker_buy_base_volume",
        "taker_buy_quote_volume",
        "daily_return_pct",
        "price_range_pct",
        "ingestion_timestamp"
    )
    
    return df

def save_to_parquet(df, output_path):
    df.coalesce(1) \
        .write \
        .mode("overwrite") \
        .option("parquet.writer.version", "v2") \
        .parquet(output_path)

def process_symbol_data(spark, input_path, output_path, symbol):
    print(f"Processing {symbol}...")
    print(f"Input: {input_path}")
    print(f"Output: {output_path}")
    
    df = load_raw_data(spark, input_path, symbol)
    print(f"Loaded {df.count()} raw records for {symbol}")
    
    df_transformed = transform_data(df)
    print(f"Transformed to {df_transformed.count()} clean records")
    
    save_to_parquet(df_transformed, output_path)
    print(f"Saved to {output_path}")
    
    return df_transformed.count()

def main():
    if len(sys.argv) < 4:
        print("Usage: spark_transform.py <input_path> <output_path> <symbol>")
        sys.exit(1)
    
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    symbol = sys.argv[3]
    
    spark = create_spark_session()
    
    try:
        record_count = process_symbol_data(spark, input_path, output_path, symbol)
        print(f"Successfully processed {record_count} records for {symbol}")
        
    except Exception as e:
        print(f"Error processing {symbol}: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
