from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as _sum, window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import os

def main():
    # 1. Setup Spark session
    spark = SparkSession.builder \
        .appName("SoccerStreamingAnalytics") \
        .getOrCreate()

    print("--- Started professional streaming process ---")

    # 2. Define Schema to match Producer messages
    json_schema = StructType([
        StructField("date", StringType(), True),
        StructField("season", StringType(), True),
        StructField("home_team", StringType(), True),
        StructField("away_team", StringType(), True),
        StructField("home_goals", IntegerType(), True),
        StructField("away_goals", IntegerType(), True)
    ])

    # 3. Read data from Kafka
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "soccer_results") \
        .option("startingOffsets", "earliest") \
        .load()

    # 4. Data Parsing & Transformation
    # We will convert text to JSON, then convert date column to Timestamp for Windowing
    stream_df = raw_stream.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), json_schema).alias("data")) \
        .select("data.*") \
        .withColumn("timestamp", col("date").cast(TimestampType())) \
        .withWatermark("timestamp", "10 minutes") # Project requirement: handle late data

    # 5. Perform Aggregation using Windowing
    # We will calculate total goals per team within a 10-minute time window
    # Note: We use window(col("timestamp"), "10 minutes") to achieve the Windowing requirement
    stats_df = stream_df \
        .withColumn("team", col("home_team")) \
        .withColumn("goals", col("home_goals")) \
        .groupBy(window(col("timestamp"), "10 minutes"), col("team")) \
        .agg(_sum("goals").alias("total_goals")) \
        .select("window.start", "window.end", "team", "total_goals")

    # 6. Define output paths
    output_path = "../outputs/streaming/realtime_stats"
    checkpoint_path = "../outputs/checkpoints/streaming_job"
    os.makedirs(output_path, exist_ok=True)
    os.makedirs(checkpoint_path, exist_ok=True)

    # --- Output 1: Console display (to see results live) ---
    # We use outputMode("complete") here because Console supports displaying the full table continuously
    query_console = stats_df.writeStream \
        .outputMode("complete") \
        .format("console") \
        .start()

    # --- Output 2: Save to Parquet files (for the project) ---
    # We use outputMode("append") because Parquet does not support complete mode
    # Append mode works here because we used Windowing and Watermarking
    query_file = stats_df.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", output_path) \
        .option("checkpointLocation", checkpoint_path) \
        .start()

    print(f"🚀 System is now running!")
    print(f"👀 Watch the screen to see live results (Console Output).")
    print(f"💾 Final results will be saved to: {output_path}")

    # Wait until the program is stopped
    query_console.awaitTermination()
    query_file.awaitTermination()

if __name__ == "__main__":
    main()