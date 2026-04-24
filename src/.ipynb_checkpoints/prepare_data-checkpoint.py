from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, year
from pyspark.sql.types import IntegerType, DateType
import os

def main():
    # 1. Setup Spark session
    spark = SparkSession.builder \
        .appName("SoccerDataPreparation") \
        .getOrCreate()

    # Using absolute path to avoid any issues
    # Note: Make sure this is the actual path to the file inside the container
    input_path = "/home/jovyan/work/mini-project-spark-football/data/rim_championnat_results_2007-2025.csv"
    output_path = "/home/jovyan/work/mini-project-spark-football/outputs/prepared/cleaned_soccer_data.parquet"

    print(f"--- Started process: Reading file from {input_path} ---")

    try:
        # 2. Load data
        if not os.path.exists(input_path):
            raise FileNotFoundError(f"File not found at path: {input_path}")

        df = spark.read.csv(input_path, header=True, inferSchema=True)
        print("✅ File loaded successfully.")
        print("Original Schema:")
        df.printSchema()

        # 3. Cleaning and transformation
        print("--- Cleaning data and creating new columns ---")
        df_clean = df.dropna(subset=["date", "home_team", "away_team", "home_goals", "away_goals"]) \
                     .withColumn("home_goals", col("home_goals").cast(IntegerType())) \
                     .withColumn("away_goals", col("away_goals").cast(IntegerType())) \
                     .withColumn("date", col("date").cast(DateType()))

        df_enriched = df_clean \
            .withColumn("total_goals", col("home_goals") + col("away_goals")) \
            .withColumn("goal_difference", col("home_goals") - col("away_goals")) \
            .withColumn("result", 
                when(col("home_goals") > col("away_goals"), "home_win")
                .when(col("home_goals") < col("away_goals"), "away_win")
                .otherwise("draw")) \
            .withColumn("match_year", year(col("date")))

        # 4. Save results
        print(f"--- Saving results to {output_path} ---")
        df_enriched.write.mode("overwrite").parquet(output_path)
        
        print("✅ Process completed successfully!")
        print("Data is now ready in Parquet format.")

    except Exception as e:
        print(f"❌ An error occurred during execution: {e}")
    
    finally:
        # Always close the session
        spark.stop()
        print("--- Spark Session closed ---")

if __name__ == "__main__":
    main()