from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import StringType, IntegerType, DoubleType
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
import os

def main():
    # 1. Setup Spark session
    spark = SparkSession.builder \
        .appName("SoccerMLTraining") \
        .getOrCreate()

    print("--- Started AI model training process (improved version) ---")

    # 2. Define paths
    train_path = "../outputs/prepared/cleaned_soccer_data.parquet"
    test_path = "../data/current_season_scraped.csv"

    # 3. Load data
    print(f"📥 Loading training data from: {train_path}")
    df_train = spark.read.parquet(train_path)

    print(f"📥 Loading test data from: {test_path}")
    df_test = spark.read.csv(test_path, header=True, inferSchema=True)

    # 4. Convert result to numbers (Label) using UDF
    def map_result_logic(res):
        if res == 'home_win': return 0.0
        if res == 'draw': return 1.0
        if res == 'away_win': return 2.0
        return 1.0 # default to draw
    
    map_result_udf = udf(map_result_logic, DoubleType())

    # Prepare training data
    df_train = df_train.withColumn("label", map_result_udf(col("result")))

    # Prepare test data
    if 'result' in df_test.columns:
        df_test = df_test.withColumn("label", map_result_udf(col("result")))
    else:
        df_test = df_test.withColumn("label", lit(1.0))

    # 5. Feature Engineering
    # Convert team names to numbers
    indexer_home = StringIndexer(inputCol="home_team", outputCol="home_team_index", handleInvalid="keep")
    indexer_away = StringIndexer(inputCol="away_team", outputCol="away_team_index", handleInvalid="keep")

    # Assemble features into a vector
    assembler = VectorAssembler(
        inputCols=["home_team_index", "away_team_index"],
        outputCol="features"
    )

    # 6. Build the model with maxBins fix 🚀
    # Added maxBins=50 to accommodate 44 teams
    rf = RandomForestClassifier(
        labelCol="label", 
        featuresCol="features", 
        numTrees=100, 
        maxBins=50 
    )

    # 7. Create Pipeline
    pipeline = Pipeline(stages=[indexer_home, indexer_away, assembler, rf])

    # 8. Training
    print("🚀 Training Random Forest model... (please wait a moment)")
    model = pipeline.fit(df_train)
    print("✅ Training completed successfully!")

    # 9. Evaluation
    print("🧪 Testing model on current season data...")
    predictions = model.transform(df_test)

    evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)

    print(f"\n📊 Final results:")
    print(f"🎯 Model Accuracy: {accuracy * 100:.2f}%")

    # 10. Display sample predictions
    print("\n🔮 Sample of model predictions (0: home win, 1: draw, 2: away win):")
    predictions.select("home_team", "away_team", "label", "prediction").show(10)

    # 11. Save model
    model_save_path = "../outputs/models/soccer_rf_model"
    model.save(model_save_path)
    print(f"💾 Model saved at: {model_save_path}")

    spark.stop()

if __name__ == "__main__":
    main()