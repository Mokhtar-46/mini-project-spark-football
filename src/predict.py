from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
import sys

def main():
    # 1. Setup Spark
    spark = SparkSession.builder \
        .appName("SoccerPrediction") \
        .getOrCreate()

    # 2. Load the model saved in the previous stage
    model_path = "../outputs/models/soccer_rf_model"
    print(f"--- Loading model from: {model_path} ---")
    try:
        model = PipelineModel.load(model_path)
        print("✅ Model loaded successfully!")
    except Exception as e:
        print(f"❌ Failed to load model: {e}")
        return

    # 3. Function to convert number (0, 1, 2) to readable text (Home Win, Draw, Away Win)
    def map_prediction(pred):
        if pred == 0.0: return "Home Win"
        if pred == 1.0: return "Draw"
        if pred == 2.0: return "Away Win"
        return "Unknown"

    map_prediction_udf = udf(map_prediction, StringType())

    # 4. Receive match from user via Terminal
    # Usage: python3 predict.py "Team A" "Team B"
    if len(sys.argv) < 3:
        print("\n❌ Error: You must enter the home team name and the away team name.")
        print("Example usage:")
        print("python3 predict.py 'Nouadhibou AC' 'ASAC Concorde'")
        return

    home_team_input = sys.argv[1]
    away_team_input = sys.argv[2]

    print(f"\n🔍 Predicting match result: {home_team_input} VS {away_team_input}...")

    # 5. Create DataFrame for the new match
    new_match_data = [(home_team_input, away_team_input)]
    new_match_df = spark.createDataFrame(new_match_data, ["home_team", "away_team"])

    # 6. Use the model for prediction
    # The Pipeline will automatically convert names to numbers then predict
    predictions = model.transform(new_match_df)

    # 7. Display the final result
    result = predictions.select(
        col("home_team"), 
        col("away_team"), 
        map_prediction_udf(col("prediction")).alias("predicted_result")
    ).collect()

    if result:
        res = result[0]
        print("\n" + "="*40)
        print(f"⚽ Predicted match result:")
        print(f"   {res['home_team']}  VS  {res['away_team']}")
        print(f"   👉 Prediction: {res['predicted_result']}")
        print("="*40 + "\n")
    else:
        print("❌ No result found.")

    spark.stop()

if __name__ == "__main__":
    main()