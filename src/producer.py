import json
import time
import pandas as pd
from kafka import KafkaProducer
import os

def main():
    # 1. Set up paths
    input_path = "../data/current_season_scraped.csv"
    
    # 2. Kafka settings
    bootstrap_servers = ['localhost:9092', 'kafka:9092']
    topic_name = 'soccer_results'

    print(f"--- Starting live stream simulation (improved version) ---")

    # 3. Load data and process immediately to avoid type errors
    if not os.path.exists(input_path):
        print(f"❌ Error: File not found at {input_path}")
        return
    
    try:
        df = pd.read_csv(input_path)
        
        # Clean data and convert columns to correct types immediately
        # We'll use pd.to_numeric with errors='coerce' to convert any non-numeric to NaN then convert to 0
        df['home_goals'] = pd.to_numeric(df['home_goals'], errors='coerce').fillna(0).astype(int)
        df['away_goals'] = pd.to_numeric(df['away_goals'], errors='coerce').fillna(0).astype(int)
        
        # Ensure text columns are explicitly strings
        df['home_team'] = df['home_team'].astype(str)
        df['away_team'] = df['away_team'].astype(str)
        df['date'] = df['date'].astype(str)
        df['season'] = df['season'].astype(str)

        print(f"✅ Successfully loaded and prepared {len(df)} matches.")
    except Exception as e:
        print(f"❌ Failed to process CSV file: {e}")
        return

    # 4. Create the Producer
    producer = None
    for server in bootstrap_servers:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[server],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print(f"✅ Successfully connected to Kafka via: {server}")
            break
        except Exception:
            continue

    if producer is None:
        print("❌ Failed to connect to Kafka. Make sure the Kafka container is running.")
        return

    # 5. Send data
    try:
        for index, row in df.iterrows():
            # Create the message using the cleaned values from the DataFrame
            message = {
                "date": row['date'],
                "season": row['season'],
                "home_team": row['home_team'],
                "away_team": row['away_team'],
                "home_goals": int(row['home_goals']),
                "away_goals": int(row['away_goals'])
            }

            producer.send(topic_name, value=message)
            print(f"📢 [Live stream]: {message['home_team']} {message['home_goals']} - {message['away_goals']} {message['away_team']}")
            
            time.sleep(2) 

        print("--- Stream ended successfully ---")

    except KeyboardInterrupt:
        print("\n🛑 Stream stopped.")
    except Exception as e:
        print(f"❌ An error occurred during sending: {e}")
    finally:
        if producer:
            producer.flush()
            producer.close()
        print("--- Connection closed ---")

if __name__ == "__main__":
    main()