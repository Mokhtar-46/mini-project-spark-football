# Mini-Project-Spark-Football

A comprehensive data pipeline centered on soccer match results from the Mauritanian football championship (Ligue 1), combining:

- **Batch Processing** with Apache Spark
- **Real-time Processing** with Kafka and Spark Structured Streaming
- **Data Storage** in Parquet format
- **Machine Learning** using Spark MLlib (Random Forest classifier)

---

## 📊 Project Overview

This project processes football match data from the Mauritanian Super D1 league:

| Component | Description |
|-----------|-------------|
| **Data Source** | Mauritanian Ligue 1 (2007-2025 historical + current season) |
| **Scraping** | [soccer365.net](https://soccer365.net/competitions/1090/results/) |
| **Batch Processing** | Spark DataFrame operations for data cleaning & transformation |
| **Streaming** | Kafka + Spark Structured Streaming for real-time analytics |
| **ML Model** | Random Forest classifier for match outcome prediction |

---

## 🏗️ Project Structure

```
mini-project-spark-football/
├── data/
│   ├── rim_championnat_results_2007-2025.csv  # Historical data
│   └── current_season_scraped.csv              # Current season (scraped)
├── notebooks/
│   ├── prepare_data.ipynb                       # Data preparation notebook
│   └── train_model.ipynb                        # ML training notebook
├── src/
│   ├── scraper.py                               # Web scraper for current season
│   ├── prepare_data.py                          # Batch data processing
│   ├── producer.py                              # Kafka message producer
│   ├── stream_job.py                            # Spark Streaming job
│   ├── train_model.py                           # ML model training
│   └── predict.py                               # Match prediction script
├── outputs/
│   ├── prepared/                                # Cleaned Parquet data
│   ├── models/                                  # Trained ML model
│   ├── streaming/                               # Real-time results
│   └── checkpoints/                             # Streaming checkpoints
└── README.md
```

---

## 🚀 How to Run the Project

### Prerequisites

- Python 3.8+
- Apache Spark
- Kafka (running on localhost:9092 or kafka:9092)
- Required Python packages:
  ```bash
  pip install pyspark kafka-python pandas requests beautifulsoup4
  ```

### Step 1: Scrape Current Season Data

```bash
cd src
python scraper.py
```

This scrapes the latest Mauritanian Super D1 results from **soccer365.net** and saves them to `data/current_season_scraped.csv`.

### Step 2: Prepare Historical Data (Batch Processing)

```bash
cd src
python prepare_data.py
```

This cleans the historical data, creates derived columns (total_goals, goal_difference, result, match_year), and saves it in Parquet format.

### Step 3: Train the ML Model

```bash
cd src
python train_model.py
```

Trains a Random Forest classifier to predict match outcomes (home_win, draw, away_win).

### Step 4: Run Kafka Producer

```bash
cd src
python producer.py
```

Sends match data to the Kafka topic `soccer_results` for real-time processing.

### Step 5: Run Spark Streaming Job

```bash
cd src
python stream_job.py
```

Processes real-time data from Kafka, performs windowed aggregation (10-minute windows), and outputs results to console and Parquet files.

### Step 6: Predict Match Results

```bash
cd src
python predict.py "Team A" "Team B"
```

Example:
```bash
python predict.py "Nouadhibou" "ASAC Concorde"
```

---

## 🔧 Data Sources

### Historical Data
- **File**: `data/rim_championnat_results_2007-2025.csv`
- **Content**: Mauritanian football championship results from 2007 to 2025

### Current Season (Scraped)
- **Source**: [soccer365.net](https://soccer365.net/competitions/1090/results/)
- **League**: Mauritanian Super D1 (Ligue 1)
- **URL**: `https://soccer365.net/competitions/1090/results/`
- **File**: `data/current_season_scraped.csv`

The scraper uses BeautifulSoup to parse HTML and extracts:
- Match date
- Home team name
- Away team name
- Home goals
- Away goals

---

## 📈 Output Examples

### Batch Processing Output
Cleaned data saved to Parquet with columns:
- `date`, `home_team`, `away_team`, `home_goals`, `away_goals`
- `total_goals`, `goal_difference`, `result`, `match_year`

### Streaming Output
Real-time aggregation showing total goals per team within 10-minute windows.

### ML Prediction
```
⚽ Predicted match result:
   Nouadhibou  VS  ASAC Concorde
   👉 Prediction: Home Win
```

---

## 📝 License

This project is for educational purposes.
