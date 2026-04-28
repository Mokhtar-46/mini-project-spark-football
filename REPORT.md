# Mini-Project-Spark-Football: Technical Report

## 1. Introduction

This report presents the work carried out on the **Mini-Project-Spark-Football**, a comprehensive data pipeline centered on soccer match results from the Mauritanian football championship (Ligue 1). The project demonstrates the integration of big data technologies including Apache Spark for batch and streaming processing, Kafka for real-time messaging, and Spark MLlib for machine learning predictions.

The primary objective was to build an end-to-end data pipeline that can:
- Collect football match data through web scraping
- Process historical data using batch processing
- Handle real-time data streams for live match analytics
- Train a machine learning model to predict match outcomes

---

## 2. Project Architecture

The project follows a modern data pipeline architecture consisting of four main layers:

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Data Source   │────▶│  Batch Layer    │────▶│  Storage Layer  │
│   (Web Scraper) │     │  (Spark)        │     │  (Parquet)      │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                               │                        │
                               ▼                        ▼
                        ┌─────────────────┐     ┌─────────────────┐
                        │ Streaming Layer │    │  ML Layer       │
                        │ (Kafka+Spark)   │    │  (MLlib)        │
                        └─────────────────┘     └─────────────────┘
```

### Components Overview

| Component | Technology | Purpose |
|-----------|------------|---------|
| Data Collection | BeautifulSoup + Requests | Web scraping from soccer365.net |
| Batch Processing | Apache Spark DataFrame | Data cleaning, transformation, aggregation |
| Message Queue | Apache Kafka | Real-time data streaming |
| Stream Processing | Spark Structured Streaming | Real-time analytics with windowing |
| Storage | Parquet Format | Efficient columnar storage |
| Machine Learning | Spark MLlib (Random Forest) | Match outcome prediction |

---

## 3. Implementation Details

### 3.1 Data Collection (Scraper)

**File**: `src/scraper.py`

The scraper collects current season match data from the Mauritanian Super D1 league:

- **Source**: [soccer365.net](https://soccer365.net/competitions/1090/results/)
- **URL**: `https://soccer365.net/competitions/1090/results/`
- **Technology**: BeautifulSoup for HTML parsing

**Key Features**:
- Extracts match date, home team, away team, and scores
- Handles date parsing (handles month/year logic for season spanning)
- Includes fallback static data if scraping fails
- Outputs to `data/current_season_scraped.csv`

```python
SCRAPE_URL = "https://soccer365.net/competitions/1090/results/"
SEASON_LABEL = "Ligue 1 2025/2026"
```

### 3.2 Batch Data Processing

**File**: `src/prepare_data.py`

The batch processing layer handles historical data from 2007-2025:

**Data Cleaning**:
- Removes rows with missing values in essential columns
- Converts data types (IntegerType for goals, DateType for dates)

**Feature Engineering**:
- `total_goals`: Sum of home and away goals
- `goal_difference`: Difference between home and away goals
- `result`: Classification (home_win, draw, away_win)
- `match_year`: Extracted year from date

**Output**: Parquet format for efficient storage and querying

```python
df_enriched = df_clean \
    .withColumn("total_goals", col("home_goals") + col("away_goals")) \
    .withColumn("goal_difference", col("home_goals") - col("away_goals")) \
    .withColumn("result", 
        when(col("home_goals") > col("away_goals"), "home_win")
        .when(col("home_goals") < col("away_goals"), "away_win")
        .otherwise("draw"))
```

### 3.3 Real-Time Streaming Pipeline

**Files**: `src/producer.py`, `src/stream_job.py`

The streaming layer demonstrates real-time data processing:

#### Kafka Producer (`producer.py`)
- Reads match data from CSV
- Serializes messages as JSON
- Publishes to Kafka topic `soccer_results`
- Includes error handling and connection retry logic

#### Spark Streaming (`stream_job.py`)
- Consumes data from Kafka topic `soccer_results`
- Parses JSON messages using defined schema
- Implements **watermarking** (10 minutes) to handle late-arriving data
- Performs **windowed aggregation** (10-minute tumbling windows)
- Calculates total goals per team within each window
- Outputs to both console and Parquet files

```python
# Watermarking for late data handling
stream_df = stream_df.withWatermark("timestamp", "10 minutes")

# Windowed aggregation
stats_df = stream_df \
    .groupBy(window(col("timestamp"), "10 minutes"), col("team")) \
    .agg(_sum("goals").alias("total_goals"))
```

### 3.4 Machine Learning Model

**File**: `src/train_model.py`

The ML component uses Spark MLlib to predict match outcomes:

**Algorithm**: Random Forest Classifier

**Features**:
- `home_team_index`: Indexed home team name
- `away_team_index`: Indexed away team name

**Target Variable**:
- `label`: Numeric encoding of match result
  - 0: Home Win
  - 1: Draw
  - 2: Away Win

**Pipeline Stages**:
1. StringIndexer (home_team → home_team_index)
2. StringIndexer (away_team → away_team_index)
3. VectorAssembler (combines features)
4. RandomForestClassifier

**Model Configuration**:
- Number of trees: 100
- maxBins: 50 (to accommodate all teams)

**Evaluation**: MulticlassClassificationEvaluator with accuracy metric

### 3.5 Prediction Service

**File**: `src/predict.py`

A simple prediction service that:
- Loads the trained model
- Takes two team names as input
- Returns predicted outcome (Home Win, Draw, or Away Win)

```bash
python predict.py "Nouadhibou" "ASAC Concorde"
```

---

## 4. Data Sources

### Historical Data
- **File**: `data/rim_championnat_results_2007-2025.csv`
- **Coverage**: Mauritanian football championship results from 2007 to 2025
- **Columns**: date, home_team, away_team, home_goals, away_goals

### Current Season Data
- **Source**: [soccer365.net](https://soccer365.net/competitions/1090/results/)
- **League**: Mauritanian Super D1 (Ligue 1)
- **File**: `data/current_season_scraped.csv`

---

## 5. Results and Outputs

### Batch Processing Output
- Cleaned and enriched data saved to `outputs/prepared/cleaned_soccer_data.parquet`
- New columns: total_goals, goal_difference, result, match_year

### Streaming Output
- Real-time aggregation results in `outputs/streaming/realtime_stats/`
- Checkpoint data in `outputs/checkpoints/streaming_job/`

### ML Model
- Trained model saved to `outputs/models/soccer_rf_model/`
- Model can predict match outcomes with given team names

---

## 6. Conclusion

The Mini-Project-Spark-Football successfully demonstrates a complete data pipeline for football match data processing. The project showcases:

1. **Web Scraping**: Automated data collection from external sources
2. **Batch Processing**: Efficient data cleaning and feature engineering with Spark
3. **Real-time Processing**: Kafka integration with Spark Structured Streaming
4. **Windowed Analytics**: Time-windowed aggregation with watermarking for late data
5. **Machine Learning**: Random Forest classification for outcome prediction

The architecture follows industry best practices and can be extended for production use with additional considerations for monitoring, error handling, and scalability.


---
