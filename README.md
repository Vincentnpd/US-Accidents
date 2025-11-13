# US Traffic Accidents Analysis (2019-2022)

Analysis of traffic accident patterns in the United States using data from Kaggle.

## Project Structure

```
us-accidents-analysis/
├── data/
│   ├── raw/                      # Raw dataset
│   └── processed/                # Processed outputs
│       ├── cleaned/              # Cleaned data
│       ├── dimensions/           # Dimension tables
│       ├── fact/                 # Fact table
│       └── aggregates/           # Aggregate tables
├── src/
│   ├── config.py                 # Configuration
│   ├── loader.py                 # Data loading
│   ├── eda.py                    # Exploratory analysis
│   ├── cleaner.py                # Data cleaning
│   ├── splitter.py               # Star schema creation
│   ├── aggregate.py              # Aggregate generation
│   └── utils.py                  # Helper functions
├── main.py                       # Pipeline orchestrator
├── requirements.txt
└── README.md
```

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Download dataset:
   - Source: https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents
   - Place `US_Accidents_March23.csv` in `data/raw/`

3. Create directories:
```bash
mkdir -p data/raw data/processed/{cleaned,dimensions,fact,aggregates}
```

## Usage

Run complete pipeline:
```bash
python main.py
```

Run individual modules:
```bash
python src/loader.py
python src/eda.py
python src/cleaner.py
python src/splitter.py
python src/aggregate.py
```

## Pipeline Steps

1. **Data Loading** (loader.py)
   - Load raw CSV data
   - Filter years 2019-2022
   - Basic statistics

2. **Exploratory Analysis** (eda.py)
   - Temporal patterns
   - Geographic distribution
   - Severity analysis
   - Data quality checks

3. **Data Cleaning** (cleaner.py)
   - Handle missing values
   - Remove outliers
   - Create time features
   - Create weather features
   - Create infrastructure features

4. **Star Schema** (splitter.py)
   - Dimension tables: time, location, weather
   - Fact table: accidents
   - Relationship validation

5. **Aggregates** (aggregate.py)
   - State-year aggregates
   - City-severity aggregates
   - Time pattern aggregates
   - Weather impact aggregates
   - Infrastructure aggregates

## Output Files

### Cleaned Data
- `accidents_cleaned.csv` - Main cleaned dataset

### Star Schema
- `dim_time.csv` - Time dimension
- `dim_location.csv` - Location dimension
- `dim_weather.csv` - Weather dimension
- `fact_accident.csv` - Fact table

### Aggregates (for Tableau)
- `agg_state_year.csv` - State level by year
- `agg_city_severity.csv` - City level with severity
- `agg_time_pattern.csv` - Time patterns
- `agg_weather_impact.csv` - Weather impact
- `agg_infrastructure.csv` - Infrastructure impact

## Configuration

Edit `src/config.py` to customize:
- File paths
- Year range (default 2019-2022)
- Outlier thresholds
- Aggregation parameters

## Data Quality

### Outlier Handling
- Temperature: -20°F to 120°F
- Visibility: 0 to 10 miles
- Duration: 0 to 24 hours
- Invalid severity values removed

### Features Created
- Time: Year, Month, Hour, Day of Week, Time Period, etc.
- Weather: Rain/Snow/Fog flags, Low visibility flag
- Infrastructure: Infrastructure score
- Location: Location ID for joining

## Requirements

- Python 3.8+
- pandas 2.0+
- numpy 1.24+
- pyarrow 12.0+ (for parquet support)

## Dataset Citation

Moosavi, Sobhan, Mohammad Hossein Samavatian, Srinivasan Parthasarathy, and Rajiv Ramnath. "A Countrywide Traffic Accident Dataset.", 2019.

Dataset: https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents

## License

This project is for educational purposes.
