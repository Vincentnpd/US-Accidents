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
   - Standardize data types
   - Validate severity values

4. **Star Schema** (splitter.py)
   - Create dimension tables: dim_date, dim_location, dim_weather
   - Create fact table: accident_details
   - Validate referential integrity

5. **Aggregates** (aggregate.py)
   - State-year aggregates
   - City-severity aggregates
   - Time pattern aggregates
   - Weather impact aggregates
   - Infrastructure aggregates

## Star Schema Design

### Schema Diagram
```
                          ┌─────────────────────┐
                          │      dim_date       │
                          ├─────────────────────┤
                          │ full_date (PK)      │
                          │ day                 │
                          │ month               │
                          │ quarter             │
                          │ year                │
                          └──────────┬──────────┘
                                     │
                                     │
┌─────────────────────┐              │              ┌─────────────────────┐
│    dim_location     │              │              │    dim_weather      │
├─────────────────────┤              │              ├─────────────────────┤
│ location_id (PK)    │              │              │ weather_id (PK)     │
│ street              │              │              │ weather_condition   │
│ city                │              │              └──────────┬──────────┘
│ county              │              │                         │
│ state               │    ┌─────────┴─────────┐               │
│ zipcode             │    │  accident_details │               │
│ timezone            │    ├───────────────────┤               │
│ total_amenity       │    │ id (PK)           │               │
│ total_crossing      │    │ severity          │               │
│ total_junction      ├────┤ weather_id (FK)   ├───────────────┘
│ total_stop          │    │ location_id (FK)  │
│ total_traffic_signal│    │ full_date (FK)    │
└─────────────────────┘    │ start_time        │
                           │ end_time          │
                           │ duration          │
                           │ description       │
                           └───────────────────┘
```

### Table Definitions

#### dim_date
| Column | Type | Description |
|--------|------|-------------|
| full_date | DATE (PK) | Date extracted from start_time |
| day | INT | Day of month (1-31) |
| month | INT | Month (1-12) |
| quarter | INT | Quarter (1-4) |
| year | INT | Year |

#### dim_location
| Column | Type | Description |
|--------|------|-------------|
| location_id | VARCHAR (PK) | Composite key: street_city |
| street | VARCHAR | Street name |
| city | VARCHAR | City name |
| county | VARCHAR | County name |
| state | VARCHAR | State code |
| zipcode | VARCHAR | Zip code |
| timezone | VARCHAR | Timezone |
| total_amenity | INT | Count of accidents near amenity |
| total_crossing | INT | Count of accidents near crossing |
| total_junction | INT | Count of accidents near junction |
| total_stop | INT | Count of accidents near stop sign |
| total_traffic_signal | INT | Count of accidents near traffic signal |

#### dim_weather
| Column | Type | Description |
|--------|------|-------------|
| weather_id | VARCHAR (PK) | Surrogate key: W + index |
| weather_condition | VARCHAR | Weather condition description |

#### accident_details (Fact Table)
| Column | Type | Description |
|--------|------|-------------|
| id | VARCHAR (PK) | Accident ID from source |
| severity | INT | Severity level (1-4) |
| weather_id | VARCHAR (FK) | Reference to dim_weather |
| location_id | VARCHAR (FK) | Reference to dim_location |
| full_date | DATE (FK) | Reference to dim_date |
| start_time | TIMESTAMP | Accident start time |
| end_time | TIMESTAMP | Accident end time |
| duration | DECIMAL | Duration in minutes |
| description | TEXT | Accident description |

## Output Files

### Cleaned Data
- `accidents_cleaned.csv` - Main cleaned dataset

### Star Schema (dimensions/)
- `dim_date.csv` - Date dimension
- `dim_location.csv` - Location dimension with infrastructure counts
- `dim_weather.csv` - Weather condition dimension

### Star Schema (fact/)
- `accident_details.csv` - Fact table

### Aggregates (aggregates/)
- `agg_state_year.csv` - State level metrics by year
- `agg_city_severity.csv` - City level with severity breakdown
- `agg_time_pattern.csv` - Time-based patterns
- `agg_weather_impact.csv` - Weather condition impact
- `agg_infrastructure.csv` - Infrastructure feature impact

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

### Key Transformations
- location_id: Concatenation of street and city
- weather_id: 'W' prefix with sequential index after deduplication
- full_date: Date portion extracted from start_time
- duration: Calculated as (end_time - start_time) in minutes
- Infrastructure counts: Aggregated boolean flags by location

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
