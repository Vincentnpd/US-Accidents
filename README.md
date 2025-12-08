# US Accidents Analysis - ETL Pipeline

Production-ready data pipeline for analyzing US traffic accidents (2019-2022).

## Quick Start

```bash
# 1. Install dependencies
pip install pandas numpy pyarrow python-dotenv

# 2. Setup configuration
cp .env.example .env
# Edit .env with your settings

# 3. Run pipeline
python main.py
```

## Project Structure
```
.
├── config.py           # Configuration management
├── main.py            # Pipeline orchestrator
├── loader.py          # Data loading
├── cleaner.py         # Data cleaning
├── splitter.py        # Star schema creation
├── aggregate.py       # Pre-aggregation
├── eda.py             # Exploratory analysis
├── utils.py           # Helper functions
├── .env.example       # Configuration template
└── data/
    ├── raw/           # Raw CSV data
    └── processed/     # Pipeline outputs
        ├── cleaned/
        ├── dimensions/
        ├── fact/
        └── aggregates/
```

## Pipeline Flow

```
Load --> Clean --> Split --> Aggregate
  ↓        ↓         ↓          ↓
 Raw   Cleaned   Star      Tableau
Data   + Feat   Schema    Aggregates
```

## Data Model

### Star Schema (No dim_weather)

```
dim_time (100K rows)
    |
    v (1:many)
fact_accident (2.1M rows)
    - Weather embedded (Temperature, Visibility, etc.)
    ^ (many:1)
    |
dim_location (500K rows)
```

<<<<<<< HEAD
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
=======
Weather attributes are embedded in fact table because Temperature and Visibility are continuous variables with no dimension reuse.
>>>>>>> b0f1684 (update code)

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

<<<<<<< HEAD
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
=======
### Star Schema
- `accidents_cleaned.csv` - Cleaned data with engineered features
- `dim_time.csv` - Time dimension (unique timestamps)
- `dim_location.csv` - Location dimension (unique locations)
- `fact_accident.csv` - Fact table with embedded weather

### Aggregates (For Tableau)
- `agg_state_year.csv` - State-level yearly trends
- `agg_city_severity.csv` - City-level with Pareto analysis
- `agg_time_pattern.csv` - Temporal patterns (hour x day)
- `agg_weather_impact.csv` - Weather category impact
- `agg_infrastructure.csv` - Infrastructure features
- `model_features.parquet` - ML-ready feature set

## Usage

### Basic

```bash
python main.py              # Run full pipeline
python main.py --validate   # Check outputs
```

### Advanced

```bash
python main.py --eda                     # Include EDA
python main.py --from-step splitter      # Resume from step
python main.py --setup                   # Setup directories
```

### Resume Options

- `--from-step cleaner` - Re-run from cleaning
- `--from-step splitter` - Re-run from star schema
- `--from-step aggregate` - Re-run aggregation only
>>>>>>> b0f1684 (update code)

## Configuration

### .env File

```env
# Required
RAW_DATA_PATH=data/raw/US_Accidents_March23.csv
START_YEAR=2019
END_YEAR=2022

# Optional
DB_HOST=localhost
DB_NAME=us_accidents
PARETO_THRESHOLD=0.8
LOW_VISIBILITY_THRESHOLD=2.0
```

<<<<<<< HEAD
### Key Transformations
- location_id: Concatenation of street and city
- weather_id: 'W' prefix with sequential index after deduplication
- full_date: Date portion extracted from start_time
- duration: Calculated as (end_time - start_time) in minutes
- Infrastructure counts: Aggregated boolean flags by location
=======
### config.py

Contains business logic that cannot go in .env:
- Infrastructure weights (dictionary)
- Peak hour definitions (tuples)
- Path derivation logic
- Utility functions
>>>>>>> b0f1684 (update code)

## Requirements

- Python 3.8+
- pandas >= 2.0.0
- numpy >= 1.24.0
- pyarrow >= 12.0.0
- python-dotenv >= 1.0.0

## Performance

- Execution time: 10-15 minutes
- Memory usage: 4-8 GB
- Output size: ~2 GB total

## Key Design Decisions

### No dim_weather Table

Weather attributes (Temperature, Visibility) are continuous variables creating hundreds of thousands of unique combinations. This violates dimension reusability principles. Solution: embed weather in fact table and categorize at aggregation time.

### config.py + .env Hybrid

- .env: Environment-specific values (credentials, paths)
- config.py: Business logic (dictionaries, derived paths, functions)

Both are necessary. .env only stores strings; config.py handles complex structures.

## Troubleshooting

### FileNotFoundError
Place `US_Accidents_March23.csv` in `data/raw/`

### ModuleNotFoundError
```bash
pip install python-dotenv pandas numpy pyarrow
```

### MemoryError
Reduce year range in .env:
```env
START_YEAR=2022
END_YEAR=2022
```

### Import Errors
From project root:
```bash
python main.py  # Not python src/main.py
```

## Validation

After pipeline completes:

```bash
python main.py --validate
```

Expected output:
```
[EXISTS] Cleaned data: 800.5 MB
[EXISTS] dim_time: 10.2 MB
[EXISTS] dim_location: 52.3 MB
[EXISTS] fact_accident: 205.1 MB
[EXISTS] agg_state_year: 0.1 MB
[EXISTS] agg_city_severity: 2.3 MB
[EXISTS] agg_time_pattern: 0.02 MB
[EXISTS] agg_weather_impact: 0.05 MB
[EXISTS] agg_infrastructure: 3.1 MB
[EXISTS] model_features: 152.4 MB

Validation PASSED
```

## Next Steps

1. Load aggregates into Tableau
2. Build 4-level dashboard:
   - Level 1: Executive Overview
   - Level 2: State Deep-Dive
   - Level 3: Causal Analysis (MECE)
   - Level 4: Predictive Model
3. Train ML model using model_features.parquet

## Data Source

Download from Kaggle:
https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents

## Visualize
https://public.tableau.com/views/Visualize_17651699112650/WeatherDeepDive?:language=en-US&:sid=&:redirect=auth&:display_count=n&:origin=viz_share_link

File: US_Accidents_March23.csv
Place in: data/raw/

## License

Project for academic/analytical purposes.
