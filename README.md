# Project Summary

## Overview
Analysis of US traffic accidents (2019-2022) using ETL pipeline to generate insights for Tableau visualization.

## Files Created

### Core Modules (src/)
1. **config.py** - Configuration settings
2. **loader.py** - Load and filter data
3. **eda.py** - Exploratory analysis
4. **cleaner.py** - Clean data and create features
5. **splitter.py** - Create star schema
6. **aggregate.py** - Generate aggregates
7. **utils.py** - Helper functions

### Main Pipeline
- **main.py** - Orchestrates entire pipeline

### Setup
- **setup.py** - Create directory structure
- **requirements.txt** - Python dependencies
- **README.md** - Documentation

## Quick Start

```bash
# 1. Setup
python setup.py
pip install -r requirements.txt

# 2. Download data
# Place US_Accidents_March23.csv in data/raw/

# 3. Run pipeline
python main.py
```

## Pipeline Flow

```
Raw Data (7.7M records)
    ↓
[loader.py] Filter 2019-2022
    ↓
[eda.py] Analyze patterns
    ↓
[cleaner.py] Clean + Create features
    ↓
[splitter.py] Create star schema
    ↓
[aggregate.py] Generate aggregates
    ↓
Output: 10 CSV files ready for Tableau
```

## Output Structure

```
data/processed/
├── cleaned/
│   └── accidents_cleaned.csv          (Main dataset)
├── dimensions/
│   ├── dim_time.csv                   (Time dimension)
│   ├── dim_location.csv               (Location dimension)
│   └── dim_weather.csv                (Weather dimension)
├── fact/
│   └── fact_accident.csv              (Fact table)
└── aggregates/
    ├── agg_state_year.csv             (State trends)
    ├── agg_city_severity.csv          (City analysis)
    ├── agg_time_pattern.csv           (Time patterns)
    ├── agg_weather_impact.csv         (Weather effects)
    └── agg_infrastructure.csv         (Infrastructure)
```

## Key Features

### Data Cleaning
- Missing value imputation
- Outlier handling (domain knowledge based)
- Invalid record removal

### Feature Engineering
- 9 time features (Year, Month, Hour, Time_Period, etc.)
- 4 weather features (Is_Rain, Is_Snow, Is_Fog, Low_Visibility)
- 1 infrastructure feature (Infra_Score)
- Location ID for joining

### Star Schema
- 1 Fact table: accident details
- 3 Dimension tables: time, location, weather
- Validated relationships

### Aggregates
- 5 aggregate tables for different analyses
- Pareto analysis (80/20 rule)
- Year-over-year trends

## Technical Details

### Data Volume
- Input: ~7.7M records (2016-2023)
- Filtered: ~2.8M records (2019-2022)
- Final: ~2.8M records after cleaning

### Processing Time
- Expected: 5-10 minutes on standard laptop
- Depends on system specifications

### Memory Usage
- Raw data: ~3.2 GB
- Processed: ~1.8 GB (optimized dtypes)

## Code Quality

### Best Practices Applied
- Modular design (separate concerns)
- Configuration file for parameters
- Error handling
- Input validation
- Memory optimization
- Clear documentation

### No External Styling
- Clean, professional code
- No emoji or excessive formatting
- Academic-appropriate style
- Suitable for university submission

## For Tableau

All output files are ready to import into Tableau:
1. Import aggregate files
2. Create relationships if needed
3. Build dashboards based on aggregates

## Customization

Edit `src/config.py` to change:
- Year range
- Outlier thresholds
- File paths
- Aggregation parameters

## Dataset Source

Kaggle: US Accidents Dataset (Sobhan Moosavi)
https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents

## Execution

Single command runs entire pipeline:
```bash
python main.py
```

Individual modules can be tested:
```bash
python src/loader.py
python src/eda.py
python src/cleaner.py
python src/splitter.py
python src/aggregate.py
```
