"""
Configuration settings for the project
"""

from pathlib import Path

# Paths
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_DATA_PATH = DATA_DIR / "raw" / "US_Accidents_March23.csv"

PROCESSED_DIR = DATA_DIR / "processed"
CLEANED_DIR = PROCESSED_DIR / "cleaned"
DIM_DIR = PROCESSED_DIR / "dimensions"
FACT_DIR = PROCESSED_DIR / "fact"
AGG_DIR = PROCESSED_DIR / "aggregates"

# Analysis parameters
START_YEAR = 2019
END_YEAR = 2022

# Outlier thresholds
TEMP_MIN = -20
TEMP_MAX = 120
VISIBILITY_MAX = 10
DURATION_MAX_HOURS = 24
PRECIPITATION_MAX = 5

# Aggregation parameters
TOP_N_STATES = 20
TOP_N_CITIES = 100
PARETO_THRESHOLD = 0.8