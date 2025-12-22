"""
Configuration Module - Single Source of Truth for All Settings
==============================================================
All project constants defined here. Import and use everywhere.
NEVER hardcode values in other modules.
"""

from pathlib import Path

# =============================================================================
# PATHS
# =============================================================================

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_DATA_PATH = DATA_DIR / "raw" / "US_Accidents_March23.csv"

PROCESSED_DIR = DATA_DIR / "processed"
CLEANED_DIR = PROCESSED_DIR / "cleaned"
DIM_DIR = PROCESSED_DIR / "dimensions"
FACT_DIR = PROCESSED_DIR / "fact"
AGG_DIR = PROCESSED_DIR / "aggregates"

# =============================================================================
# TIME RANGE
# =============================================================================

START_YEAR = 2019
END_YEAR = 2022

# =============================================================================
# OUTLIER THRESHOLDS - Data Quality
# =============================================================================

# Temperature (Fahrenheit)
TEMP_MIN = -20
TEMP_MAX = 120

# Visibility (miles)
VISIBILITY_MIN = 0
VISIBILITY_MAX = 10

# Duration (hours) - CRITICAL: Used for capping
DURATION_MAX_HOURS = 24
DURATION_MAX_MIN = DURATION_MAX_HOURS * 60  # 1440 minutes

# Precipitation (inches)
PRECIPITATION_MAX = 5

# Severity
SEVERITY_MIN = 1
SEVERITY_MAX = 4
HIGH_SEVERITY_THRESHOLD = 3  # Severity >= 3 is "high"

# =============================================================================
# INFRASTRUCTURE WEIGHTS - For Risk Score Calculation
# =============================================================================

# Junction most dangerous (weight 3), Traffic Signal and Crossing (weight 2)
INFRA_WEIGHTS = {
    'Junction': 3,
    'Traffic_Signal': 2,
    'Crossing': 2,
    'Stop': 1,
    'Amenity': 1
}

INFRA_COLUMNS = list(INFRA_WEIGHTS.keys())

# =============================================================================
# WEATHER CATEGORIES
# =============================================================================

# Keywords for weather categorization (order matters - first match wins)
WEATHER_KEYWORDS = {
    'Foggy': ['fog', 'mist', 'haze'],
    'Snowy': ['snow', 'ice', 'sleet', 'freezing'],
    'Rainy': ['rain', 'drizzle', 'shower'],
    'Windy': ['wind', 'storm', 'thunder'],
    'Cloudy': ['cloud', 'overcast']
}
# Default category if no match: 'Clear'

# Base risk scores by weather type
WEATHER_RISK_BASE = {
    'Clear': 0,
    'Cloudy': 1,
    'Windy': 2,
    'Rainy': 3,
    'Snowy': 4,
    'Foggy': 5
}

# =============================================================================
# AGGREGATION PARAMETERS
# =============================================================================

TOP_N_STATES = 20
TOP_N_CITIES = 100
PARETO_THRESHOLD = 0.8  # 80/20 rule

# Z-score thresholds for anomaly detection
ZSCORE_CRITICAL = 2.0
ZSCORE_HIGH = 1.0
ZSCORE_ELEVATED = 0.0

# Risk score thresholds
RISK_EXTREME = 8
RISK_HIGH = 6
RISK_MODERATE = 4

# =============================================================================
# VALIDATION THRESHOLDS
# =============================================================================

# Expected ranges for validation
VALIDATION_RULES = {
    'Severity': {'min': 1, 'max': 4, 'null_pct_max': 0},
    'Duration_min': {'min': 0, 'max': 1440, 'null_pct_max': 0.05},
    'Temperature(F)': {'min': -20, 'max': 120},
    'Visibility(mi)': {'min': 0, 'max': 10},
    'Year': {'min': 2019, 'max': 2022},
}

# Aggregate-specific validation
AGG_VALIDATION = {
    'severity_impact_pct': {'min': -50, 'max': 100},
    'duration_impact_pct': {'min': -50, 'max': 200},
    'pct_of_national': {'min': 0, 'max': 100},
    'pct_of_state': {'min': 0, 'max': 100},
}

# =============================================================================
# FILE RETRY SETTINGS
# =============================================================================

FILE_MAX_RETRIES = 3
FILE_RETRY_DELAY = 2  # seconds


# =============================================================================
# PRINT SETTINGS FOR DEBUGGING
# =============================================================================

if __name__ == "__main__":
    print("Configuration Settings")
    print("=" * 60)
    print(f"\nPaths:")
    print(f"  BASE_DIR: {BASE_DIR}")
    print(f"  DATA_DIR: {DATA_DIR}")
    print(f"  RAW_DATA_PATH: {RAW_DATA_PATH}")
    
    print(f"\nTime Range: {START_YEAR} - {END_YEAR}")
    
    print(f"\nOutlier Thresholds:")
    print(f"  Temperature: {TEMP_MIN} to {TEMP_MAX} F")
    print(f"  Visibility: {VISIBILITY_MIN} to {VISIBILITY_MAX} mi")
    print(f"  Duration: max {DURATION_MAX_HOURS} hours ({DURATION_MAX_MIN} min)")
    print(f"  Severity: {SEVERITY_MIN} to {SEVERITY_MAX}")
    
    print(f"\nInfrastructure Weights: {INFRA_WEIGHTS}")
    print(f"Weather Risk Base: {WEATHER_RISK_BASE}")