"""
Main pipeline orchestrator
Run complete ETL pipeline from raw data to aggregates
"""

import sys
from pathlib import Path
import time

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from loader import DataLoader
from eda import EDA
from cleaner import DataCleaner
from splitter import DataSplitter
from aggregate import Aggregator
from data_validator import Validator


def print_step(step_num, title):
    """Print step header"""
    print("\n" + "="*60)
    print(f"STEP {step_num}: {title}")
    print("="*60)


def main():
    """Run complete pipeline"""
    start_time = time.time()
    
    print("\n" + "="*60)
    print("US ACCIDENTS ANALYSIS PIPELINE (2019-2022)")
    print("="*60)
    
    try:
        # Step 1: Load Data
        print_step(1, "DATA LOADING")
        loader = DataLoader()
        df = loader.load_data()
        df = loader.filter_by_year()
        loader.get_summary()
        
        # Step 2: Exploratory Data Analysis
        print_step(2, "EXPLORATORY DATA ANALYSIS")
        eda = EDA(df)
        eda.run_all()
        
        # Step 3: Data Cleaning
        print_step(3, "DATA CLEANING")
        cleaner = DataCleaner(df)
        df_cleaned = cleaner.run_all()
        
        # Step 4: Create Star Schema
        print_step(4, "STAR SCHEMA CREATION")
        splitter = DataSplitter(df_cleaned)
        splitter.run_all()
        
        # Step 5: Generate Aggregates
        print_step(5, "AGGREGATE GENERATION")
        aggregator = Aggregator()
        aggregator.run_all()
        
        # Step 6: Validate Data
        print_step(6, "DATA VALIDATION")
        validator = Validator()
        validation_passed = validator.run_all()
        
        # Summary
        elapsed = time.time() - start_time
        
        print("\n" + "="*60)
        print("PIPELINE COMPLETED SUCCESSFULLY")
        print("="*60)
        print(f"\nExecution time: {elapsed:.1f} seconds ({elapsed/60:.1f} minutes)")
        print(f"\nValidation: {'PASSED' if validation_passed else 'SOME CHECKS FAILED'}")
        print(f"\nOutput files created in data/processed/:")
        print("  - cleaned/accidents_cleaned.csv")
        print("  - dimensions/dim_time.csv")
        print("  - dimensions/dim_location.csv")
        print("  - dimensions/dim_weather.csv")
        print("  - fact/accident_detail.csv")
        print("  - aggregates/agg_federal.csv          (Dashboard 1)")
        print("  - aggregates/agg_state_anomaly.csv    (Dashboard 2)")
        print("  - aggregates/agg_city_by_state.csv    (Dashboard 3)")
        print("  - aggregates/agg_weather_by_state.csv (Dashboard 4)")
        print("  - aggregates/agg_time_pattern.csv     (Supplementary)")        
        
        print("\nData is ready for Tableau visualization")
        
    except FileNotFoundError as e:
        print(f"\nError: {e}")
        print("\nPlease ensure dataset is placed at:")
        print("  data/raw/US_Accidents_March23.csv")
        print("\nDownload from:")
        print("  https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents")
        sys.exit(1)
        
    except Exception as e:
        print(f"\nError occurred: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()