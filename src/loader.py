"""
Data loading module
Load raw data and filter by time period
"""

import pandas as pd
from pathlib import Path
import config


class DataLoader:
    """Load and filter accident data"""
    
    def __init__(self):
        self.raw_path = config.RAW_DATA_PATH
        self.df = None
        
    def load_data(self):
        """Load raw CSV data with optimized dtypes"""
        print(f"\nLoading data from {self.raw_path}")
        
        if not self.raw_path.exists():
            raise FileNotFoundError(f"Dataset not found at {self.raw_path}")
        
        dtype_dict = {
            'ID': 'str',
            'Severity': 'int8',
            'City': 'str',
            'State': 'str',
            'Start_Lat': 'float32',
            'Start_Lng': 'float32',
            'End_Lat': 'float32',
            'End_Lng': 'float32',
            'Temperature(F)': 'float32',
            'Visibility(mi)': 'float32',
            'Precipitation(in)': 'float32',
            'Amenity': 'bool',
            'Crossing': 'bool',
            'Junction': 'bool',
            'Railway': 'bool',
            'Station': 'bool',
            'Stop': 'bool',
            'Traffic_Signal': 'bool',
        }
        
        self.df = pd.read_csv(
            self.raw_path,
            dtype=dtype_dict,
            parse_dates=['Start_Time', 'End_Time'],
            low_memory=False
        )
        
        print(f"Loaded {len(self.df):,} records")
        return self.df
    
    def filter_by_year(self, start_year=None, end_year=None):
        """Filter data by year range"""
        start_year = start_year or config.START_YEAR
        end_year = end_year or config.END_YEAR
        
        print(f"Filtering data: {start_year}-{end_year}")
        self.df['Start_Time'] = pd.to_datetime(self.df['Start_Time'], errors='coerce')
        self.df['End_Time'] = pd.to_datetime(self.df['End_Time'], errors='coerce')        
        self.df['Year'] = self.df['Start_Time'].dt.year
        initial_count = len(self.df)
        
        self.df = self.df[
            (self.df['Year'] >= start_year) & 
            (self.df['Year'] <= end_year)
        ].copy()
        
        print(f"Filtered to {len(self.df):,} records ({len(self.df)/initial_count*100:.1f}%)")
        return self.df
    
    def get_summary(self):
        """Print basic summary statistics"""
        print("\n" + "="*60)
        print("Data Summary")
        print("="*60)
        print(f"Total records: {len(self.df):,}")
        print(f"Date range: {self.df['Start_Time'].min()} to {self.df['Start_Time'].max()}")
        print(f"States: {self.df['State'].nunique()}")
        print(f"Cities: {self.df['City'].nunique()}")
        print(f"\nSeverity distribution:")
        print(self.df['Severity'].value_counts().sort_index())
        print("="*60)
