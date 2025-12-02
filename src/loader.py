"""
Data loading module
Load raw data and filter by time period
"""

import pandas as pd
import config

class DataLoader:
    """Load and filter accident data"""
    
    def __init__(self):
        self.raw_path = config.RAW_DATA_PATH #Đường dẫn tới US_Accidents_March23.csv
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
        
        print(f"Loaded {len(self.df):,} record(s)")
        return self.df
    
    def filter_by_year(self): #xóa start_year=None, end_year=None
        """Filter data by year range"""
        start_year = config.START_YEAR  #xóa start_year or
        end_year = config.END_YEAR     #xóa end_year or 
        
        print(f"Filtering data: {start_year}-{end_year}")
        self.df['Start_Time'] = pd.to_datetime(self.df['Start_Time'], errors='coerce') 
        self.df['End_Time'] = pd.to_datetime(self.df['End_Time'], errors='coerce')        
        self.df['Year'] = self.df['Start_Time'].dt.year
        initial_count = len(self.df)
        
        self.df = self.df[
            (self.df['Year'] >= start_year) & 
            (self.df['Year'] <= end_year)
        ].copy()
        
        print(f"Filtered to {len(self.df):,} record(s) ({len(self.df)/initial_count*100:.2f}%)")
        return self.df
    
    def get_summary(self):
        """Print basic summary statistics"""
        min_date = self.df['Start_Time'].min()
        max_date = self.df['Start_Time'].max()
        count_states = self.df['State'].nunique()
        count_cities = self.df['City'].nunique()
        print("\n" + "="*60)
        print("Data Summary")
        print("="*60)
        print(f"Total records: {len(self.df):,}")
        print(f"Date range: {min_date} to {max_date}")
        print(f"States: {count_states}")
        print(f"Cities: {count_cities}")
        print(f"\nSeverity distribution:")
        print(self.df['Severity'].value_counts().sort_index())
        print("="*60)