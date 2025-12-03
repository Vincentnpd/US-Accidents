"""
Data Modeling Module - Star Schema Creation
============================================
Creates star schema from cleaned data:
  - dim_time: Date (PK), Day, Month, Quarter, Year
  - dim_location: Location_id (PK), Street, City, County, State, Timezone, infrastructure counts
  - dim_weather: weather_id (PK), Weather_Condition
  - accident_detail: ID, Location_id (FK), weather_id (FK), full_date (FK), 
                     Start_Time, End_Time, Severity, Duration_min, Description
"""

import pandas as pd
import os
import time
import config


def safe_to_csv(df, output_path, max_retries=3, retry_delay=2):
    """
    Safely save DataFrame to CSV with retry logic for permission errors.
    
    Args:
        df: DataFrame to save
        output_path: Path to save file
        max_retries: Maximum number of retry attempts
        retry_delay: Seconds to wait between retries
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    for attempt in range(max_retries):
        try:
            # Try to remove existing file first
            if output_path.exists():
                try:
                    os.remove(output_path)
                except PermissionError:
                    pass  # Will be caught in main try
            
            df.to_csv(output_path, index=False)
            return True
            
        except PermissionError as e:
            if attempt < max_retries - 1:
                print(f"  Warning: File is locked. Retrying in {retry_delay}s... ({attempt + 1}/{max_retries})")
                print(f"  Please close the file if it's open in Excel or another program.")
                time.sleep(retry_delay)
            else:
                print(f"\n  ERROR: Cannot write to {output_path}")
                print(f"  The file is being used by another process.")
                print(f"  Please:")
                print(f"    1. Close Excel or any program using this file")
                print(f"    2. Close any File Explorer windows showing this folder")
                print(f"    3. Run the script again")
                raise e
    
    return False


class DataSplitter:
    """Create star schema from cleaned data"""
    
    def __init__(self, df):
        self.df = df.copy()
        
    def create_dim_time(self):
        """
        Create time dimension table.
        
        PK: Date (date only, no time component)
        Columns: Date, Day, Month, Quarter, Year
        """
        print("\nCreating dim_time")
        
        # Parse datetime
        self.df['Start_Time'] = pd.to_datetime(self.df['Start_Time'])
        
        # Extract date components
        self.df['Date'] = self.df['Start_Time'].dt.date
        self.df['Day'] = self.df['Start_Time'].dt.day
        self.df['Month'] = self.df['Start_Time'].dt.month
        self.df['Quarter'] = self.df['Start_Time'].dt.quarter
        self.df['Year'] = self.df['Start_Time'].dt.year
        
        # Create dimension (Date as PK - no Start_Time)
        time_cols = ['Date', 'Day', 'Month', 'Quarter', 'Year']
        dim_time = self.df[time_cols].drop_duplicates().reset_index(drop=True)
        
        # Sort by date
        dim_time = dim_time.sort_values('Date').reset_index(drop=True)
        
        # Save with retry logic
        output_path = config.DIM_DIR / "dim_time.csv"
        safe_to_csv(dim_time, output_path)
        
        print(f"  dim_time created: {len(dim_time):,} records")
        print(f"  PK: Date")
        print(f"  Date range: {dim_time['Date'].min()} to {dim_time['Date'].max()}")
        
        return dim_time
    
    def create_dim_location(self):
        """
        Create location dimension table with aggregated boolean counts.
        
        PK: Location_id (Street + "_" + City) - MUST BE UNIQUE
        """
        print("\nCreating dim_location")
        
        # Create Location_id
        self.df['Location_id'] = (
            self.df['Street'].astype(str) + "_" + 
            self.df['City'].astype(str)
        )
        
        location_cols = ['Street', 'City', 'County', 'State', 'Timezone']
        bool_cols = ['Amenity', 'Crossing', 'Junction', 'Stop', 'Traffic_Signal']
        
        # Check which columns exist
        available_location = [c for c in location_cols if c in self.df.columns]
        available_bool = [c for c in bool_cols if c in self.df.columns]
        
        # Build aggregation dict:
        # - Location columns: take first value
        # - Boolean columns: sum of True values
        agg_dict = {}
        for col in available_location:
            agg_dict[col] = 'first'
        for col in available_bool:
            agg_dict[col] = lambda x: (x == True).sum()
        
        # Groupby ONLY Location_id to ensure uniqueness
        dim_location = self.df.groupby('Location_id', dropna=False).agg(agg_dict).reset_index()
        
        # Sort
        dim_location = dim_location.sort_values(
            ['State', 'City']
        ).reset_index(drop=True)
        
        # Validate uniqueness
        n_unique = dim_location['Location_id'].nunique()
        n_total = len(dim_location)
        if n_unique != n_total:
            print(f"  WARNING: Location_id not unique! {n_unique} unique vs {n_total} total")
        else:
            print(f"  Location_id uniqueness: VERIFIED")
        
        # Save with retry logic
        output_path = config.DIM_DIR / "dim_location.csv"
        safe_to_csv(dim_location, output_path)
        
        print(f"  dim_location created: {len(dim_location):,} records")
        print(f"  PK: Location_id")
        print(f"  States: {dim_location['State'].nunique()}")
        
        return dim_location
    
    def create_dim_weather(self):
        """
        Create weather dimension table.
        
        PK: weather_id (W1, W2, ...)
        """
        print("\nCreating dim_weather")
        
        dim_weather = self.df[['Weather_Condition']].drop_duplicates().reset_index(drop=True)
        dim_weather['weather_id'] = ["W" + str(i+1) for i in range(len(dim_weather))]
        
        # Reorder columns
        dim_weather = dim_weather[['weather_id', 'Weather_Condition']]
        
        # Save with retry logic
        output_path = config.DIM_DIR / "dim_weather.csv"
        safe_to_csv(dim_weather, output_path)
        
        print(f"  dim_weather created: {len(dim_weather):,} records")
        print(f"  PK: weather_id")
        
        return dim_weather
    
    def create_fact_table(self, dim_weather, dim_location):
        """
        Create accident_detail fact table.
        
        Keys:
          - ID: Primary key
          - Location_id: FK -> dim_location
          - weather_id: FK -> dim_weather  
          - full_date: FK -> dim_time.Date
        
        Measures:
          - Severity, Duration_min
        
        Attributes:
          - Start_Time, End_Time (full timestamp), Description
        """
        print("\nCreating accident_detail")
        
        fact = self.df.copy()
        
        # Map weather_id from dim_weather
        fact = fact.merge(dim_weather, on='Weather_Condition', how='left')
        
        # Create Location_id (if not exists)
        if 'Location_id' not in fact.columns:
            fact['Location_id'] = (
                fact['Street'].astype(str) + "_" + 
                fact['City'].astype(str)
            )
        
        # Create full_date as FK to dim_time.Date
        fact['Start_Time'] = pd.to_datetime(fact['Start_Time'])
        fact['End_Time'] = pd.to_datetime(fact['End_Time'])
        fact['full_date'] = fact['Start_Time'].dt.date
        
        # Calculate Duration_min
        fact['Duration_min'] = (
            (fact['End_Time'] - fact['Start_Time']).dt.total_seconds() / 60
        )
        
        # Select fact columns
        fact_cols = [
            'ID',               # PK
            'Location_id',      # FK -> dim_location
            'weather_id',       # FK -> dim_weather
            'full_date',        # FK -> dim_time.Date
            'Start_Time',       # Full timestamp
            'End_Time',         # Full timestamp
            'Severity',         # Measure
            'Duration_min',     # Measure
            'Description'       # Attribute
        ]
        
        # Only keep columns that exist
        available_cols = [c for c in fact_cols if c in fact.columns]
        accident_detail = fact[available_cols]
        
        # Save with retry logic
        output_path = config.FACT_DIR / "accident_detail.csv"
        safe_to_csv(accident_detail, output_path)
        
        print(f"  accident_detail created: {len(accident_detail):,} records")
        print(f"  PK: ID")
        print(f"  FKs: Location_id, weather_id, full_date")
        
        return accident_detail
    
    def validate_schema(self, dim_time, dim_location, dim_weather, fact):
        """Validate referential integrity"""
        print("\nValidating schema")
        
        # Convert dim_time.Date to same type as fact.full_date
        dim_dates = set(pd.to_datetime(dim_time['Date']).dt.date)
        fact_dates = set(pd.to_datetime(fact['full_date']).dt.date)
        
        missing_date = len(fact_dates - dim_dates)
        missing_location = fact[~fact['Location_id'].isin(dim_location['Location_id'])].shape[0]
        missing_weather = fact[~fact['weather_id'].isin(dim_weather['weather_id'])].shape[0]
        
        print(f"  Missing full_date in dim_time: {missing_date}")
        print(f"  Missing Location_id in dim_location: {missing_location}")
        print(f"  Missing weather_id in dim_weather: {missing_weather}")
        
        if missing_date == 0 and missing_location == 0 and missing_weather == 0:
            print("  Schema validation: PASSED")
        else:
            print("  Schema validation: WARNING - orphan records found")
    
    def run_all(self):
        """Create complete star schema"""
        print("\n" + "="*60)
        print("Creating Star Schema")
        print("="*60)
        
        dim_time = self.create_dim_time()
        dim_location = self.create_dim_location()
        dim_weather = self.create_dim_weather()
        fact = self.create_fact_table(dim_weather, dim_location)
        
        self.validate_schema(dim_time, dim_location, dim_weather, fact)
        
        print("\n" + "-"*60)
        print("Schema Summary")
        print("-"*60)
        print(f"  dim_time:        {len(dim_time):>10,} records  (PK: Date)")
        print(f"  dim_location:    {len(dim_location):>10,} records  (PK: Location_id)")
        print(f"  dim_weather:     {len(dim_weather):>10,} records  (PK: weather_id)")
        print(f"  accident_detail: {len(fact):>10,} records  (PK: ID)")
        print("\nStar schema created successfully")
        
        return dim_time, dim_location, dim_weather, fact