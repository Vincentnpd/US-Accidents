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
import config


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
        
        # Save
        output_path = config.DIM_DIR / "dim_time.csv"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        dim_time.to_csv(output_path, index=False)
        
        print(f"  dim_time created: {len(dim_time):,} records")
        print(f"  PK: Date")
        print(f"  Date range: {dim_time['Date'].min()} to {dim_time['Date'].max()}")
        
        return dim_time
    
    def create_dim_location(self):
        """
        Create location dimension table with aggregated boolean counts.
        
        PK: Location_id (Street + "_" + City)
        """
        print("\nCreating dim_location")
        
        # Create Location_id
        self.df['Location_id'] = (
            self.df['Street'].astype(str) + "_" + 
            self.df['City'].astype(str)
        )
        
        location_cols = ['Street', 'City', 'County', 'State', 'Timezone']
        bool_cols = ['Amenity', 'Crossing', 'Junction', 'Stop', 'Traffic_Signal']
        
        # Aggregate: count True values per location
        agg_dict = {col: lambda x: (x == True).sum() for col in bool_cols}
        
        dim_location = self.df.groupby(
            ['Location_id'] + location_cols, 
            dropna=False
        ).agg(agg_dict).reset_index()
        
        # Sort
        dim_location = dim_location.sort_values(
            ['State', 'City']
        ).reset_index(drop=True)
        
        # Save
        output_path = config.DIM_DIR / "dim_location.csv"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        dim_location.to_csv(output_path, index=False)
        
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
        
        # Save
        output_path = config.DIM_DIR / "dim_weather.csv"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        dim_weather.to_csv(output_path, index=False)
        
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
        
        accident_detail = fact[fact_cols]
        
        # Save
        output_path = config.FACT_DIR / "accident_detail.csv"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        accident_detail.to_csv(output_path, index=False)
        
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