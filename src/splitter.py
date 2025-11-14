"""
Data modeling module
Create star schema with fact and dimension tables
"""

import pandas as pd
import config


class DataSplitter:
    """Create star schema from cleaned data"""
    
    def __init__(self, df):
        self.df = df.copy()
        
    def create_dim_time(self):
        """Create time dimension table"""
        print("\nCreating dim_time")
        
        time_cols = ['Start_Time', 'Year', 'Month', 'Day', 'Hour', 
                     'DayOfWeek', 'DayName', 'Time_Period', 'Is_Weekend']
        
        dim_time = self.df[time_cols].drop_duplicates().reset_index(drop=True)
        dim_time['Time_ID'] = range(1, len(dim_time) + 1)
        
        cols = ['Time_ID'] + time_cols
        dim_time = dim_time[cols].sort_values('Start_Time').reset_index(drop=True)
        
        output_path = config.DIM_DIR / "dim_time.csv"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        dim_time.to_csv(output_path, index=False)
        
        print(f"dim_time created: {len(dim_time):,} records")
        return dim_time
    
    def create_dim_location(self):
        """Create location dimension table"""
        print("\nCreating dim_location")
        
        location_cols = ['Location_ID', 'City', 'State', 'County', 'Street',
                        'Start_Lat', 'Start_Lng', 'End_Lat', 'End_Lng',
                        'Amenity', 'Crossing', 'Junction', 'Railway',
                        'Station', 'Stop', 'Traffic_Signal', 'Infra_Score']
        
        available_cols = [col for col in location_cols if col in self.df.columns]
        
        dim_location = self.df[available_cols].drop_duplicates(
            subset=['Location_ID']
        ).reset_index(drop=True)
        
        dim_location = dim_location.sort_values(['State', 'City']).reset_index(drop=True)
        
        output_path = config.DIM_DIR / "dim_location.csv"
        dim_location.to_csv(output_path, index=False)
        
        print(f"dim_location created: {len(dim_location):,} records")
        return dim_location
    
    def create_dim_weather(self):
        """Create weather dimension table"""
        print("\nCreating dim_weather")
        
        weather_cols = ['Temperature(F)', 'Visibility(mi)', 'Precipitation(in)',
                       'Weather_Condition', 'Is_Rain', 'Is_Snow', 'Is_Fog', 
                       'Low_Visibility']
        
        available_cols = [col for col in weather_cols if col in self.df.columns]
        
        dim_weather = self.df[available_cols + ['ID']].copy()
        dim_weather['Weather_ID'] = range(1, len(dim_weather) + 1)
        
        cols = ['Weather_ID'] + available_cols
        dim_weather = dim_weather[cols]
        
        output_path = config.DIM_DIR / "dim_weather.csv"
        dim_weather.to_csv(output_path, index=False)
        
        print(f"dim_weather created: {len(dim_weather):,} records")
        return dim_weather
    
    def create_fact_table(self, dim_time, dim_weather):
        """Create fact table"""
        print("\nCreating fact_accident")
        
        fact = self.df.copy()
        
        # Merge Time_ID
        fact = fact.merge(
            dim_time[['Start_Time', 'Time_ID']],
            on='Start_Time',
            how='left'
        )
        
        # Add Weather_ID
        fact['Weather_ID'] = dim_weather['Weather_ID'].values
        
        fact_cols = ['ID', 'Time_ID', 'Weather_ID', 'Location_ID',
                    'Severity', 'Duration_min']
        
        available_cols = [col for col in fact_cols if col in fact.columns]
        fact_accident = fact[available_cols]
        
        output_path = config.FACT_DIR / "fact_accident.csv"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        fact_accident.to_csv(output_path, index=False)
        
        print(f"fact_accident created: {len(fact_accident):,} records")
        return fact_accident
    
    def validate_schema(self, dim_time, dim_location, fact):
        """Validate relationships"""
        print("\nValidating schema")
        
        missing_time = fact[~fact['Time_ID'].isin(dim_time['Time_ID'])].shape[0]
        missing_location = fact[~fact['Location_ID'].isin(dim_location['Location_ID'])].shape[0]
        
        if missing_time == 0 and missing_location == 0:
            print("Schema validation: PASSED")
        else:
            print(f"Warning: Missing Time_ID: {missing_time}, Location_ID: {missing_location}")
    
    def run_all(self):
        """Create complete star schema"""
        print("\n" + "="*60)
        print("Creating Star Schema")
        print("="*60)
        
        dim_time = self.create_dim_time()
        dim_location = self.create_dim_location()
        dim_weather = self.create_dim_weather()
        fact = self.create_fact_table(dim_time, dim_weather)
        
        self.validate_schema(dim_time, dim_location, fact)
        
        print("\nStar schema created successfully")
        return dim_time, dim_location, dim_weather, fact
