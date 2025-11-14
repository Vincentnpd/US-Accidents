"""
Data cleaning module
Handle missing values, outliers, and feature engineering
"""

import pandas as pd
import numpy as np
import config


class DataCleaner:
    """Clean data and create features"""
    
    def __init__(self, df):
        self.df = df.copy()
        
    def handle_missing(self):
        """Handle missing values"""
        print("\nHandling missing values")
        
        numeric_cols = ['Temperature(F)', 'Visibility(mi)', 'Precipitation(in)']
        for col in numeric_cols:
            if col in self.df.columns:
                self.df[col].fillna(self.df[col].median(), inplace=True)
        
        bool_cols = ['Amenity', 'Crossing', 'Junction', 'Railway', 
                     'Station', 'Stop', 'Traffic_Signal']
        for col in bool_cols:
            if col in self.df.columns:
                self.df[col].fillna(False, inplace=True)
                
        if 'Weather_Condition' in self.df.columns:
            self.df['Weather_Condition'].fillna('Clear', inplace=True)
            
        print(f"Missing values handled")
        
    def handle_outliers(self):
        """Handle outliers using domain knowledge"""
        print("\nHandling outliers")
        
        # Temperature
        if 'Temperature(F)' in self.df.columns:
            outliers = ((self.df['Temperature(F)'] < config.TEMP_MIN) | 
                       (self.df['Temperature(F)'] > config.TEMP_MAX)).sum()
            self.df.loc[self.df['Temperature(F)'] < config.TEMP_MIN, 'Temperature(F)'] = config.TEMP_MIN
            self.df.loc[self.df['Temperature(F)'] > config.TEMP_MAX, 'Temperature(F)'] = config.TEMP_MAX
            print(f"Temperature outliers capped: {outliers}")
        
        # Visibility
        if 'Visibility(mi)' in self.df.columns:
            outliers = (self.df['Visibility(mi)'] > config.VISIBILITY_MAX).sum()
            self.df.loc[self.df['Visibility(mi)'] > config.VISIBILITY_MAX, 'Visibility(mi)'] = config.VISIBILITY_MAX
            print(f"Visibility outliers capped: {outliers}")
        
        # Duration
        if 'Duration_min' in self.df.columns:
            max_duration = config.DURATION_MAX_HOURS * 60
            outliers = (self.df['Duration_min'] > max_duration).sum()
            self.df.loc[self.df['Duration_min'] > max_duration, 'Duration_min'] = max_duration
            self.df.loc[self.df['Duration_min'] < 0, 'Duration_min'] = 0
            print(f"Duration outliers capped: {outliers}")
        
        # Remove invalid severity
        invalid = ~self.df['Severity'].isin([1, 2, 3, 4])
        removed = invalid.sum()
        if removed > 0:
            self.df = self.df[~invalid]
            print(f"Invalid severity records removed: {removed}")
    
    def create_time_features(self):
        """Create time-based features"""
        print("\nCreating time features")
        
        self.df['Year'] = self.df['Start_Time'].dt.year
        self.df['Month'] = self.df['Start_Time'].dt.month
        self.df['Day'] = self.df['Start_Time'].dt.day
        self.df['Hour'] = self.df['Start_Time'].dt.hour
        self.df['DayOfWeek'] = self.df['Start_Time'].dt.dayofweek
        self.df['DayName'] = self.df['Start_Time'].dt.day_name()
        
        if 'Duration_min' not in self.df.columns:
            self.df['Duration_min'] = (
                self.df['End_Time'] - self.df['Start_Time']
            ).dt.total_seconds() / 60
        
        def get_time_period(hour):
            if 6 <= hour < 9:
                return 'Morning_Rush'
            elif 9 <= hour < 17:
                return 'Day'
            elif 17 <= hour < 20:
                return 'Evening_Rush'
            else:
                return 'Night'
        
        self.df['Time_Period'] = self.df['Hour'].apply(get_time_period)
        self.df['Is_Weekend'] = (self.df['DayOfWeek'] >= 5).astype(int)
        
        print(f"Time features created: 9 columns")
        
    def create_weather_features(self):
        """Create weather-based features"""
        print("\nCreating weather features")
        
        if 'Weather_Condition' in self.df.columns:
            self.df['Is_Rain'] = self.df['Weather_Condition'].str.contains(
                'Rain|Drizzle', case=False, na=False
            ).astype(int)
            
            self.df['Is_Snow'] = self.df['Weather_Condition'].str.contains(
                'Snow|Ice', case=False, na=False
            ).astype(int)
            
            self.df['Is_Fog'] = self.df['Weather_Condition'].str.contains(
                'Fog|Mist', case=False, na=False
            ).astype(int)
        
        if 'Visibility(mi)' in self.df.columns:
            self.df['Low_Visibility'] = (self.df['Visibility(mi)'] < 1).astype(int)
        
        print(f"Weather features created: 4 columns")
    
    def create_infrastructure_features(self):
        """Create infrastructure features"""
        print("\nCreating infrastructure features")
        
        infra_cols = ['Amenity', 'Crossing', 'Junction', 'Railway', 
                      'Station', 'Stop', 'Traffic_Signal']
        
        self.df['Infra_Score'] = 0
        for col in infra_cols:
            if col in self.df.columns:
                self.df['Infra_Score'] += self.df[col].astype(int)
        
        print(f"Infrastructure features created: 1 column")
    
    def create_location_id(self):
        """Create location identifier"""
        print("\nCreating location ID")
        
        self.df['Location_ID'] = (
            self.df['State'] + '_' + 
            self.df['City'].fillna('Unknown') + '_' +
            self.df['Street'].fillna('Unknown')
        )
        
    def save_cleaned_data(self):
        """Save cleaned dataset"""
        output_path = config.CLEANED_DIR / "accidents_cleaned.csv"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        print(f"\nSaving cleaned data to {output_path}")
        self.df.to_csv(output_path, index=False)
        
        file_size = output_path.stat().st_size / (1024**2)
        print(f"Saved: {len(self.df):,} records ({file_size:.1f} MB)")
    
    def run_all(self):
        """Run complete cleaning pipeline"""
        print("\n" + "="*60)
        print("Data Cleaning Pipeline")
        print("="*60)
        
        initial_count = len(self.df)
        
        self.handle_missing()
        self.handle_outliers()
        self.create_time_features()
        self.create_weather_features()
        self.create_infrastructure_features()
        self.create_location_id()
        self.save_cleaned_data()
        
        print(f"\nCleaning complete: {initial_count:,} -> {len(self.df):,} records")
        return self.df
