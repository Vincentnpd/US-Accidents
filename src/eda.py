"""
Exploratory Data Analysis module
Analyze patterns and data quality
"""

import pandas as pd
import numpy as np


class EDA:
    """Perform exploratory data analysis"""
    
    def __init__(self, df):
        self.df = df.copy()
        
    def analyze_temporal(self):
        """Analyze temporal patterns"""
        print("\nTemporal Analysis")
        print("-" * 60)
        
        self.df['Hour'] = self.df['Start_Time'].dt.hour
        self.df['Month'] = self.df['Start_Time'].dt.month
        self.df['DayOfWeek'] = self.df['Start_Time'].dt.dayofweek
        
        print("\nAccidents by year:")
        print(self.df['Year'].value_counts().sort_index())
        
        print("\nPeak hours (top 5):")
        print(self.df['Hour'].value_counts().head())
        
    def analyze_geographic(self):
        """Analyze geographic distribution"""
        print("\nGeographic Analysis")
        print("-" * 60)
        
        print("\nTop 10 states:")
        print(self.df['State'].value_counts().head(10))
        
        print("\nTop 10 cities:")
        city_state = self.df.groupby(['State', 'City']).size().reset_index(name='count')
        print(city_state.nlargest(10, 'count'))
        
    def analyze_severity(self):
        """Analyze severity patterns"""
        print("\nSeverity Analysis")
        print("-" * 60)
        
        severity_stats = self.df.groupby('Severity').agg({
            'ID': 'count',
            'Duration_min': 'mean'
        }).rename(columns={'ID': 'count'})
        
        print(severity_stats)
        
    def check_data_quality(self):
        """Check for missing values and anomalies"""
        print("\nData Quality Check")
        print("-" * 60)
        
        print("\nMissing values:")
        missing = self.df.isnull().sum()
        missing = missing[missing > 0].sort_values(ascending=False)
        if len(missing) > 0:
            print(missing.head(10))
        else:
            print("No missing values")
            
        print(f"\nDuplicate IDs: {self.df.duplicated(subset=['ID']).sum()}")
        
        print("\nNumeric ranges:")
        numeric_cols = ['Temperature(F)', 'Visibility(mi)', 'Precipitation(in)']
        for col in numeric_cols:
            if col in self.df.columns:
                print(f"{col}: min={self.df[col].min():.2f}, max={self.df[col].max():.2f}")
                
    def run_all(self):
        """Run all analysis"""
        print("\n" + "="*60)
        print("Exploratory Data Analysis")
        print("="*60)
        if self.df['Start_Time'].dtype != 'datetime64[ns]':
            self.df['Start_Time'] = pd.to_datetime(self.df['Start_Time'], errors='coerce')
        if self.df['End_Time'].dtype != 'datetime64[ns]':
            self.df['End_Time'] = pd.to_datetime(self.df['End_Time'], errors='coerce')
        
        if 'Duration_min' not in self.df.columns:
            self.df['Duration_min'] = (
                self.df['End_Time'] - self.df['Start_Time']
            ).dt.total_seconds() / 60
        
        self.analyze_temporal()
        self.analyze_geographic()
        self.analyze_severity()
        self.check_data_quality()