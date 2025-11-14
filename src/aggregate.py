"""
Aggregation module
Create aggregate tables for Tableau dashboards
"""

import pandas as pd
import config


class Aggregator:
    """Generate aggregate tables for analysis"""
    
    def __init__(self):
        self.fact = None
        self.dim_time = None
        self.dim_location = None
        self.dim_weather = None
        self.df = None
        
    def load_tables(self):
        """Load fact and dimension tables"""
        print("\nLoading tables")
        
        self.fact = pd.read_csv(config.FACT_DIR / "fact_accident.csv")
        self.dim_time = pd.read_csv(config.DIM_DIR / "dim_time.csv", 
                                     parse_dates=['Start_Time'])
        self.dim_location = pd.read_csv(config.DIM_DIR / "dim_location.csv")
        self.dim_weather = pd.read_csv(config.DIM_DIR / "dim_weather.csv")
        
        # Merge all
        self.df = self.fact.merge(self.dim_time, on='Time_ID', how='left')
        self.df = self.df.merge(self.dim_location, on='Location_ID', how='left')
        self.df = self.df.merge(self.dim_weather, on='Weather_ID', how='left')
        
        print(f"Merged dataset: {len(self.df):,} records")
    
    def agg_state_year(self):
        """State-level yearly aggregates"""
        print("\nCreating agg_state_year")
        
        agg = self.df.groupby(['State', 'Year']).agg({
            'ID': 'count',
            'Severity': ['mean', 'std'],
            'Duration_min': 'mean'
        }).reset_index()
        
        agg.columns = ['State', 'Year', 'Total_Accidents', 
                       'Avg_Severity', 'Std_Severity', 'Avg_Duration']
        
        # YoY change
        agg = agg.sort_values(['State', 'Year'])
        agg['YoY_Change'] = agg.groupby('State')['Total_Accidents'].pct_change() * 100
        
        output_path = config.AGG_DIR / "agg_state_year.csv"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        agg.to_csv(output_path, index=False)
        
        print(f"Saved: {len(agg)} records")
        return agg
    
    def agg_city_severity(self):
        """City-level severity aggregates"""
        print("\nCreating agg_city_severity")
        
        agg = self.df.groupby(['State', 'City']).agg({
            'ID': 'count',
            'Severity': ['mean', lambda x: (x >= 3).sum()]
        }).reset_index()
        
        agg.columns = ['State', 'City', 'Total_Accidents', 
                       'Avg_Severity', 'High_Severity_Count']
        
        # Calculate percentage of state total
        state_totals = agg.groupby('State')['Total_Accidents'].sum()
        agg['Pct_of_State'] = agg.apply(
            lambda x: (x['Total_Accidents'] / state_totals[x['State']]) * 100,
            axis=1
        )
        
        # Pareto analysis
        agg = agg.sort_values(['State', 'Total_Accidents'], ascending=[True, False])
        agg['Cumulative_Pct'] = agg.groupby('State')['Pct_of_State'].cumsum()
        agg['Is_Top_80'] = (agg['Cumulative_Pct'] <= 80).astype(int)
        
        output_path = config.AGG_DIR / "agg_city_severity.csv"
        agg.to_csv(output_path, index=False)
        
        print(f"Saved: {len(agg)} records")
        return agg
    
    def agg_time_pattern(self):
        """Time pattern aggregates"""
        print("\nCreating agg_time_pattern")
        
        agg = self.df.groupby(['Hour', 'DayOfWeek', 'DayName']).agg({
            'ID': 'count',
            'Severity': 'mean'
        }).reset_index()
        
        agg.columns = ['Hour', 'DayOfWeek', 'DayName', 
                       'Accident_Count', 'Avg_Severity']
        
        agg['Is_Rush_Hour'] = agg['Hour'].apply(
            lambda x: 1 if x in [7, 8, 17, 18] else 0
        )
        agg['Is_Weekend'] = (agg['DayOfWeek'] >= 5).astype(int)
        
        output_path = config.AGG_DIR / "agg_time_pattern.csv"
        agg.to_csv(output_path, index=False)
        
        print(f"Saved: {len(agg)} records")
        return agg
    
    def agg_weather_impact(self):
        """Weather impact aggregates"""
        print("\nCreating agg_weather_impact")
        
        # Group by weather conditions
        weather_cols = ['Is_Rain', 'Is_Snow', 'Is_Fog', 'Low_Visibility']
        available_weather = [col for col in weather_cols if col in self.df.columns]
        
        if len(available_weather) == 0:
            print("No weather columns available")
            return None
        
        group_cols = ['State'] + available_weather
        
        agg = self.df.groupby(group_cols).agg({
            'ID': 'count',
            'Severity': 'mean',
            'Temperature(F)': 'mean',
            'Visibility(mi)': 'mean'
        }).reset_index()
        
        agg.columns = group_cols + ['Accident_Count', 'Avg_Severity',
                                     'Avg_Temperature', 'Avg_Visibility']
        
        output_path = config.AGG_DIR / "agg_weather_impact.csv"
        agg.to_csv(output_path, index=False)
        
        print(f"Saved: {len(agg)} records")
        return agg
    
    def agg_infrastructure(self):
        """Infrastructure impact aggregates"""
        print("\nCreating agg_infrastructure")
        
        infra_cols = ['Traffic_Signal', 'Junction', 'Crossing']
        available_infra = [col for col in infra_cols if col in self.df.columns]
        
        if len(available_infra) == 0:
            print("No infrastructure columns available")
            return None
        
        group_cols = ['State'] + available_infra
        
        agg = self.df.groupby(group_cols).agg({
            'ID': 'count',
            'Severity': 'mean',
            'Infra_Score': 'mean'
        }).reset_index()
        
        col_names = group_cols + ['Accident_Count', 'Avg_Severity', 'Avg_Infra_Score']
        agg.columns = col_names
        
        output_path = config.AGG_DIR / "agg_infrastructure.csv"
        agg.to_csv(output_path, index=False)
        
        print(f"Saved: {len(agg)} records")
        return agg
    
    def run_all(self):
        """Generate all aggregate tables"""
        print("\n" + "="*60)
        print("Generating Aggregate Tables")
        print("="*60)
        
        self.load_tables()
        
        self.agg_state_year()
        self.agg_city_severity()
        self.agg_time_pattern()
        self.agg_weather_impact()
        self.agg_infrastructure()
        
        print("\nAll aggregates created successfully")
