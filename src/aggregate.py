"""
Aggregation Module - Dashboard-Aligned Aggregate Tables
========================================================
Compatible with updated star schema:
  - dim_time: Date (PK), Day, Month, Quarter, Year
  - dim_location: Location_id (PK), Street, City, County, State, Timezone, infrastructure counts
  - dim_weather: weather_id (PK), Weather_Condition
  - accident_detail: ID, Location_id (FK), weather_id (FK), full_date (FK->Date),
                     Start_Time, End_Time, Severity, Duration_min, Description

Creates 4 dashboard aggregates + 2 supplementary:
  1. agg_federal.csv       -> Dashboard 1: National Overview
  2. agg_state_anomaly.csv -> Dashboard 2: State Anomaly Detection
  3. agg_infrastructure.csv -> Dashboard 3: Infrastructure Stats
  4. agg_weather_deep.csv  -> Dashboard 4: Weather Deep Dive
  5. agg_city_pareto.csv   -> Supplementary: 80/20 Analysis
  6. agg_time_pattern.csv  -> Supplementary: Time Patterns
"""

import pandas as pd
import numpy as np
from typing import Dict, Optional
import config


class Aggregator:
    """
    Generate dashboard-aligned aggregate tables.
    
    Workflow:
        1. Load fact + dimension tables (from splitter.py)
        2. Merge into denormalized view
        3. Generate 4 stakeholder aggregates
        4. Validate outputs
        5. Export to CSV for Tableau
    """
    
    def __init__(self):
        self.fact: Optional[pd.DataFrame] = None
        self.dim_time: Optional[pd.DataFrame] = None
        self.dim_location: Optional[pd.DataFrame] = None
        self.dim_weather: Optional[pd.DataFrame] = None
        self.df: Optional[pd.DataFrame] = None
        self.national_avg_severity: float = 0.0
        self.validation_results: Dict[str, bool] = {}
        
    # =========================================================================
    # DATA LOADING
    # =========================================================================
    
    def load_tables(self) -> pd.DataFrame:
        """
        Load and merge fact + dimension tables.
        
        Join keys:
          - fact.full_date -> dim_time.Date
          - fact.Location_id -> dim_location.Location_id
          - fact.weather_id -> dim_weather.weather_id
        """
        print("\nLoading tables from star schema...")
        
        # Load tables
        self.fact = pd.read_csv(
            config.FACT_DIR / "accident_detail.csv",
            parse_dates=['Start_Time', 'End_Time', 'full_date']
        )
        self.dim_time = pd.read_csv(
            config.DIM_DIR / "dim_time.csv",
            parse_dates=['Date']
        )
        self.dim_location = pd.read_csv(config.DIM_DIR / "dim_location.csv")
        self.dim_weather = pd.read_csv(config.DIM_DIR / "dim_weather.csv")
        
        print(f"  accident_detail: {len(self.fact):>10,} records")
        print(f"  dim_time:        {len(self.dim_time):>10,} records")
        print(f"  dim_location:    {len(self.dim_location):>10,} records")
        print(f"  dim_weather:     {len(self.dim_weather):>10,} records")
        
        # Normalize date columns for join
        self.fact['full_date'] = pd.to_datetime(self.fact['full_date']).dt.date
        self.dim_time['Date'] = pd.to_datetime(self.dim_time['Date']).dt.date
        
        # Merge: fact -> dim_time (on full_date = Date)
        self.df = self.fact.merge(
            self.dim_time,
            left_on='full_date',
            right_on='Date',
            how='left'
        )
        
        # Merge: -> dim_location
        self.df = self.df.merge(
            self.dim_location,
            on='Location_id',
            how='left'
        )
        
        # Merge: -> dim_weather
        self.df = self.df.merge(
            self.dim_weather,
            on='weather_id',
            how='left'
        )
        
        # Extract time components from Start_Time (for hour-level analysis)
        self.df['Start_Time'] = pd.to_datetime(self.df['Start_Time'])
        self.df['Hour'] = self.df['Start_Time'].dt.hour
        self.df['DayOfWeek'] = self.df['Start_Time'].dt.dayofweek
        self.df['DayName'] = self.df['Start_Time'].dt.day_name()
        
        # Calculate Infra_Score from infrastructure counts
        infra_cols = ['Amenity', 'Crossing', 'Junction', 'Stop', 'Traffic_Signal']
        available_infra = [c for c in infra_cols if c in self.df.columns]
        self.df['Infra_Score'] = self.df[available_infra].sum(axis=1)
        
        # National baseline
        self.national_avg_severity = self.df['Severity'].mean()
        
        print(f"\n  Merged dataset: {len(self.df):,} records")
        print(f"  National avg severity: {self.national_avg_severity:.3f}")
        
        return self.df
    
    # =========================================================================
    # AGGREGATE 1: FEDERAL OVERVIEW (Dashboard 1)
    # =========================================================================
    
    def agg_federal(self) -> pd.DataFrame:
        """
        Dashboard 1: National Overview for Federal Government.
        
        Purpose: Understand overall trends 2019-2022, YoY changes
        Grain: Year level
        Stakeholder: Federal Government
        
        Metrics:
            - total_accidents: COUNT(ID)
            - total_high_severity: SUM(IF Severity >= 3)
            - avg_severity: AVG(Severity)
            - yoy_change_pct: (Current - Previous) / Previous * 100
        """
        print("\nCreating agg_federal (Dashboard 1: National Overview)")
        
        agg = self.df.groupby('Year').agg(
            total_accidents=('ID', 'count'),
            total_high_severity=('Severity', lambda x: (x >= 3).sum()),
            avg_severity=('Severity', 'mean'),
            std_severity=('Severity', 'std'),
            avg_duration_min=('Duration_min', 'mean'),
            median_duration_min=('Duration_min', 'median'),
            total_states=('State', 'nunique'),
            total_cities=('City', 'nunique')
        ).reset_index()
        
        # YoY change
        agg = agg.sort_values('Year')
        agg['yoy_accidents_change'] = agg['total_accidents'].pct_change() * 100
        agg['yoy_severity_change'] = agg['avg_severity'].pct_change() * 100
        
        # High severity percentage
        agg['high_severity_pct'] = (
            agg['total_high_severity'] / agg['total_accidents'] * 100
        )
        
        # Cumulative total
        agg['cumulative_accidents'] = agg['total_accidents'].cumsum()
        
        # Formula references for Tableau
        agg['formula_yoy'] = '(ZN(SUM([Accidents])) - LOOKUP(ZN(SUM([Accidents])), -1)) / ABS(LOOKUP(ZN(SUM([Accidents])), -1)) * 100'
        agg['formula_high_sev'] = 'SUM(IF [Severity] >= 3 THEN 1 ELSE 0 END)'
        
        # Validation
        agg['is_valid'] = (
            (agg['total_accidents'] > 0) & 
            (agg['avg_severity'].between(1, 4))
        ).astype(int)
        
        # Round
        float_cols = ['avg_severity', 'std_severity', 'avg_duration_min', 
                      'median_duration_min', 'yoy_accidents_change', 
                      'yoy_severity_change', 'high_severity_pct']
        agg[float_cols] = agg[float_cols].round(3)
        
        # Save
        output_path = config.AGG_DIR / "agg_federal.csv"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        agg.to_csv(output_path, index=False)
        
        print(f"  Saved: {len(agg)} records -> {output_path.name}")
        self.validation_results['agg_federal'] = bool(agg['is_valid'].all())
        
        return agg
    
    # =========================================================================
    # AGGREGATE 2: STATE ANOMALY (Dashboard 2)
    # =========================================================================
    
    def agg_state_anomaly(self) -> pd.DataFrame:
        """
        Dashboard 2: State Anomaly Detection for State/Local Government.
        
        Purpose: Identify states with severity > national average
        Grain: State + Year level
        Stakeholder: State/Local Government
        
        Metrics:
            - is_anomaly: 1 if avg_severity > national_avg
            - severity_zscore: (state_avg - national_avg) / national_std
            - anomaly_category: Critical/High/Elevated/Normal
        """
        print("\nCreating agg_state_anomaly (Dashboard 2: State Outliers)")
        
        agg = self.df.groupby(['State', 'Year']).agg(
            total_accidents=('ID', 'count'),
            total_high_severity=('Severity', lambda x: (x >= 3).sum()),
            avg_severity=('Severity', 'mean'),
            std_severity=('Severity', 'std'),
            avg_duration_min=('Duration_min', 'mean'),
            total_cities=('City', 'nunique'),
            total_counties=('County', 'nunique')
        ).reset_index()
        
        # National stats per year
        national_stats = self.df.groupby('Year').agg(
            national_avg_severity=('Severity', 'mean'),
            national_std_severity=('Severity', 'std'),
            national_total=('ID', 'count')
        ).reset_index()
        
        agg = agg.merge(national_stats, on='Year', how='left')
        
        # Anomaly detection
        agg['is_anomaly'] = (
            agg['avg_severity'] > agg['national_avg_severity']
        ).astype(int)
        
        # Z-score
        agg['severity_zscore'] = (
            (agg['avg_severity'] - agg['national_avg_severity']) / 
            agg['national_std_severity'].replace(0, np.nan)
        )
        
        # Percentage of national
        agg['pct_of_national'] = agg['total_accidents'] / agg['national_total'] * 100
        
        # YoY change per state
        agg = agg.sort_values(['State', 'Year'])
        agg['yoy_change'] = agg.groupby('State')['total_accidents'].pct_change() * 100
        
        # Anomaly category
        def categorize(z):
            if pd.isna(z): return 'Normal'
            if z > 2: return 'Critical'
            if z > 1: return 'High'
            if z > 0: return 'Elevated'
            return 'Normal'
        
        agg['anomaly_category'] = agg['severity_zscore'].apply(categorize)
        
        # Formulas
        agg['formula_anomaly'] = 'IF [Avg Severity] > {FIXED : AVG([Severity])} THEN 1 ELSE 0 END'
        agg['formula_zscore'] = '([State Avg] - [National Avg]) / [National Std]'
        
        # Validation
        agg['is_valid'] = (
            (agg['total_accidents'] > 0) & 
            (agg['avg_severity'].between(1, 4))
        ).astype(int)
        
        # Round
        float_cols = ['avg_severity', 'std_severity', 'avg_duration_min',
                      'national_avg_severity', 'national_std_severity',
                      'severity_zscore', 'pct_of_national', 'yoy_change']
        agg[float_cols] = agg[float_cols].round(3)
        
        # Save
        output_path = config.AGG_DIR / "agg_state_anomaly.csv"
        agg.to_csv(output_path, index=False)
        
        anomaly_pct = agg['is_anomaly'].mean() * 100
        print(f"  Saved: {len(agg)} records -> {output_path.name}")
        print(f"  Anomaly rate: {anomaly_pct:.1f}%")
        
        self.validation_results['agg_state_anomaly'] = bool(agg['is_valid'].all())
        return agg
    
    # =========================================================================
    # AGGREGATE 3: INFRASTRUCTURE (Dashboard 3)
    # =========================================================================
    
    def agg_infrastructure(self) -> pd.DataFrame:
        """
        Dashboard 3: Infrastructure Stats for Law Enforcement.
        
        Purpose: Identify high-risk infrastructure, guide patrol deployment
        Grain: State + Urban/Rural + Infrastructure profile
        Stakeholder: Law Enforcement / Cảnh sát
        
        Metrics:
            - infra_risk_score: Junction*3 + Traffic_Signal*2 + Crossing*2 + Stop*1 + Amenity*1
            - risk_category: Critical/High/Medium/Low
        """
        print("\nCreating agg_infrastructure (Dashboard 3: Infrastructure Stats)")
        
        # Create Urban/Rural proxy based on Infra_Score
        median_infra = self.df['Infra_Score'].median()
        self.df['Urban_Rural'] = np.where(
            self.df['Infra_Score'] >= median_infra, 'Urban', 'Rural'
        )
        
        # Group by State + Urban_Rural
        agg = self.df.groupby(['State', 'Urban_Rural']).agg(
            total_accidents=('ID', 'count'),
            total_high_severity=('Severity', lambda x: (x >= 3).sum()),
            avg_severity=('Severity', 'mean'),
            std_severity=('Severity', 'std'),
            avg_duration_min=('Duration_min', 'mean'),
            avg_infra_score=('Infra_Score', 'mean'),
            total_junction=('Junction', 'sum'),
            total_traffic_signal=('Traffic_Signal', 'sum'),
            total_crossing=('Crossing', 'sum'),
            total_stop=('Stop', 'sum'),
            total_amenity=('Amenity', 'sum')
        ).reset_index()
        
        # Weighted risk score (normalized by accident count)
        agg['infra_risk_score'] = (
            agg['total_junction'] * 3 +
            agg['total_traffic_signal'] * 2 +
            agg['total_crossing'] * 2 +
            agg['total_stop'] * 1 +
            agg['total_amenity'] * 1
        ) / agg['total_accidents']
        
        # High risk flag
        median_risk = agg['infra_risk_score'].median()
        agg['is_high_risk'] = (agg['infra_risk_score'] > median_risk).astype(int)
        
        # Risk category
        def risk_cat(row):
            score = row['infra_risk_score']
            sev = row['avg_severity']
            if score > 5 and sev >= 3: return 'Critical'
            if score > 3 or sev >= 3: return 'High'
            if score > 1: return 'Medium'
            return 'Low'
        
        agg['risk_category'] = agg.apply(risk_cat, axis=1)
        
        # Pct within urban/rural
        type_totals = agg.groupby('Urban_Rural')['total_accidents'].transform('sum')
        agg['pct_within_type'] = (agg['total_accidents'] / type_totals * 100)
        
        # High severity rate
        agg['high_severity_rate'] = agg['total_high_severity'] / agg['total_accidents'] * 100
        
        # Formulas
        agg['formula_risk'] = 'Junction*3 + Traffic_Signal*2 + Crossing*2 + Stop*1 + Amenity*1'
        agg['formula_high_risk'] = 'IF [Risk Score] > MEDIAN([Risk Score]) THEN 1 ELSE 0 END'
        
        # Validation
        agg['is_valid'] = (
            (agg['total_accidents'] > 0) & 
            (agg['avg_severity'].between(1, 4))
        ).astype(int)
        
        # Round
        float_cols = ['avg_severity', 'std_severity', 'avg_duration_min',
                      'avg_infra_score', 'infra_risk_score', 'pct_within_type',
                      'high_severity_rate']
        agg[float_cols] = agg[float_cols].round(3)
        
        # Save
        output_path = config.AGG_DIR / "agg_infrastructure.csv"
        agg.to_csv(output_path, index=False)
        
        high_risk_pct = agg['is_high_risk'].mean() * 100
        print(f"  Saved: {len(agg)} records -> {output_path.name}")
        print(f"  High risk locations: {high_risk_pct:.1f}%")
        
        self.validation_results['agg_infrastructure'] = bool(agg['is_valid'].all())
        return agg
    
    # =========================================================================
    # AGGREGATE 4: WEATHER DEEP DIVE (Dashboard 4)
    # =========================================================================
    
    def agg_weather_deep(self) -> pd.DataFrame:
        """
        Dashboard 4: Weather Deep Dive for Insurance & Victims.
        
        Purpose: Analyze weather impact, identify correlations
        Grain: Weather_Condition + State + Urban/Rural
        Stakeholder: Insurance Companies / Nạn nhân & Gia đình
        
        Metrics:
            - severity_impact_pct: % increase vs clear weather baseline
            - weather_risk_score: Composite risk factor
        """
        print("\nCreating agg_weather_deep (Dashboard 4: Weather Deep Dive)")
        
        # Ensure Urban_Rural exists
        if 'Urban_Rural' not in self.df.columns:
            median_infra = self.df['Infra_Score'].median()
            self.df['Urban_Rural'] = np.where(
                self.df['Infra_Score'] >= median_infra, 'Urban', 'Rural'
            )
        
        # Create weather category from Weather_Condition
        def categorize_weather(cond):
            if pd.isna(cond): return 'Clear'
            cond = str(cond).lower()
            if 'rain' in cond or 'drizzle' in cond: return 'Rainy'
            if 'snow' in cond or 'ice' in cond or 'sleet' in cond: return 'Snowy'
            if 'fog' in cond or 'mist' in cond or 'haze' in cond: return 'Foggy'
            if 'cloud' in cond or 'overcast' in cond: return 'Cloudy'
            if 'wind' in cond or 'storm' in cond: return 'Windy'
            return 'Clear'
        
        self.df['Weather_Category'] = self.df['Weather_Condition'].apply(categorize_weather)
        
        # Group by Weather_Category + State + Urban_Rural
        agg = self.df.groupby(['Weather_Category', 'State', 'Urban_Rural']).agg(
            total_accidents=('ID', 'count'),
            total_high_severity=('Severity', lambda x: (x >= 3).sum()),
            avg_severity=('Severity', 'mean'),
            std_severity=('Severity', 'std'),
            avg_duration_min=('Duration_min', 'mean'),
            median_duration_min=('Duration_min', 'median'),
            avg_infra_score=('Infra_Score', 'mean')
        ).reset_index()
        
        # Calculate clear weather baseline per state
        clear_baseline = agg[agg['Weather_Category'] == 'Clear'].groupby('State').agg(
            baseline_severity=('avg_severity', 'mean'),
            baseline_duration=('avg_duration_min', 'mean')
        ).reset_index()
        
        agg = agg.merge(clear_baseline, on='State', how='left')
        
        # Fill missing baseline with overall clear average
        overall_clear = agg[agg['Weather_Category'] == 'Clear']['avg_severity'].mean()
        overall_duration = agg[agg['Weather_Category'] == 'Clear']['avg_duration_min'].mean()
        agg['baseline_severity'] = agg['baseline_severity'].fillna(overall_clear)
        agg['baseline_duration'] = agg['baseline_duration'].fillna(overall_duration)
        
        # Impact vs baseline
        agg['severity_impact_pct'] = (
            (agg['avg_severity'] - agg['baseline_severity']) / 
            agg['baseline_severity'].replace(0, np.nan) * 100
        ).fillna(0)
        
        agg['duration_impact_pct'] = (
            (agg['avg_duration_min'] - agg['baseline_duration']) / 
            agg['baseline_duration'].replace(0, np.nan) * 100
        ).fillna(0)
        
        # Weather risk score
        def calc_risk(row):
            risk = 0
            cat = row['Weather_Category']
            if cat in ['Rainy', 'Snowy']: risk += 2
            if cat in ['Foggy']: risk += 3
            if cat in ['Windy']: risk += 1
            if row['avg_severity'] >= 3: risk += 2
            if row['severity_impact_pct'] > 10: risk += 1
            if row['duration_impact_pct'] > 20: risk += 1
            return risk
        
        agg['weather_risk_score'] = agg.apply(calc_risk, axis=1)
        
        # Risk category
        def risk_cat(score):
            if score >= 6: return 'Extreme'
            if score >= 4: return 'High'
            if score >= 2: return 'Moderate'
            return 'Low'
        
        agg['risk_category'] = agg['weather_risk_score'].apply(risk_cat)
        
        # High severity rate
        agg['high_severity_rate'] = agg['total_high_severity'] / agg['total_accidents'] * 100
        
        # Formulas
        agg['formula_impact'] = '([Weather Avg] - [Clear Baseline]) / [Clear Baseline] * 100'
        agg['formula_risk'] = 'Weather_Score + Severity_Score + Impact_Score'
        
        # Validation
        agg['is_valid'] = (
            (agg['total_accidents'] > 0) & 
            (agg['avg_severity'].between(1, 4))
        ).astype(int)
        
        # Round
        float_cols = ['avg_severity', 'std_severity', 'avg_duration_min',
                      'median_duration_min', 'avg_infra_score',
                      'baseline_severity', 'baseline_duration',
                      'severity_impact_pct', 'duration_impact_pct',
                      'high_severity_rate']
        agg[float_cols] = agg[float_cols].round(3)
        
        # Save
        output_path = config.AGG_DIR / "agg_weather_deep.csv"
        agg.to_csv(output_path, index=False)
        
        high_risk = len(agg[agg['risk_category'].isin(['Extreme', 'High'])])
        print(f"  Saved: {len(agg)} records -> {output_path.name}")
        print(f"  High/Extreme risk combos: {high_risk}")
        
        self.validation_results['agg_weather_deep'] = bool(agg['is_valid'].all())
        return agg
    
    # =========================================================================
    # SUPPLEMENTARY: CITY PARETO (80/20 Analysis)
    # =========================================================================
    
    def agg_city_pareto(self) -> pd.DataFrame:
        """
        Supplementary: City-level Pareto (80/20) analysis.
        
        Purpose: Identify top cities contributing to 80% of accidents
        Grain: State + City level
        """
        print("\nCreating agg_city_pareto (Supplementary: 80/20 Analysis)")
        
        agg = self.df.groupby(['State', 'City']).agg(
            total_accidents=('ID', 'count'),
            total_high_severity=('Severity', lambda x: (x >= 3).sum()),
            avg_severity=('Severity', 'mean'),
            avg_duration_min=('Duration_min', 'mean')
        ).reset_index()
        
        # State totals
        state_totals = agg.groupby('State')['total_accidents'].transform('sum')
        agg['pct_of_state'] = agg['total_accidents'] / state_totals * 100
        
        # Sort and cumulative
        agg = agg.sort_values(['State', 'total_accidents'], ascending=[True, False])
        agg['cumulative_pct'] = agg.groupby('State')['pct_of_state'].cumsum()
        
        # Pareto flag
        agg['is_pareto_top'] = (agg['cumulative_pct'] <= 80).astype(int)
        
        # Rank within state
        agg['rank_in_state'] = agg.groupby('State').cumcount() + 1
        
        # Formula
        agg['formula_pareto'] = 'IF RUNNING_SUM([Pct]) <= 80 THEN 1 ELSE 0 END'
        
        # Round
        float_cols = ['avg_severity', 'avg_duration_min', 'pct_of_state', 'cumulative_pct']
        agg[float_cols] = agg[float_cols].round(3)
        
        # Save
        output_path = config.AGG_DIR / "agg_city_pareto.csv"
        agg.to_csv(output_path, index=False)
        
        pareto_cities = agg[agg['is_pareto_top'] == 1]
        print(f"  Saved: {len(agg)} records -> {output_path.name}")
        print(f"  Cities in Pareto top 80%: {len(pareto_cities)} ({len(pareto_cities)/len(agg)*100:.1f}%)")
        
        return agg
    
    # =========================================================================
    # SUPPLEMENTARY: TIME PATTERNS
    # =========================================================================
    
    def agg_time_pattern(self) -> pd.DataFrame:
        """
        Supplementary: Time pattern analysis.
        
        Purpose: Identify peak hours/days for staffing and patrol
        Grain: Hour + DayOfWeek level
        """
        print("\nCreating agg_time_pattern (Supplementary: Time Patterns)")
        
        agg = self.df.groupby(['Hour', 'DayOfWeek', 'DayName']).agg(
            total_accidents=('ID', 'count'),
            avg_severity=('Severity', 'mean'),
            avg_duration_min=('Duration_min', 'mean')
        ).reset_index()
        
        # Rush hour flag (7-9 AM, 4-6 PM)
        agg['is_rush_hour'] = agg['Hour'].apply(
            lambda x: 1 if x in [7, 8, 9, 16, 17, 18] else 0
        )
        
        # Weekend flag
        agg['is_weekend'] = (agg['DayOfWeek'] >= 5).astype(int)
        
        # Time period
        def get_period(hour):
            if 6 <= hour < 9: return 'Morning_Rush'
            if 9 <= hour < 12: return 'Morning'
            if 12 <= hour < 14: return 'Lunch'
            if 14 <= hour < 17: return 'Afternoon'
            if 17 <= hour < 20: return 'Evening_Rush'
            return 'Night'
        
        agg['time_period'] = agg['Hour'].apply(get_period)
        
        # Hotspot score (0-10 scale)
        max_acc = agg['total_accidents'].max()
        agg['hotspot_score'] = (agg['total_accidents'] / max_acc * 10).round(1)
        
        # Round
        float_cols = ['avg_severity', 'avg_duration_min']
        agg[float_cols] = agg[float_cols].round(3)
        
        # Save
        output_path = config.AGG_DIR / "agg_time_pattern.csv"
        agg.to_csv(output_path, index=False)
        
        rush_total = agg[agg['is_rush_hour'] == 1]['total_accidents'].sum()
        total = agg['total_accidents'].sum()
        print(f"  Saved: {len(agg)} records -> {output_path.name}")
        print(f"  Rush hour accidents: {rush_total:,} ({rush_total/total*100:.1f}%)")
        
        return agg
    
    # =========================================================================
    # VALIDATION & SUMMARY
    # =========================================================================
    
    def validate_all(self) -> Dict[str, bool]:
        """Validate all generated aggregates."""
        print("\n" + "-"*50)
        print("Validation Summary")
        print("-"*50)
        
        all_valid = True
        for name, valid in self.validation_results.items():
            status = "PASS" if valid else "FAIL"
            print(f"  {name}: {status}")
            if not valid:
                all_valid = False
        
        print(f"\n  Overall: {'ALL PASSED' if all_valid else 'SOME FAILED'}")
        return self.validation_results
    
    def generate_summary(self) -> pd.DataFrame:
        """Generate summary of all aggregates."""
        summary_data = []
        
        agg_files = [
            ('agg_federal.csv', 'Dashboard 1', 'Federal Government'),
            ('agg_state_anomaly.csv', 'Dashboard 2', 'State/Local Government'),
            ('agg_infrastructure.csv', 'Dashboard 3', 'Law Enforcement'),
            ('agg_weather_deep.csv', 'Dashboard 4', 'Insurance/Victims'),
            ('agg_city_pareto.csv', 'Supplementary', 'City Analysis'),
            ('agg_time_pattern.csv', 'Supplementary', 'Time Analysis')
        ]
        
        for filename, dashboard, stakeholder in agg_files:
            filepath = config.AGG_DIR / filename
            if filepath.exists():
                df = pd.read_csv(filepath)
                summary_data.append({
                    'Aggregate': filename,
                    'Dashboard': dashboard,
                    'Stakeholder': stakeholder,
                    'Records': len(df),
                    'Columns': len(df.columns),
                    'Size_KB': round(filepath.stat().st_size / 1024, 1)
                })
        
        summary = pd.DataFrame(summary_data)
        
        # Save
        output_path = config.AGG_DIR / "_aggregate_summary.csv"
        summary.to_csv(output_path, index=False)
        
        print("\n" + "="*60)
        print("Aggregate Summary")
        print("="*60)
        print(summary.to_string(index=False))
        
        return summary
    
    # =========================================================================
    # MAIN RUNNER
    # =========================================================================
    
    def run_all(self) -> None:
        """
        Execute complete aggregation pipeline.
        
        Steps:
            1. Load and merge tables
            2. Generate 4 main dashboard aggregates
            3. Generate 2 supplementary aggregates
            4. Validate outputs
            5. Generate summary
        """
        print("\n" + "="*60)
        print("AGGREGATE GENERATION PIPELINE")
        print("="*60)
        
        # Load
        self.load_tables()
        
        # Main aggregates (4 dashboards)
        self.agg_federal()
        self.agg_state_anomaly()
        self.agg_infrastructure()
        self.agg_weather_deep()
        
        # Supplementary
        self.agg_city_pareto()
        self.agg_time_pattern()
        
        # Validate
        self.validate_all()
        
        # Summary
        self.generate_summary()
        
        print("\n" + "="*60)
        print("AGGREGATION COMPLETE")
        print("="*60)
