"""
Aggregation Module - Dashboard-Aligned Aggregate Tables
========================================================
Creates 4 dashboard aggregates + 1 supplementary:
  1. agg_federal.csv           -> Dashboard 1: National Overview
  2. agg_state_anomaly.csv     -> Dashboard 2: State Anomaly Detection
  3. agg_city_by_state.csv     -> Dashboard 3: City Analysis BY STATE
  4. agg_weather_by_state.csv  -> Dashboard 4: Weather Impact BY STATE
  5. agg_time_pattern.csv      -> Supplementary: Time Patterns
"""

import pandas as pd
import numpy as np
import config


class Aggregator:
    """Generate dashboard-aligned aggregate tables."""
    
    def __init__(self):
        self.fact = None
        self.dim_time = None
        self.dim_location = None
        self.dim_weather = None
        self.df = None
        self.national_avg_severity = 0.0
        
    def load_tables(self) -> pd.DataFrame:
        """Load and merge fact + dimension tables."""
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
        
        # Normalize date columns
        self.fact['full_date'] = pd.to_datetime(self.fact['full_date']).dt.date
        self.dim_time['Date'] = pd.to_datetime(self.dim_time['Date']).dt.date
        
        # Merge fact -> dim_time
        self.df = self.fact.merge(
            self.dim_time,
            left_on='full_date',
            right_on='Date',
            how='left'
        )
        
        # Merge -> dim_location
        self.df = self.df.merge(
            self.dim_location,
            on='Location_id',
            how='left'
        )
        
        # Merge -> dim_weather
        self.df = self.df.merge(
            self.dim_weather,
            on='weather_id',
            how='left'
        )
        
        # Extract time components
        self.df['Start_Time'] = pd.to_datetime(self.df['Start_Time'])
        self.df['Hour'] = self.df['Start_Time'].dt.hour
        self.df['DayOfWeek'] = self.df['Start_Time'].dt.dayofweek
        
        # Create Weather_Category (vectorized)
        self.df['Weather_Category'] = self._categorize_weather_vectorized(
            self.df['Weather_Condition']
        )
        
        # Calculate Infra_Score using config weights
        infra_cols = config.INFRA_COLUMNS
        available_infra = [c for c in infra_cols if c in self.df.columns]
        if available_infra:
            self.df['Infra_Score'] = sum(
                self.df[col].fillna(0) * config.INFRA_WEIGHTS.get(col, 1)
                for col in available_infra
            )
        else:
            self.df['Infra_Score'] = 0
        
        # National baseline
        self.national_avg_severity = self.df['Severity'].mean()
        
        print(f"\n  Merged dataset: {len(self.df):,} records")
        print(f"  National avg severity: {self.national_avg_severity:.3f}")
        
        return self.df
    
    def _categorize_weather_vectorized(self, weather_col: pd.Series) -> pd.Series:
        """Categorize weather using vectorized operations."""
        cond = weather_col.str.lower().fillna('')
        
        # Use config keywords
        conditions = []
        choices = []
        for category, keywords in config.WEATHER_KEYWORDS.items():
            pattern = '|'.join(keywords)
            conditions.append(cond.str.contains(pattern, regex=True))
            choices.append(category)
        
        return pd.Series(
            np.select(conditions, choices, default='Clear'),
            index=weather_col.index
        )
    
    # =========================================================================
    # AGGREGATE 1: FEDERAL OVERVIEW (Dashboard 1)
    # =========================================================================
    
    def agg_federal(self) -> pd.DataFrame:
        """Dashboard 1: National Overview. Grain: Year"""
        print("\n[Aggregate] Creating agg_federal (Dashboard 1)")
        
        agg = self.df.groupby('Year', as_index=False).agg(
            total_accidents=('ID', 'count'),
            total_high_severity=('Severity', lambda x: (x >= config.HIGH_SEVERITY_THRESHOLD).sum()),
            avg_severity=('Severity', 'mean'),
            avg_duration_min=('Duration_min', 'mean'),
            total_states=('State', 'nunique'),
            total_cities=('City', 'nunique')
        )
        
        agg = agg.sort_values('Year')
        agg['yoy_change_pct'] = agg['total_accidents'].pct_change() * 100
        agg['high_severity_pct'] = agg['total_high_severity'] / agg['total_accidents'] * 100
        
        float_cols = ['avg_severity', 'avg_duration_min', 'yoy_change_pct', 'high_severity_pct']
        agg[float_cols] = agg[float_cols].round(2)
        
        output_path = config.AGG_DIR / "agg_federal.csv"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        agg.to_csv(output_path, index=False)
        
        print(f"  Saved: {len(agg)} rows -> {output_path.name}")
        return agg
    
    # =========================================================================
    # AGGREGATE 2: STATE ANOMALY (Dashboard 2)
    # =========================================================================
    
    def agg_state_anomaly(self) -> pd.DataFrame:
        """Dashboard 2: State Anomaly Detection. Grain: State + Year"""
        print("\n[Aggregate] Creating agg_state_anomaly (Dashboard 2)")
        
        agg = self.df.groupby(['State', 'Year'], as_index=False).agg(
            total_accidents=('ID', 'count'),
            total_high_severity=('Severity', lambda x: (x >= config.HIGH_SEVERITY_THRESHOLD).sum()),
            avg_severity=('Severity', 'mean'),
            avg_duration_min=('Duration_min', 'mean'),
            total_cities=('City', 'nunique')
        )
        
        national = self.df.groupby('Year', as_index=False).agg(
            national_avg_severity=('Severity', 'mean'),
            national_std_severity=('Severity', 'std'),
            national_total=('ID', 'count')
        )
        
        agg = agg.merge(national, on='Year', how='left')
        
        agg['severity_zscore'] = (
            (agg['avg_severity'] - agg['national_avg_severity']) / 
            agg['national_std_severity'].replace(0, np.nan)
        ).fillna(0)
        
        agg['pct_of_national'] = (agg['total_accidents'] / agg['national_total'] * 100).round(2)
        
        # Use config thresholds
        agg['anomaly_category'] = np.select(
            [agg['severity_zscore'] > config.ZSCORE_CRITICAL,
             agg['severity_zscore'] > config.ZSCORE_HIGH,
             agg['severity_zscore'] > config.ZSCORE_ELEVATED],
            ['Critical', 'High', 'Elevated'],
            default='Normal'
        )
        
        float_cols = ['avg_severity', 'avg_duration_min', 'national_avg_severity', 
                      'national_std_severity', 'severity_zscore', 'pct_of_national']
        agg[float_cols] = agg[float_cols].round(3)
        
        output_path = config.AGG_DIR / "agg_state_anomaly.csv"
        agg.to_csv(output_path, index=False)
        
        print(f"  Saved: {len(agg)} rows -> {output_path.name}")
        return agg
    
    # =========================================================================
    # AGGREGATE 3: CITY BY STATE (Dashboard 3)
    # =========================================================================
    
    def agg_city_by_state(self) -> pd.DataFrame:
        """
        Dashboard 3: City Analysis BY STATE.
        Grain: State + City
        Filter: State dropdown in Tableau
        """
        print("\n[Aggregate] Creating agg_city_by_state (Dashboard 3)")
        
        # Determine dominant infrastructure
        infra_cols = config.INFRA_COLUMNS
        available_infra = [c for c in infra_cols if c in self.df.columns]
        
        if available_infra:
            infra_df = self.df[available_infra].fillna(0).astype(int)
            self.df['dominant_infra_col'] = infra_df.idxmax(axis=1)
            self.df['has_any_infra'] = infra_df.any(axis=1)
            self.df['dominant_infra'] = np.where(
                self.df['has_any_infra'],
                self.df['dominant_infra_col'],
                'None'
            )
        else:
            self.df['dominant_infra'] = 'None'
        
        agg = self.df.groupby(['State', 'City'], as_index=False).agg(
            total_accidents=('ID', 'count'),
            total_high_severity=('Severity', lambda x: (x >= config.HIGH_SEVERITY_THRESHOLD).sum()),
            avg_severity=('Severity', 'mean'),
            avg_duration_min=('Duration_min', 'mean'),
            dominant_infra=('dominant_infra', lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else 'None')
        )
        
        state_stats = self.df.groupby('State', as_index=False).agg(
            state_total=('ID', 'count'),
            state_avg_severity=('Severity', 'mean'),
            state_avg_duration=('Duration_min', 'mean')
        )
        
        agg = agg.merge(state_stats, on='State', how='left')
        
        agg['pct_of_state'] = (agg['total_accidents'] / agg['state_total'] * 100).round(2)
        agg['high_severity_pct'] = (agg['total_high_severity'] / agg['total_accidents'] * 100).round(2)
        agg['severity_vs_state'] = (
            (agg['avg_severity'] - agg['state_avg_severity']) / 
            agg['state_avg_severity'] * 100
        ).round(2)
        
        agg = agg.sort_values(['State', 'total_accidents'], ascending=[True, False])
        
        float_cols = ['avg_severity', 'avg_duration_min', 'state_avg_severity', 'state_avg_duration']
        agg[float_cols] = agg[float_cols].round(2)
        
        # Validate
        assert (agg['pct_of_state'] >= 0).all(), "pct_of_state has negative values!"
        
        output_path = config.AGG_DIR / "agg_city_by_state.csv"
        agg.to_csv(output_path, index=False)
        
        print(f"  Saved: {len(agg)} rows -> {output_path.name}")
        print(f"  States: {agg['State'].nunique()}, Cities: {agg['City'].nunique()}")
        return agg
    
    # =========================================================================
    # AGGREGATE 4: WEATHER BY STATE (Dashboard 4)
    # =========================================================================
    
    def agg_weather_by_state(self) -> pd.DataFrame:
        """
        Dashboard 4: Weather Impact BY STATE.
        Grain: State + Weather_Category
        Baseline: Clear weather of SAME state
        Clear = 0%, others >= 0%
        """
        print("\n[Aggregate] Creating agg_weather_by_state (Dashboard 4)")
        
        agg = self.df.groupby(['State', 'Weather_Category'], as_index=False).agg(
            total_accidents=('ID', 'count'),
            total_high_severity=('Severity', lambda x: (x >= config.HIGH_SEVERITY_THRESHOLD).sum()),
            avg_severity=('Severity', 'mean'),
            avg_duration_min=('Duration_min', 'mean')
        )
        
        # Get Clear baseline for each state
        clear_baseline = agg[agg['Weather_Category'] == 'Clear'][
            ['State', 'avg_severity', 'avg_duration_min']
        ].rename(columns={
            'avg_severity': 'clear_severity',
            'avg_duration_min': 'clear_duration'
        })
        
        agg = agg.merge(clear_baseline, on='State', how='left')
        
        # Fill missing baseline with state average
        state_avg = self.df.groupby('State', as_index=False).agg(
            state_avg_severity=('Severity', 'mean'),
            state_avg_duration=('Duration_min', 'mean')
        )
        agg = agg.merge(state_avg, on='State', how='left')
        
        agg['clear_severity'] = agg['clear_severity'].fillna(agg['state_avg_severity'])
        agg['clear_duration'] = agg['clear_duration'].fillna(agg['state_avg_duration'])
        
        # Calculate increase vs Clear baseline
        agg['severity_increase_pct'] = np.where(
            agg['Weather_Category'] == 'Clear',
            0.0,
            ((agg['avg_severity'] - agg['clear_severity']) / agg['clear_severity'] * 100).clip(lower=0)
        )
        
        agg['duration_increase_pct'] = np.where(
            agg['Weather_Category'] == 'Clear',
            0.0,
            ((agg['avg_duration_min'] - agg['clear_duration']) / agg['clear_duration'] * 100).clip(lower=0)
        )
        
        # Risk score using config
        def calc_risk_score(row):
            score = config.WEATHER_RISK_BASE.get(row['Weather_Category'], 1)
            
            if row['severity_increase_pct'] > 15: score += 3
            elif row['severity_increase_pct'] > 10: score += 2
            elif row['severity_increase_pct'] > 5: score += 1
            
            if row['duration_increase_pct'] > 30: score += 2
            elif row['duration_increase_pct'] > 15: score += 1
            
            return min(score, 10)
        
        agg['risk_score'] = agg.apply(calc_risk_score, axis=1)
        
        # Risk category using config
        agg['risk_category'] = np.select(
            [agg['risk_score'] >= config.RISK_EXTREME,
             agg['risk_score'] >= config.RISK_HIGH,
             agg['risk_score'] >= config.RISK_MODERATE],
            ['Extreme', 'High', 'Moderate'],
            default='Low'
        )
        
        agg['high_severity_pct'] = (agg['total_high_severity'] / agg['total_accidents'] * 100).round(2)
        
        # Sort with Clear first
        weather_order = {'Clear': 0, 'Cloudy': 1, 'Rainy': 2, 'Snowy': 3, 'Foggy': 4, 'Windy': 5}
        agg['weather_sort'] = agg['Weather_Category'].map(weather_order).fillna(6)
        agg = agg.sort_values(['State', 'weather_sort']).drop(columns=['weather_sort'])
        
        # Drop helper columns
        agg = agg.drop(columns=['state_avg_severity', 'state_avg_duration'], errors='ignore')
        
        float_cols = ['avg_severity', 'avg_duration_min', 'clear_severity', 'clear_duration',
                      'severity_increase_pct', 'duration_increase_pct']
        agg[float_cols] = agg[float_cols].round(2)
        
        # Validate
        clear_rows = agg[agg['Weather_Category'] == 'Clear']
        assert (clear_rows['severity_increase_pct'] == 0).all(), "Clear should have 0% increase!"
        assert (agg['severity_increase_pct'] >= 0).all(), "No negative severity_increase_pct!"
        
        output_path = config.AGG_DIR / "agg_weather_by_state.csv"
        agg.to_csv(output_path, index=False)
        
        print(f"  Saved: {len(agg)} rows -> {output_path.name}")
        print(f"  States: {agg['State'].nunique()}, Weather types: {agg['Weather_Category'].nunique()}")
        return agg
    
    # =========================================================================
    # SUPPLEMENTARY: TIME PATTERNS
    # =========================================================================
    
    def agg_time_pattern(self) -> pd.DataFrame:
        """Supplementary: Time pattern analysis."""
        print("\n[Aggregate] Creating agg_time_pattern (Supplementary)")
        
        agg = self.df.groupby(['Hour', 'DayOfWeek'], as_index=False).agg(
            total_accidents=('ID', 'count'),
            avg_severity=('Severity', 'mean'),
            avg_duration_min=('Duration_min', 'mean')
        )
        
        day_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        agg['DayName'] = agg['DayOfWeek'].map(lambda x: day_names[x])
        
        agg['is_rush_hour'] = agg['Hour'].isin([7, 8, 9, 16, 17, 18]).astype(int)
        agg['is_weekend'] = (agg['DayOfWeek'] >= 5).astype(int)
        
        def get_period(hour):
            if 6 <= hour < 9: return 'Morning_Rush'
            if 9 <= hour < 12: return 'Morning'
            if 12 <= hour < 14: return 'Lunch'
            if 14 <= hour < 17: return 'Afternoon'
            if 17 <= hour < 20: return 'Evening_Rush'
            return 'Night'
        
        agg['time_period'] = agg['Hour'].apply(get_period)
        
        max_acc = agg['total_accidents'].max()
        agg['hotspot_score'] = (agg['total_accidents'] / max_acc * 10).round(1)
        
        agg[['avg_severity', 'avg_duration_min']] = agg[['avg_severity', 'avg_duration_min']].round(2)
        
        output_path = config.AGG_DIR / "agg_time_pattern.csv"
        agg.to_csv(output_path, index=False)
        
        print(f"  Saved: {len(agg)} rows -> {output_path.name}")
        return agg
    
    # =========================================================================
    # MAIN RUNNER
    # =========================================================================
    
    def run_all(self) -> None:
        """Execute complete aggregation pipeline."""
        print("\n" + "="*60)
        print("AGGREGATE GENERATION PIPELINE")
        print("="*60)
        
        self.load_tables()
        
        self.agg_federal()
        self.agg_state_anomaly()
        self.agg_city_by_state()
        self.agg_weather_by_state()
        self.agg_time_pattern()
        
        print("\n" + "="*60)
        print("AGGREGATION COMPLETE")
        print("="*60)
        print("\nOutput files:")
        print("  - agg_federal.csv           (Dashboard 1)")
        print("  - agg_state_anomaly.csv     (Dashboard 2)")
        print("  - agg_city_by_state.csv     (Dashboard 3)")
        print("  - agg_weather_by_state.csv  (Dashboard 4)")
        print("  - agg_time_pattern.csv      (Supplementary)")
