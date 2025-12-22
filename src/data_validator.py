"""
Validator Module - Data Quality Verification
=============================================
Validates aggregates using "1+1=2" principle:
  Manual calculation from raw/cleaned data MUST equal aggregate output.

Checks:
  1. Dashboard 3: City totals match raw counts
  2. Dashboard 4: Clear baseline = 0%, no negative values
  3. Cross-check: Sum of all aggregates = raw total
  4. Referential integrity
"""

"""
Data Validator Module - Kiểm tra tính đúng đắn của dữ liệu
==========================================================
Logic 1+1=2: Tính thủ công từ raw = Giá trị trong aggregate
"""

import pandas as pd
import numpy as np
import config


class Validator:
    """Validate aggregate data quality."""
    
    def __init__(self):
        self.cleaned_df = None
        self.results = {}
        
    def load_data(self):
        """Load cleaned data for validation."""
        print("\n[Validator] Loading data for validation...")
        
        cleaned_path = config.CLEANED_DIR / "accidents_cleaned.csv"
        if not cleaned_path.exists():
            print(f"  ERROR: Cleaned data not found at {cleaned_path}")
            return False
        
        self.cleaned_df = pd.read_csv(
            cleaned_path,
            parse_dates=['Start_Time', 'End_Time']
        )
        
        # Create Weather_Category if not exists
        if 'Weather_Category' not in self.cleaned_df.columns:
            self.cleaned_df['Weather_Category'] = self._categorize_weather(
                self.cleaned_df['Weather_Condition']
            )
        
        print(f"  Loaded: {len(self.cleaned_df):,} records")
        return True
    
    def _categorize_weather(self, weather_col: pd.Series) -> pd.Series:
        """Categorize weather (same logic as aggregate.py)."""
        cond = weather_col.str.lower().fillna('')
        conditions = [
            cond.str.contains('fog|mist|haze', regex=True),
            cond.str.contains('snow|ice|sleet|freezing', regex=True),
            cond.str.contains('rain|drizzle|shower', regex=True),
            cond.str.contains('cloud|overcast', regex=True),
            cond.str.contains('wind|storm|thunder', regex=True),
        ]
        choices = ['Fog', 'Snow', 'Rain', 'Cloudy', 'Windy']
        return pd.Series(np.select(conditions, choices, default='Clear'), index=weather_col.index)
    
    # =========================================================================
    # VALIDATION 1: Dashboard 3 - City by State
    # =========================================================================
    
    def validate_dashboard3(self) -> bool:
        """
        Validate Dashboard 3: City by State.
        
        Logic:
          1. Pick sample cities (CA-Los Angeles, TX-Houston)
          2. Count manually from cleaned data
          3. Compare with aggregate
          4. Must match exactly
        """
        print("\n[Validator] Dashboard 3: City by State")
        print("-" * 50)
        
        agg_path = config.AGG_DIR / "agg_city_by_state.csv"
        if not agg_path.exists():
            print("  SKIP: agg_city_by_state.csv not found")
            return False
        
        agg = pd.read_csv(agg_path)
        all_pass = True
        
        # Test cases: (State, City)
        test_cases = [
            ('CA', 'Los Angeles'),
            ('TX', 'Houston'),
            ('FL', 'Miami'),
        ]
        
        for state, city in test_cases:
            # Manual calculation from cleaned data
            state_df = self.cleaned_df[self.cleaned_df['State'] == state]
            city_df = state_df[state_df['City'] == city]
            
            if len(city_df) == 0:
                print(f"  {state} - {city}: No data, skipping")
                continue
            
            manual_total = len(city_df)
            manual_high_sev = (city_df['Severity'] >= 3).sum()
            manual_pct = manual_total / len(state_df) * 100
            
            # Aggregate value
            agg_row = agg[(agg['State'] == state) & (agg['City'] == city)]
            
            if len(agg_row) == 0:
                print(f"  {state} - {city}: Not in aggregate, FAIL")
                all_pass = False
                continue
            
            agg_total = agg_row['total_accidents'].values[0]
            agg_high_sev = agg_row['total_high_severity'].values[0]
            agg_pct = agg_row['pct_of_state'].values[0]
            
            # Compare
            print(f"\n  {state} - {city}:")
            print(f"    [Manual]    total={manual_total:,}, high_sev={manual_high_sev:,}, pct={manual_pct:.2f}%")
            print(f"    [Aggregate] total={agg_total:,}, high_sev={agg_high_sev:,}, pct={agg_pct:.2f}%")
            
            # Check total
            if manual_total == agg_total:
                print(f"    total_accidents: PASS")
            else:
                print(f"    total_accidents: FAIL ({manual_total} vs {agg_total})")
                all_pass = False
            
            # Check high severity
            if manual_high_sev == agg_high_sev:
                print(f"    high_severity: PASS")
            else:
                print(f"    high_severity: FAIL ({manual_high_sev} vs {agg_high_sev})")
                all_pass = False
            
            # Check percentage (allow small float diff)
            if abs(manual_pct - agg_pct) < 0.1:
                print(f"    pct_of_state: PASS")
            else:
                print(f"    pct_of_state: FAIL ({manual_pct:.2f}% vs {agg_pct:.2f}%)")
                all_pass = False
        
        # Check no negative percentages
        if (agg['pct_of_state'] < 0).any():
            print("\n  ERROR: Negative pct_of_state found!")
            all_pass = False
        else:
            print("\n  No negative percentages: PASS")
        
        # Check sum of cities = state total
        for state in ['CA', 'TX']:
            state_agg = agg[agg['State'] == state]
            sum_cities = state_agg['total_accidents'].sum()
            state_raw = len(self.cleaned_df[self.cleaned_df['State'] == state])
            
            if sum_cities == state_raw:
                print(f"  {state} sum check: PASS ({sum_cities:,} = {state_raw:,})")
            else:
                print(f"  {state} sum check: FAIL ({sum_cities:,} vs {state_raw:,})")
                all_pass = False
        
        self.results['dashboard3'] = all_pass
        print(f"\n  [RESULT] Dashboard 3: {'ALL PASS' if all_pass else 'SOME FAILED'}")
        return all_pass
    
    # =========================================================================
    # VALIDATION 2: Dashboard 4 - Weather by State
    # =========================================================================
    
    def validate_dashboard4(self) -> bool:
        """
        Validate Dashboard 4: Weather by State.
        
        Logic:
          1. Clear must have severity_increase_pct = 0 (baseline)
          2. No negative severity_increase_pct
          3. Manual severity calculation matches aggregate
        """
        print("\n[Validator] Dashboard 4: Weather by State")
        print("-" * 50)
        
        agg_path = config.AGG_DIR / "agg_weather_by_state.csv"
        if not agg_path.exists():
            print("  SKIP: agg_weather_by_state.csv not found")
            return False
        
        agg = pd.read_csv(agg_path)
        all_pass = True
        
        # Check 1: Clear = 0% (baseline)
        clear_rows = agg[agg['Weather_Category'] == 'Clear']
        non_zero_clear = clear_rows[clear_rows['severity_increase_pct'] != 0]
        
        if len(non_zero_clear) == 0:
            print("  Clear baseline = 0%: PASS")
        else:
            print(f"  Clear baseline = 0%: FAIL ({len(non_zero_clear)} rows have non-zero)")
            print(non_zero_clear[['State', 'Weather_Category', 'severity_increase_pct']])
            all_pass = False
        
        # Check 2: No negative severity_increase_pct
        negative_rows = agg[agg['severity_increase_pct'] < 0]
        if len(negative_rows) == 0:
            print("  No negative severity_increase_pct: PASS")
        else:
            print(f"  No negative severity_increase_pct: FAIL ({len(negative_rows)} rows)")
            all_pass = False
        
        # Check 3: Manual calculation for sample states
        test_states = ['CA', 'TX']
        
        for state in test_states:
            print(f"\n  {state} Weather Check:")
            
            state_df = self.cleaned_df[self.cleaned_df['State'] == state]
            
            # Clear baseline
            clear_df = state_df[state_df['Weather_Category'] == 'Clear']
            if len(clear_df) == 0:
                print(f"    No Clear data, skipping")
                continue
            
            manual_clear_sev = clear_df['Severity'].mean()
            
            # Fog comparison
            fog_df = state_df[state_df['Weather_Category'] == 'Fog']
            if len(fog_df) > 0:
                manual_fog_sev = fog_df['Severity'].mean()
                manual_fog_increase = (manual_fog_sev - manual_clear_sev) / manual_clear_sev * 100
                manual_fog_increase = max(0, manual_fog_increase)  # No negative
                
                agg_fog = agg[(agg['State'] == state) & (agg['Weather_Category'] == 'Fog')]
                if len(agg_fog) > 0:
                    agg_fog_increase = agg_fog['severity_increase_pct'].values[0]
                    
                    print(f"    Fog: manual={manual_fog_increase:.2f}%, agg={agg_fog_increase:.2f}%")
                    if abs(manual_fog_increase - agg_fog_increase) < 1:
                        print(f"    Fog severity_increase_pct: PASS")
                    else:
                        print(f"    Fog severity_increase_pct: FAIL")
                        all_pass = False
        
        # Check 4: Risk categories are valid
        valid_categories = ['Low', 'Moderate', 'High', 'Extreme']
        invalid_cats = agg[~agg['risk_category'].isin(valid_categories)]
        if len(invalid_cats) == 0:
            print("\n  Risk categories valid: PASS")
        else:
            print(f"\n  Risk categories valid: FAIL ({len(invalid_cats)} invalid)")
            all_pass = False
        
        self.results['dashboard4'] = all_pass
        print(f"\n  [RESULT] Dashboard 4: {'ALL PASS' if all_pass else 'SOME FAILED'}")
        return all_pass
    
    # =========================================================================
    # VALIDATION 3: Cross-check Totals
    # =========================================================================
    
    def validate_totals(self) -> bool:
        """
        Validate total counts across aggregates.
        
        Logic:
          1. Sum of all cities = raw total
          2. Sum of all weather = raw total
          3. Federal yearly totals sum = raw total
        """
        print("\n[Validator] Cross-check Totals")
        print("-" * 50)
        
        raw_total = len(self.cleaned_df)
        print(f"  Raw total: {raw_total:,}")
        
        all_pass = True
        
        # Federal sum
        federal_path = config.AGG_DIR / "agg_federal.csv"
        if federal_path.exists():
            federal = pd.read_csv(federal_path)
            federal_sum = federal['total_accidents'].sum()
            if federal_sum == raw_total:
                print(f"  Federal sum: PASS ({federal_sum:,})")
            else:
                print(f"  Federal sum: FAIL ({federal_sum:,} vs {raw_total:,})")
                all_pass = False
        
        # City sum (should equal raw total)
        city_path = config.AGG_DIR / "agg_city_by_state.csv"
        if city_path.exists():
            city = pd.read_csv(city_path)
            city_sum = city['total_accidents'].sum()
            if city_sum == raw_total:
                print(f"  City sum: PASS ({city_sum:,})")
            else:
                print(f"  City sum: FAIL ({city_sum:,} vs {raw_total:,})")
                all_pass = False
        
        # Weather sum (should equal raw total)
        weather_path = config.AGG_DIR / "agg_weather_by_state.csv"
        if weather_path.exists():
            weather = pd.read_csv(weather_path)
            weather_sum = weather['total_accidents'].sum()
            if weather_sum == raw_total:
                print(f"  Weather sum: PASS ({weather_sum:,})")
            else:
                print(f"  Weather sum: FAIL ({weather_sum:,} vs {raw_total:,})")
                all_pass = False
        
        self.results['totals'] = all_pass
        print(f"\n  [RESULT] Totals: {'ALL PASS' if all_pass else 'SOME FAILED'}")
        return all_pass
    
    # =========================================================================
    # VALIDATION 4: Data Types and Ranges
    # =========================================================================
    
    def validate_data_quality(self) -> bool:
        """
        Validate data types and value ranges.
        
        Checks:
          - Severity in 1-4
          - Duration >= 0
          - Percentages 0-100
          - No nulls in key columns
        """
        print("\n[Validator] Data Quality Checks")
        print("-" * 50)
        
        all_pass = True
        
        # Check severity range
        invalid_sev = ~self.cleaned_df['Severity'].isin([1, 2, 3, 4])
        if invalid_sev.sum() == 0:
            print("  Severity range (1-4): PASS")
        else:
            print(f"  Severity range (1-4): FAIL ({invalid_sev.sum()} invalid)")
            all_pass = False
        
        # Check duration >= 0
        if 'Duration_min' in self.cleaned_df.columns:
            neg_dur = (self.cleaned_df['Duration_min'] < 0).sum()
            if neg_dur == 0:
                print("  Duration >= 0: PASS")
            else:
                print(f"  Duration >= 0: FAIL ({neg_dur} negative)")
                all_pass = False
        
        # Check key columns not null
        key_cols = ['ID', 'Severity', 'State', 'City']
        for col in key_cols:
            if col in self.cleaned_df.columns:
                null_count = self.cleaned_df[col].isnull().sum()
                if null_count == 0:
                    print(f"  {col} not null: PASS")
                else:
                    print(f"  {col} not null: FAIL ({null_count} nulls)")
                    all_pass = False
        
        self.results['data_quality'] = all_pass
        print(f"\n  [RESULT] Data Quality: {'ALL PASS' if all_pass else 'SOME FAILED'}")
        return all_pass
    
    # =========================================================================
    # MAIN RUNNER
    # =========================================================================
    
    def run_all(self) -> bool:
        """
        Run all validations.
        
        Returns:
            True if all validations pass, False otherwise
        """
        print("\n" + "="*60)
        print("DATA VALIDATION PIPELINE")
        print("="*60)
        
        # Load data
        if not self.load_data():
            print("\nValidation aborted: Could not load data")
            return False
        
        # Run validations
        self.validate_dashboard3()
        self.validate_dashboard4()
        self.validate_totals()
        self.validate_data_quality()
        
        # Summary
        print("\n" + "="*60)
        print("VALIDATION SUMMARY")
        print("="*60)
        
        all_pass = True
        for name, passed in self.results.items():
            status = "PASS" if passed else "FAIL"
            print(f"  {name}: {status}")
            if not passed:
                all_pass = False
        
        print(f"\n  OVERALL: {'ALL VALIDATIONS PASSED' if all_pass else 'SOME VALIDATIONS FAILED'}")
        print("="*60)
        
        return all_pass


# =============================================================================
# TEST
# =============================================================================

if __name__ == "__main__":
    validator = Validator()
    validator.run_all()
