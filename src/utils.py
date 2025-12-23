"""
Utility Functions Module
Reusable helper functions across pipeline
"""

import pandas as pd
import numpy as np


def calculate_pareto(df, group_col, value_col, threshold=0.8):
    """
    Calculate Pareto analysis (80/20 rule)
    
    Parameters:
        df: DataFrame
        group_col: Column to group by (e.g. 'City')
        value_col: Column to aggregate (e.g. 'Total_Accidents')
        threshold: Cumulative percentage threshold (default 0.8)
    
    Returns:
        DataFrame with pareto analysis
    """
    grouped = df.groupby(group_col)[value_col].sum().sort_values(ascending=False)
    
    result = pd.DataFrame({
        group_col: grouped.index,
        value_col: grouped.values
    })
    
    result['Percentage'] = result[value_col] / result[value_col].sum() * 100
    result['Cumulative_Percentage'] = result['Percentage'].cumsum()
    result['Pareto_Group'] = np.where(
        result['Cumulative_Percentage'] <= threshold * 100,
        'Top_80%',
        'Bottom_20%'
    )
    
    return result.reset_index(drop=True)


def get_severity_category(severity):
    """
    Convert numeric severity to text category
    
    Parameters:
        severity: int (1-4)
    
    Returns:
        str: 'Minor', 'Moderate', 'Serious', 'Severe'
    """
    severity_map = {
        1: 'Minor',
        2: 'Moderate',
        3: 'Serious',
        4: 'Severe'
    }
    return severity_map.get(severity, 'Unknown')


def get_time_period(hour):
    """
    Categorize hour into time period
    
    Parameters:
        hour: int (0-23)
    
    Returns:
        str: Time period name
    """
    if 0 <= hour < 6:
        return 'Night'
    elif 6 <= hour < 9:
        return 'Morning_Rush'
    elif 9 <= hour < 12:
        return 'Late_Morning'
    elif 12 <= hour < 15:
        return 'Afternoon'
    elif 15 <= hour < 18:
        return 'Evening_Rush'
    elif 18 <= hour < 21:
        return 'Evening'
    else:
        return 'Late_Night'


def calculate_yoy_change(df, year_col, value_col):
    """
    Calculate year-over-year percentage change
    
    Parameters:
        df: DataFrame with year and value columns
        year_col: Name of year column
        value_col: Name of value column
    
    Returns:
        DataFrame with YoY change column
    """
    df = df.sort_values(year_col)
    df['YoY_Change'] = df[value_col].pct_change() * 100
    return df


def validate_dataframe(df, required_columns):
    """
    Validate DataFrame has required columns
    
    Parameters:
        df: DataFrame to validate
        required_columns: List of required column names
    
    Returns:
        bool: True if valid, raises ValueError if not
    """
    missing = [col for col in required_columns if col not in df.columns]
    
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    
    return True


def optimize_dtypes(df):
    """
    Optimize DataFrame memory by downcasting numeric types
    
    Parameters:
        df: DataFrame to optimize
    
    Returns:
        DataFrame with optimized dtypes
    """
    initial_memory = df.memory_usage(deep=True).sum() / 1024**2
    
    for col in df.select_dtypes(include=['int']).columns:
        df[col] = pd.to_numeric(df[col], downcast='integer')
    
    for col in df.select_dtypes(include=['float']).columns:
        df[col] = pd.to_numeric(df[col], downcast='float')
    
    final_memory = df.memory_usage(deep=True).sum() / 1024**2
    saved = initial_memory - final_memory
    
    print(f"Memory optimized: {initial_memory:.1f} MB -> {final_memory:.1f} MB (saved {saved:.1f} MB)")
    
    return df


def get_weather_category(condition):
    """
    Categorize weather condition into simplified groups
    
    Parameters:
        condition: str (weather condition text)
    
    Returns:
        str: Simplified category
    """
    if pd.isna(condition):
        return 'Clear'
    
    condition = str(condition).lower()
    
    if any(word in condition for word in ['clear', 'fair']):
        return 'Clear'
    elif any(word in condition for word in ['rain', 'drizzle', 'shower']):
        return 'Rain'
    elif any(word in condition for word in ['snow', 'sleet', 'ice', 'freezing']):
        return 'Snow'
    elif any(word in condition for word in ['fog', 'mist', 'haze']):
        return 'Fog'
    elif any(word in condition for word in ['cloud', 'overcast']):
        return 'Cloudy'
    elif any(word in condition for word in ['thunder', 'storm']):
        return 'Storm'
    elif any(word in condition for word in ['wind']):
        return 'Windy'
    else:
        return 'Other'


def create_date_dimension(start_date, end_date):
    """
    Create a complete date dimension table
    
    Parameters:
        start_date: str or datetime
        end_date: str or datetime
    
    Returns:
        DataFrame with date dimension
    """
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    
    dim_date = pd.DataFrame({
        'Date': date_range,
        'Year': date_range.year,
        'Quarter': date_range.quarter,
        'Month': date_range.month,
        'Month_Name': date_range.strftime('%B'),
        'Week': date_range.isocalendar().week,
        'Day': date_range.day,
        'DayOfWeek': date_range.dayofweek,
        'DayOfWeek_Name': date_range.strftime('%A'),
        'Is_Weekend': date_range.dayofweek.isin([5, 6]).astype(int),
        'Is_Holiday': 0
    })
    
    return dim_date