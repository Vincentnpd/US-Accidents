"""
Utility functions
Helper functions used across modules
"""

import pandas as pd
import numpy as np


def calculate_pareto(df, group_col, value_col, threshold=0.8):
    """
    Calculate Pareto principle (80/20 rule)
    
    Args:
        df: DataFrame
        group_col: Column to group by
        value_col: Column to calculate cumulative percentage
        threshold: Cumulative percentage threshold (default 0.8)
    
    Returns:
        DataFrame with cumulative percentage and top flag
    """
    result = df.groupby(group_col)[value_col].sum().reset_index()
    result = result.sort_values(value_col, ascending=False)
    
    result['Cumulative_Pct'] = result[value_col].cumsum() / result[value_col].sum()
    result['Is_Top'] = (result['Cumulative_Pct'] <= threshold).astype(int)
    
    return result


def get_severity_category(severity):
    """Convert numeric severity to category"""
    categories = {
        1: 'Minor',
        2: 'Moderate',
        3: 'Serious',
        4: 'Severe'
    }
    return categories.get(severity, 'Unknown')


def get_time_period(hour):
    """Categorize hour into time period"""
    if 6 <= hour < 9:
        return 'Morning_Rush'
    elif 9 <= hour < 12:
        return 'Morning'
    elif 12 <= hour < 14:
        return 'Lunch'
    elif 14 <= hour < 17:
        return 'Afternoon'
    elif 17 <= hour < 20:
        return 'Evening_Rush'
    elif 20 <= hour < 23:
        return 'Evening'
    else:
        return 'Night'


def calculate_yoy_change(df, time_col, value_col):
    """
    Calculate year-over-year change
    
    Args:
        df: DataFrame
        time_col: Time column (e.g., 'Year')
        value_col: Value column to calculate change
    
    Returns:
        Series with YoY percentage change
    """
    df_sorted = df.sort_values(time_col)
    return df_sorted[value_col].pct_change() * 100


def validate_dataframe(df, required_cols):
    """
    Validate DataFrame has required columns
    
    Args:
        df: DataFrame to validate
        required_cols: List of required column names
    
    Raises:
        ValueError if missing columns
    """
    missing = set(required_cols) - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def optimize_dtypes(df):
    """
    Optimize DataFrame dtypes to reduce memory
    
    Args:
        df: DataFrame to optimize
    
    Returns:
        Optimized DataFrame
    """
    for col in df.select_dtypes(include=['int64']).columns:
        df[col] = pd.to_numeric(df[col], downcast='integer')
    
    for col in df.select_dtypes(include=['float64']).columns:
        df[col] = pd.to_numeric(df[col], downcast='float')
    
    return df