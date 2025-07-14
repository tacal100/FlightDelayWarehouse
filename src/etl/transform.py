from datetime import timedelta

import numpy as np
import pandas as pd


def interpolate_all_weather_columns(df):
    """
    Fill all weather columns with debug output.
    """
    df = df.copy()
    
    # Store original index to maintain row order
    df['original_index'] = df.index
    
    df['datetime'] = pd.to_datetime(df['FL_DATE']) + pd.to_timedelta(df['DEP_HOUR'].fillna(0), unit='h')
    df = df.set_index('datetime')
    df = df.sort_values(['ORIGIN', 'datetime'])
    
    # Convert numeric columns
    numeric_cols = ['WIND_SPD', 'TEMPERATURE', 'VISIBILITY']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    print(f'Amount of NaN values before interpolation: {df[numeric_cols + ["ACTIVE_WEATHER"]].isna().sum()}')
    # Forward/backward fill for ACTIVE_WEATHER
    if 'ACTIVE_WEATHER' in df.columns:
        df['ACTIVE_WEATHER'] = df.groupby('ORIGIN')['ACTIVE_WEATHER'].transform(
            lambda x: x.ffill().bfill()
        )
    
    # Time interpolation for numeric columns
    for col in numeric_cols:
        if col in df.columns:
            df[col] = df.groupby('ORIGIN')[col].transform(
                lambda x: x.interpolate(method='time')
            )
    
    # Reset index and restore original order
    df = df.reset_index()
    df = df.sort_values('original_index')
    df = df.drop(['original_index', 'datetime'], axis=1)
    df = df.reset_index(drop=True)
    print(f'Amount of NaN values after interpolation: {df[numeric_cols + ["ACTIVE_WEATHER"]].isna().sum()}')
    # Drop all rows with NaN in numeric columns or ACTIVE_WEATHER since they couldnt be interpolated
    df.dropna(subset=numeric_cols + ['ACTIVE_WEATHER'], inplace=True) 
 
    return df


def remove_duplicates(df, subset=None):
    """
    Remove duplicate rows from DataFrame based on a subset of columns.
    If subset is None, it checks for all columns.
    """
    original_flights_len = len(df)
    if subset is None:
        df = df.drop_duplicates()
    else:
        df = df.drop_duplicates(subset=subset)
    print(f"Removed duplicates from flights data: {original_flights_len - len(df)} rows")   
    return df
    

def clean_flights_csv_data(df):
    """Clean the entire flights CSV before table creation"""
    initial_count = len(df)
    
    # Remove invalid delays
    df = df[(df['DEP_DELAY'] >= -60)]
    new_count = len(df)
    print(f"Flights CSV: Removed {initial_count - len(df)} invalid rows after delay cleaning")
    # Remove cancelled flights with no valid DEP_HOUR
    df = df[~((df['DEP_HOUR'] > 0) & (df['CANCELLED'] > 0))]
    print(f"Flights CSV: Removed {new_count - len(df)} invalid rows after cancellation cleaning")
    new_count = len(df)
    # Remove invalid temperature values (e.g. unrealistic temperatures -> temps are in celsius also see for hottest day in 2022 (53 Celsius) https://en.wikipedia.org/wiki/2022_North_American_heat_waves#:~:text=On%20September%201%2C%20Death%20Valley,of%20North%20America%20at%20large.)
    df = df[(df['TEMPERATURE'] <= 60) & (df['TEMPERATURE'] >= -40)]
    print(f"Flights CSV: Removed {new_count - len(df)} invalid rows after temperature cleaning")
    new_count = len(df)
    # Remove invalid wind speed values (e.g. unrealistic wind speeds -> see Wind_Speed_Outliers in Data/Charts also see https://www.skyscanner.com/tips-and-inspiration/what-windspeed-delays-flights#:~:text=With%20this%20in%20mind%2C%20horizontal,affect%20take%2Doff%20and%20landing.)
    df = df[(df['WIND_SPD'] <= 35) & (df['WIND_SPD'] >= 0)]
    print(f"Flights CSV: Removed {new_count - len(df)} invalid rows after wind speed cleaning")
    new_count = len(df)
    # Remove entries where aircraft manufacturer is unknown or missing
    df = df[(df['MANUFACTURER'].notna()) & (df['MANUFACTURER'].str.lower() != 'unknown')]
    print(f"Flights CSV: Removed {new_count - len(df)} invalid rows after manufacturer cleaning")

    return df
