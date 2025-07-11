from datetime import timedelta

import numpy as np
import pandas as pd


#Find the nearest neighbor for ACTIVE_WEATHER in a time window and interpolate based on time for numerical weather columns
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
    
    
def create_time_dimension_from_flights(flights_df):
    """Create time dimension from existing flights data"""
    
    # Convert FL_DATE to datetime
    flights_df['FL_DATE'] = pd.to_datetime(flights_df['FL_DATE'])
    
    # Get unique dates
    unique_dates = flights_df['FL_DATE'].drop_duplicates().sort_values()
    
    time_data = []
    for date in unique_dates:
        time_data.append({
            'date_id': int(date.strftime('%Y%m%d')),
            'full_date': date.date(),
            'day': date.day,
            'month': date.month,
            'month_name': date.strftime('%B'),
            'quarter': f"Q{(date.month-1)//3 + 1}",
            'quarter_num': (date.month-1)//3 + 1,
            'year': date.year
        })
    
    return pd.DataFrame(time_data)

def create_flights_fact_table(flights_df, time_dim, routes_df, weather_df):
    """Create proper fact table with dimensional foreign keys"""
    
    fact_flights = flights_df.copy()
    
    # Add date_id from time dimension
    fact_flights['FL_DATE'] = pd.to_datetime(fact_flights['FL_DATE'])
    fact_flights['date_id'] = fact_flights['FL_DATE'].dt.strftime('%Y%m%d').astype(int)
    
    fact_columns = [
        'flight_id',        
        'date_id',          # FK to time dimension
        'route_id',
        'weather_id',
        'TAIL_NUM',
        'DEP_HOUR',
        'DEP_DELAY',
        'CANCELLED',
        'CRS_DEP_TIME'
    ]
    
    available_columns = [col for col in fact_columns if col in fact_flights.columns]
    return fact_flights[available_columns]

def transform_to_aircraft_dimension(aircraft_df, carriers_df):
    """Create aircraft dimension with carrier information"""
    
    # Merge aircraft with carriers for complete dimension
    aircraft_dim = aircraft_df.merge(
        carriers_df[['OP_UNIQUE_CARRIER', 'CARRIER_NAME']], 
        on='OP_UNIQUE_CARRIER', 
        how='left'
    )
    
    return aircraft_dim

def transform_to_airport_dimension(airports_df):
    """Transform airports to dimension"""
    return airports_df.rename(columns={'iata_code': 'airport_code'})

# def transform_to_weather_dimension(weather_df):
#     """Transform weather to dimension with categories"""
#     weather_dim = weather_df.copy()
    
#     # Add categories using your Weather model
#     for _, row in weather_dim.iterrows():
#         weather_dim.loc[row.name, 'temp_category'] = 
#         weather_dim.loc[row.name, 'wind_category'] = 
#         weather_dim.loc[row.name, 'visibility_category'] = 
    
#     return weather_dim

def clean_flights_csv_data(df):
    """Clean the entire flights CSV before table creation"""
    initial_count = len(df)
    
    # Remove invalid delays
    df = df[(df['DEP_DELAY'] >= -60) & (df['DEP_DELAY'] <= 1440)]
    
    # Remove invalid hours
    df = df[(df['DEP_HOUR'] >= 0) & df['DEP_HOUR'] <= 23]
    # Remove cancelled flights with no valid DEP_HOUR
    df = df[~((df['DEP_HOUR'] > 0) & (df['CANCELLED'] > 0))]
    df = df[(df['TEMPERATURE'] <= 60) & (df['TEMPERATURE'] >= -20)]
    
    print(f"Flights CSV: Removed {initial_count - len(df)} invalid rows")
    return df


def create_custom_table(data, columns, table_name="custom_table", add_id=True, id_column_name=None):
    """
    Create a custom table with specified columns
    
    Args:
        data (pd.DataFrame): Source data
        columns (list): List of column names to include
        table_name (str): Name for the table (used for ID column naming)
        add_id (bool): Whether to add an ID column
        id_column_name (str): Custom name for ID column, if None uses f"{table_name}_id"
    
    Returns:
        pd.DataFrame: Table with specified columns
    """
    # Filter columns that exist in the data
    available_columns = [col for col in columns if col in data.columns]
    missing_columns = [col for col in columns if col not in data.columns]
    
    if missing_columns:
        print(f"Warning: Missing columns for {table_name}: {missing_columns}")
    
    if not available_columns:
        print(f"Error: No available columns for {table_name}")
        return pd.DataFrame()
    
    # Create table with available columns
    table = data[available_columns].drop_duplicates().reset_index(drop=True)
    
    # Add ID column if requested
    if add_id:
        id_name = id_column_name if id_column_name else f"{table_name}_id"
        table[id_name] = range(1, len(table) + 1)
        # Move ID column to the front
        cols = [id_name] + [col for col in table.columns if col != id_name]
        table = table[cols]
    
    print(f"Created {table_name} table with {len(table)} rows and columns: {list(table.columns)}")
    return table

def create_normalized_tables_flights(flights_data, carriers_data=None):
    
    tables = {}
    
    tables['aircraft'] = create_custom_table(
        flights_data, 
        ['TAIL_NUM', 'MANUFACTURER', 'ICAO TYPE', 'YEAR OF MANUFACTURE', 'OP_UNIQUE_CARRIER'],
        'aircraft',
        add_id=False  # TAIL_NUM is already the primary key
    )
    
    carriers_base = create_custom_table(
        flights_data,
        ['OP_UNIQUE_CARRIER'],
        'carriers_base',
        add_id=False # OP_UNIQUE_CARRIER is already the primary key
    )
    
    if carriers_data is not None and not carriers_data.empty:
        # Merge carriers with descriptions
        tables['carriers'] = carriers_base.merge(
            carriers_data[['CODE', 'DESCRIPTION']], 
            left_on='OP_UNIQUE_CARRIER', 
            right_on='CODE', 
            how='left'
        ).drop('CODE', axis=1).rename(columns={"DESCRIPTION": "CARRIER_NAME"})  # Remove duplicate CODE column
        print(f"Created carriers table with descriptions: {len(tables['carriers'])} rows")
    else:
        tables['carriers'] = carriers_base
        print(f"Created carriers table without descriptions: {len(tables['carriers'])} rows")
    

    # Take unique airport codes from ORIGIN and DEST columns for airports table
    airports_data = []
    if 'ORIGIN' in flights_data.columns:
        airports_data.extend(flights_data['ORIGIN'].unique())
    if 'DEST' in flights_data.columns:
        airports_data.extend(flights_data['DEST'].unique())
    
    if airports_data:
        airports_df = pd.DataFrame({
            'airport_code': list(set(airports_data))
        }).dropna().reset_index(drop=True)
        tables['airports'] = create_custom_table(
            airports_df,
            ['airport_code'],
            'airports',
            id_column_name='airport_code' # Use airport_code as primary key
        )
    else:
        tables['airports'] = pd.DataFrame()
    
    tables['routes'] = create_custom_table(
        flights_data,
        ['ORIGIN', 'DEST'],
        'routes',
        add_id=True,
        id_column_name='route_id'
    )
    
    tables['weather'] = create_custom_table(
        flights_data,
        ['WIND_SPD', 'TEMPERATURE', 'ACTIVE_WEATHER', 'VISIBILITY'],
        'weather',
        add_id=True,
        id_column_name='weather_id'
    )
    
    # First create the base flights table
    flights_base = create_custom_table(
        flights_data,
        ['FL_DATE', 'DEP_HOUR', 'CRS_DEP_TIME', 'DEP_DELAY', 'CANCELLED', 'TAIL_NUM', 'ORIGIN', 'DEST', 
         'WIND_SPD', 'TEMPERATURE', 'ACTIVE_WEATHER', 'VISIBILITY'],
        'flights_temp',
        add_id=True,
        id_column_name='flight_id'
    )
    
    # Add route_id foreign key
    if not tables['routes'].empty and not flights_base.empty:
        flights_with_route = flights_base.merge(
            tables['routes'][['route_id', 'ORIGIN', 'DEST']], 
            on=['ORIGIN', 'DEST'], 
            how='left'
        )
    else:
        flights_with_route = flights_base.copy()
        flights_with_route['route_id'] = None
    
    # Add weather_id foreign key
    if not tables['weather'].empty and not flights_with_route.empty:
        flights_with_weather = flights_with_route.merge(
            tables['weather'][['weather_id', 'WIND_SPD', 'TEMPERATURE', 'ACTIVE_WEATHER', 'VISIBILITY']], 
            on=['WIND_SPD', 'TEMPERATURE', 'ACTIVE_WEATHER', 'VISIBILITY'], 
            how='left'
        )
    else:
        flights_with_weather = flights_with_route.copy()
        flights_with_weather['weather_id'] = None
    
    # Create final flights table with only necessary columns (removing original ORIGIN, DEST, weather columns)
    final_flights_columns = [
        'flight_id', 'FL_DATE', 'DEP_HOUR', 'CRS_DEP_TIME', 'DEP_DELAY', 
        'CANCELLED', 'TAIL_NUM', 'route_id', 'weather_id'
    ]
    
    # Filter to only include columns that exist
    available_flights_columns = [col for col in final_flights_columns if col in flights_with_weather.columns]
    tables['flights'] = flights_with_weather[available_flights_columns].copy()
    
    print(f"Created flights table with {len(tables['flights'])} rows and columns: {list(tables['flights'].columns)}")
    
    return tables   

def create_normalized_tables_airports(airports_data):
    """
    Create normalized tables for airports data
    
    Args:
        airports_data (pd.DataFrame): DataFrame containing airport information
    
    Returns:
        dict: Dictionary of normalized tables
    """
    tables = {}
    
    # 1. Airports Table
    tables['airports'] = create_custom_table(
        airports_data,
        ['iata_code', 'city', 'state'],
        'airports',
        add_id=False  # iata_code is already the primary key
    )
    
    return tables

# Create date dimension from flights data
def create_date_dimension(flights_df):
    """
    Create date dimension with day, week, month, year columns only.
    """
    dates = pd.to_datetime(flights_df['FL_DATE']).dt.date.unique()
    
    dim_dates = pd.DataFrame({
        'date': pd.to_datetime(dates),
        'day': pd.to_datetime(dates).day,
        'week': pd.to_datetime(dates).isocalendar().week,
        'month': pd.to_datetime(dates).month,
        'year': pd.to_datetime(dates).year
    })
    
    # Sort by date
    dim_dates = dim_dates.sort_values('date').reset_index(drop=True)
    
    return dim_dates

def create_airport_dimension(airports_df):
    """
    Create airport dimension with airport_code as primary key.
    """
    airports_dim = airports_df.rename(columns={'iata_code': 'airport_code'})
    
    # Ensure unique airport codes
    airports_dim = airports_dim.drop_duplicates(subset=['airport_code']).reset_index(drop=True)
    
    return airports_dim