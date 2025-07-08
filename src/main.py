"""
Main entry point for the flight data warehouse ETL process
"""
import os
import sys

import pandas as pd

# Add the src directory to the Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

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

def create_normalized_tables_flights(flights_data):
    
    tables = {}
    
    # 1. Aircraft Table
    tables['aircraft'] = create_custom_table(
        flights_data, 
        ['TAIL_NUM', 'MANUFACTURER', 'ICAO TYPE', 'YEAR OF MANUFACTURE', 'OP_CARRIER'],
        'aircraft',
        add_id=False  # TAIL_NUM is already the primary key
    )
    
    # 2. Aircraft Carriers Table
    tables['carriers'] = create_custom_table(
        flights_data,
        ['OP_CARRIER', 'CARRIER_NAME'],
        'carriers',
        add_id=False  # OP_CARRIER is already the primary key
    )
    
    # 3. Airports Table (from ORIGIN and DEST)
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
            add_id=True,
            id_column_name='airport_id'
        )
    else:
        tables['airports'] = pd.DataFrame()
    
    # 4. Routes Table
    tables['routes'] = create_custom_table(
        flights_data,
        ['ORIGIN', 'DEST'],
        'routes',
        add_id=True,
        id_column_name='route_id'
    )
    
    # 5. Weather Conditions Table
    tables['weather'] = create_custom_table(
        flights_data,
        ['WIND_SPEED', 'TEMPERATURE', 'ACTIVE_WEATHER', 'VISIBILITY'],
        'weather',
        add_id=True,
        id_column_name='weather_id'
    )
    
    # 6. Flights Table (Fact Table) with Foreign Keys
    # First create the base flights table
    flights_base = create_custom_table(
        flights_data,
        ['FL_DATE', 'DEP_HOUR', 'SCHED_DEP_TIME', 'DEP_DELAY', 'CANCELLED', 'TAIL_NUM', 'ORIGIN', 'DEST', 
         'WIND_SPEED', 'TEMPERATURE', 'ACTIVE_WEATHER', 'VISIBILITY'],
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
            tables['weather'][['weather_id', 'WIND_SPEED', 'TEMPERATURE', 'ACTIVE_WEATHER', 'VISIBILITY']], 
            on=['WIND_SPEED', 'TEMPERATURE', 'ACTIVE_WEATHER', 'VISIBILITY'], 
            how='left'
        )
    else:
        flights_with_weather = flights_with_route.copy()
        flights_with_weather['weather_id'] = None
    
    # Create final flights table with only necessary columns (removing original ORIGIN, DEST, weather columns)
    final_flights_columns = [
        'flight_id', 'FL_DATE', 'DEP_HOUR', 'SCHED_DEP_TIME', 'DEP_DELAY', 
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

def print_tables(tables):
    """Print all normalized tables"""
    for table_name, table_data in tables.items():
        if not table_data.empty:
            print(f"\n{'='*50}")
            print(f"{table_name.upper()} TABLE ({len(table_data)} rows)")
            print(f"{'='*50}")
            print(table_data.head(10).to_string(index=False))
            print(f"Columns: {list(table_data.columns)}")
        else:
            print(f"\n{table_name.upper()} TABLE: No data available")

def save_tables(tables, output_dir="./Data/tables"):
    """Save normalized tables to CSV files"""
    os.makedirs(output_dir, exist_ok=True)
    
    for table_name, table_data in tables.items():
        if not table_data.empty:
            output_path = os.path.join(output_dir, f"{table_name}_table.csv")
            table_data.to_csv(output_path, index=False)
            print(f"Saved {table_name} table to: {output_path}")

def main():
    """Main ETL pipeline execution"""
    print("Starting Flight Data Warehouse ETL Process...")
    
    # Define data paths
    flights_csv = "./Data/CompleteData.csv"
    filtered_csv = "./Data/filtered_flights_2022_01_01_hour_0.csv"
    airports_csv = "./Data/us-airports.csv"
    
    # Check if filtered CSV already exists
    from etl.extract import extract_data
    if os.path.exists(filtered_csv):
        print(f"Using existing filtered data from: {filtered_csv}")
        filtered_flights_csv = pd.read_csv(filtered_csv)
        print(f"Successfully loaded {len(filtered_flights_csv)} filtered flights")
    else:
        print("Processing full dataset...")
        
        # Extract data
        raw_data = extract_data(flights_csv)
        # Define required columns
        required_columns_flights = [
            'FL_DATE', 'DEP_HOUR', 'SCHED_DEP_TIME', 'DEP_DELAY', 'CANCELLED',
            'TAIL_NUM', 'MANUFACTURER', 'ICAO TYPE', 'YEAR OF MANUFACTURE', 
            'OP_CARRIER', 'CARRIER_NAME', 'ORIGIN', 'DEST',
            'WIND_SPEED', 'TEMPERATURE', 'ACTIVE_WEATHER', 'VISIBILITY'
        ]
        
        # Filter columns that exist
        available_columns = [col for col in required_columns_flights if col in raw_data.columns]
        filtered_flights_csv = raw_data[available_columns].copy()

        # Save for future use
        os.makedirs("./Data", exist_ok=True)
        filtered_flights_csv.to_csv(filtered_csv, index=False)

    raw_airports_data = extract_data(airports_csv)
    required_columns_airports = [
        'iata_code', 'city', 'state'
    ]
    available_columns = [col for col in required_columns_airports if col in raw_airports_data.columns]
    filtered_airports_csv = raw_airports_data[available_columns].copy()
    # Create normalized tables
    print("\nCreating normalized tables...")
    tables = create_normalized_tables_flights(filtered_flights_csv)
    tablesAirport = create_normalized_tables_airports(filtered_airports_csv)

    # Print all tables
    print_tables(tables)
    
    # Save tables to CSV files
    save_tables(tables)
    
    print(f"\nProcess completed! Created {len([t for t in tables.values() if not t.empty])} tables")

if __name__ == "__main__":
    main()