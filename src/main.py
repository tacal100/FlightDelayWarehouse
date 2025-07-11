"""
Main entry point for the flight data warehouse ETL process
"""
import os
import sys

import pandas as pd

from etl.load import transform_to_star_schema
from etl.transform import (clean_flights_csv_data, create_airport_dimension,
                           create_date_dimension,
                           create_normalized_tables_airports,
                           create_normalized_tables_flights,
                           create_time_dimension_from_flights,
                           interpolate_all_weather_columns, remove_duplicates)
from utils.output import print_tables, save_tables

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def analyze_null_values(df, dataset_name="Dataset"):
    """Analyze null values in the dataset for cleaning insights"""
    print(f"\n{'='*60}")
    print(f"NULL VALUE ANALYSIS - {dataset_name}")
    print(f"{'='*60}")
    
    total_rows = len(df)
    print(f"Total rows: {total_rows:,}")
    
    # Overall null statistics
    null_stats = []
    for col in df.columns:
        null_count = df[col].isnull().sum()
        null_percentage = (null_count / total_rows) * 100
        data_type = str(df[col].dtype)
        unique_values = df[col].nunique()
        
        null_stats.append({
            'Column': col,
            'Null_Count': null_count,
            'Null_Percentage': round(null_percentage, 2),
            'Data_Type': data_type,
            'Unique_Values': unique_values,
            'Non_Null_Count': total_rows - null_count
        })
    
    # Convert to DataFrame for better display
    null_df = pd.DataFrame(null_stats)
    null_df = null_df.sort_values('Null_Percentage', ascending=False)
    
    print("\nNULL VALUE SUMMARY BY COLUMN:")
    print(null_df.to_string(index=False))
    
    # Categorize columns by null percentage
    critical_nulls = null_df[null_df['Null_Percentage'] > 50]
    high_nulls = null_df[(null_df['Null_Percentage'] > 20) & (null_df['Null_Percentage'] <= 50)]
    moderate_nulls = null_df[(null_df['Null_Percentage'] > 5) & (null_df['Null_Percentage'] <= 20)]
    low_nulls = null_df[null_df['Null_Percentage'] <= 5]
    
    print(f"\nCATEGORIZATION BY NULL SEVERITY:")
    print(f"🔴 CRITICAL (>50% null): {len(critical_nulls)} columns")
    if len(critical_nulls) > 0:
        print(f"   {list(critical_nulls['Column'])}")
    
    print(f"🟠 HIGH (20-50% null): {len(high_nulls)} columns")
    if len(high_nulls) > 0:
        print(f"   {list(high_nulls['Column'])}")
    
    print(f"🟡 MODERATE (5-20% null): {len(moderate_nulls)} columns")
    if len(moderate_nulls) > 0:
        print(f"   {list(moderate_nulls['Column'])}")
    
    print(f"🟢 LOW (<5% null): {len(low_nulls)} columns")
    if len(low_nulls) > 0:
        print(f"   {list(low_nulls['Column'])}")
    

    
    # Find rows with all nulls
    all_null_rows = df.isnull().all(axis=1).sum()
    print(f"Rows with ALL null values: {all_null_rows:,}")
    
    # Find rows with no nulls
    no_null_rows = df.notnull().all(axis=1).sum()
    print(f"Rows with NO null values: {no_null_rows:,} ({(no_null_rows/total_rows)*100:.1f}%)")
    
    # Most common null combinations
    null_pattern_counts = df.isnull().value_counts().head(5)
    print(f"\nTop 5 NULL patterns:")
    for pattern, count in null_pattern_counts.items():
        pct = (count / total_rows) * 100
        print(f"  Pattern: {dict(zip(df.columns, pattern))} - {count:,} rows ({pct:.1f}%)")
    
    return null_df


def main():
    """Main ETL pipeline execution"""
    print("Starting Flight Data Warehouse ETL Process...")
    
    # Define data paths
    flights_csv = "./Data/CompleteData.csv"
    filtered_csv = "./Data/filtered_flights_2022_01_01_hour_0.csv"
    #us-airports is a more extensive list but stations includes all airports in the flights data
    # If you want to use us-airports, change airports_csv to "./Data/us-airports.csv" and rename the columns accordingly
    airports_csv = "./Data/Stations.csv"
    carriers_csv = "./Data/Carriers.csv"
 
    
    # Check if filtered CSV already exists
    if os.path.exists(filtered_csv):
        print(f"Using existing filtered data from: {filtered_csv}")
        filtered_flights_csv = pd.read_csv(filtered_csv)
        print(f"Successfully loaded {len(filtered_flights_csv)} filtered flights")
    else:
        
        # Extract data
        raw_data = pd.read_csv(flights_csv)
        print(raw_data.columns)
        # Define required columns
        required_columns_flights = [
            'FL_DATE', 'DEP_HOUR', 'CRS_DEP_TIME', 'DEP_DELAY', 'CANCELLED',
            'TAIL_NUM', 'MANUFACTURER', 'ICAO TYPE', 'YEAR OF MANUFACTURE', 
            'OP_UNIQUE_CARRIER', 'ORIGIN', 'DEST',
            'WIND_SPD', 'TEMPERATURE', 'ACTIVE_WEATHER', 'VISIBILITY'
        ]
        
        # Filter columns that exist
        available_columns = [col for col in required_columns_flights if col in raw_data.columns]
        filtered_flights_csv = raw_data[available_columns].copy()


        # Save for future use
        os.makedirs("./Data", exist_ok=True)
        filtered_flights_csv.to_csv(filtered_csv, index=False)

    raw_airports_data =  pd.read_csv(airports_csv)
    carriers_data =  pd.read_csv(carriers_csv)
    required_columns_airports = [
        'AIRPORT', 'DISPLAY_AIRPORT_CITY_NAME_FULL', 'AIRPORT_STATE_NAME'
    ]

    available_columns = [col for col in required_columns_airports if col in raw_airports_data.columns]
    filtered_airports_csv = raw_airports_data[available_columns].copy()
    filtered_airports_csv = filtered_airports_csv.rename(columns={
        'AIRPORT': 'iata_code',
        'DISPLAY_AIRPORT_CITY_NAME_FULL': 'city',
        'AIRPORT_STATE_NAME': 'state'
    })
    
    # Only keep airports that exist in flights data
    flight_airports = set(filtered_flights_csv['ORIGIN'].unique()) | set(filtered_flights_csv['DEST'].unique())
    filtered_airports_csv = filtered_airports_csv[filtered_airports_csv['iata_code'].isin(flight_airports)]
    # Clean city names
    filtered_airports_csv['city'] = filtered_airports_csv['city'].str.replace(r',\s*[A-Z]{2}$', '', regex=True)

    print(f"Filtered airports to {len(filtered_airports_csv)} airports that appear in flights data")
    
    analyze_null_values(filtered_flights_csv, "Flights Data")

    filtered_flights_csv = remove_duplicates(filtered_flights_csv, subset=['FL_DATE','TAIL_NUM', 'CRS_DEP_TIME'])
    filtered_flights_csv = interpolate_all_weather_columns(filtered_flights_csv)
    filtered_flights_csv = clean_flights_csv_data(filtered_flights_csv)

    tables = create_normalized_tables_flights(filtered_flights_csv, carriers_data)
    tablesAirport = create_normalized_tables_airports(filtered_airports_csv)

    finalSchema = transform_to_star_schema(filtered_flights_csv, filtered_airports_csv, carriers_data)
    save_tables(finalSchema, suffix="_star")

if __name__ == "__main__":
    main()