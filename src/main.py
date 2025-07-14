"""
Main entry point for the flight data warehouse ETL process
"""
import os
import sys

import pandas as pd

from etl.extract import (extract_airports_data, extract_carriers_data,
                         extract_flights_data)
from etl.load import transform_to_star_schema
from etl.transform import (clean_flights_csv_data,
                           interpolate_all_weather_columns, remove_duplicates)
from utils.output import plot_flight_data_eda, save_tables

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
    
    null_df = pd.DataFrame(null_stats)
    null_df = null_df.sort_values('Null_Percentage', ascending=False)
    
    print("\nNULL VALUE SUMMARY BY COLUMN:")
    print(null_df.to_string(index=False))
    
    

    
    # Find rows with all nulls
    all_null_rows = df.isnull().all(axis=1).sum()
    print(f"Rows with ALL null values: {all_null_rows:,}")
    
    # Find rows with no nulls
    no_null_rows = df.notnull().all(axis=1).sum()
    print(f"Rows with NO null values: {no_null_rows:,} ({(no_null_rows/total_rows)*100:.1f}%)")
    
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
    # if os.path.exists(filtered_csv):
    #     print(f"Using existing filtered data from: {filtered_csv}")
    #     filtered_flights_csv = pd.read_csv(filtered_csv)
    #     print(f"Successfully loaded {len(filtered_flights_csv)} filtered flights")
    # else:
        
    #     # Extract data
    #     raw_data = pd.read_csv(flights_csv)
    #     print(raw_data.columns)
    #     # Define required columns
    required_columns_flights = [
        'FL_DATE', 'DEP_HOUR', 'CRS_DEP_TIME', 'DEP_DELAY', 'CANCELLED',
        'TAIL_NUM', 'MANUFACTURER', 'ICAO TYPE', 'YEAR OF MANUFACTURE', 
        'OP_UNIQUE_CARRIER', 'ORIGIN', 'DEST',
        'WIND_SPD', 'TEMPERATURE', 'ACTIVE_WEATHER', 'VISIBILITY'
    ]

    required_columns_airports = [
        'AIRPORT', 'DISPLAY_AIRPORT_CITY_NAME_FULL', 'AIRPORT_STATE_NAME'
    ]

    filtered_flights_csv = extract_flights_data(flights_csv, filtered_csv, required_columns_flights)
    filtered_airports_csv = extract_airports_data(airports_csv, required_columns_airports)
    carriers_data = extract_carriers_data(carriers_csv)

    #plot_flight_data_eda(filtered_flights_csv)
    
    # Only keep airports that exist in flights data
    flight_airports = set(filtered_flights_csv['ORIGIN'].unique()) | set(filtered_flights_csv['DEST'].unique())
    filtered_airports_csv = filtered_airports_csv[filtered_airports_csv['iata_code'].isin(flight_airports)]
    # Clean city names
    filtered_airports_csv['city'] = filtered_airports_csv['city'].str.replace(r',\s*[A-Z]{2}$', '', regex=True)

    print(f"Filtered airports to {len(filtered_airports_csv)} airports that appear in flights data")
    
    analyze_null_values(filtered_flights_csv, "Flights Data")

    # Remove duplicate entries (check by flight time, tail number for plane and departure time (there cant be multiple entries like this for a single plane))
    filtered_flights_csv = remove_duplicates(filtered_flights_csv, subset=['FL_DATE','TAIL_NUM', 'CRS_DEP_TIME'])
    print(len(filtered_flights_csv[filtered_flights_csv["DEP_DELAY"] > 180]))
    #Find the nearest neighbor for ACTIVE_WEATHER in a time window and interpolate based on time for numerical weather columns (drop remaining NaNs (Approx. 3000 rows))
    filtered_flights_csv = interpolate_all_weather_columns(filtered_flights_csv)

    # Whats done in clean flights:
    # 1. Remove delay entries that are more than an hour before scheduled flight (negative departure delays) 
    #    and  more than 23 hours after scheduled flight as these are edge cases in commercial flights
    # 2. Remove cancelled flights with no valid DEP_HOUR (DEP_HOUR != 0)
    # 3. Remove invalid temperature values (e.g. unrealistic temperatures, see figure Wind_Speed_Outliers in Data/Charts)
    # 4. Remove invalid wind speed values (e.g. unrealistic wind speeds -> see Wind_Speed_Outliers in Data/Charts also see https://www.skyscanner.com/tips-and-inspiration/what-windspeed-delays-flights#:~:text=With%20this%20in%20mind%2C%20horizontal,affect%20take%2Doff%20and%20landing.)
    # 5. Remove rows where aircraft manufacturer is unknown or missing
    filtered_flights_csv = clean_flights_csv_data(filtered_flights_csv)

    finalSchema = transform_to_star_schema(filtered_flights_csv, filtered_airports_csv, carriers_data)
    save_tables(finalSchema, suffix="_star")

if __name__ == "__main__":
    main()