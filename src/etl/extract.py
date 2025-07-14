import os
import pandas as pd

def extract_flights_data(flights_csv, filtered_csv, required_columns_flights):
    """
    Extracts and filters flight data from the raw CSV.
    If a filtered CSV exists, loads it directly.
    Otherwise, reads the raw CSV, filters columns, and saves the filtered version.
    """
    if os.path.exists(filtered_csv):
        print(f"Using existing filtered data from: {filtered_csv}")
        filtered_flights_csv = pd.read_csv(filtered_csv)
        print(f"Successfully loaded {len(filtered_flights_csv)} filtered flights")
    else:
        raw_data = pd.read_csv(flights_csv)
        print(raw_data.columns)
        available_columns = [col for col in required_columns_flights if col in raw_data.columns]
        filtered_flights_csv = raw_data[available_columns].copy()
        os.makedirs(os.path.dirname(filtered_csv), exist_ok=True)
        filtered_flights_csv.to_csv(filtered_csv, index=False)
    return filtered_flights_csv

def extract_airports_data(airports_csv, required_columns_airports):
    raw_airports_data = pd.read_csv(airports_csv)
    available_columns = [col for col in required_columns_airports if col in raw_airports_data.columns]
    filtered_airports_csv = raw_airports_data[available_columns].copy()
    filtered_airports_csv = filtered_airports_csv.rename(columns={
        'AIRPORT': 'iata_code',
        'DISPLAY_AIRPORT_CITY_NAME_FULL': 'city',
        'AIRPORT_STATE_NAME': 'state'
    })
    return filtered_airports_csv

def extract_carriers_data(carriers_csv):
    return pd.read_csv(carriers_csv)