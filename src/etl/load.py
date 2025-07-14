import pandas as pd


def create_date_dimension(flights_df):
    """
    Create date dimension with day, week, month, year
    """
    # Get unique dates
    dates = pd.to_datetime(flights_df['FL_DATE']).dt.date.unique()
    dates_df = pd.DataFrame({'date': pd.to_datetime(dates)})
    
    # Create date components
    dim_date = pd.DataFrame({
        'date': dates_df['date'],
        'month': dates_df['date'].dt.month,
        'quarter': dates_df['date'].dt.quarter,
        'year': dates_df['date'].dt.year
    })
    
    # Sort by date
    dim_date = dim_date.sort_values('date').reset_index(drop=True)
    
    return dim_date


def create_weather_dimension(flights_df):
    """
    Create weather dimension with WEATHER_STATUS_DESCRIPTION instead of ACTIVE_WEATHER
    """
    # Map ACTIVE_WEATHER to descriptions FIRST
    weather_status_map = {
        0.0: 'No weather events present',
        1.0: 'Weather event(s) present',
        2.0: 'Significant weather event(s) present'
    }
    
    # Copy and transform the data
    df_weather = flights_df[['WIND_SPD', 'TEMPERATURE', 'ACTIVE_WEATHER', 'VISIBILITY']].copy()
    df_weather['WIND_SPD'] = df_weather['WIND_SPD'].round(0)
    df_weather['TEMPERATURE'] = df_weather['TEMPERATURE'].round(0)
    df_weather['VISIBILITY'] = df_weather['VISIBILITY'].round(1)
    
    # Replace ACTIVE_WEATHER with description
    df_weather['WEATHER_STATUS_DESCRIPTION'] = df_weather['ACTIVE_WEATHER'].map(weather_status_map).fillna('Unknown')
    df_weather = df_weather.drop('ACTIVE_WEATHER', axis=1)
    
    # Get unique combinations based on ALL columns including description
    dim_weather = df_weather.drop_duplicates().reset_index(drop=True)
    dim_weather['weather_id'] = range(1, len(dim_weather) + 1)
    
    # Reorder columns
    dim_weather = dim_weather[[
        'weather_id',
        'WIND_SPD',
        'TEMPERATURE',
        'WEATHER_STATUS_DESCRIPTION',
        'VISIBILITY'
    ]]
    
    return dim_weather


def create_star_schema_dimensions(filtered_flights_csv, filtered_airports_csv, carriers_data):
    """
    Create dimension tables for star schema with proper keys
    """
    dimensions = {}
    
    # 1. Date Dimension 
    dimensions['dim_date'] = create_date_dimension(filtered_flights_csv)
    
    # 2. Weather Dimension
    dimensions['dim_weather'] = create_weather_dimension(filtered_flights_csv)
    
    # 3. Aircraft Dimension
    aircraft_cols = ['TAIL_NUM', 'MANUFACTURER', 'ICAO TYPE', 'YEAR OF MANUFACTURE', 'OP_UNIQUE_CARRIER']
    dim_aircraft = filtered_flights_csv[aircraft_cols].drop_duplicates(subset=['TAIL_NUM']).reset_index(drop=True)
    dimensions['dim_aircraft'] = dim_aircraft
    
    # 4. AircraftCarriers Dimension
    carriers_dim = filtered_flights_csv[['OP_UNIQUE_CARRIER']].drop_duplicates().reset_index(drop=True)
    if carriers_data is not None and not carriers_data.empty:
        carriers_dim = carriers_dim.merge(
            carriers_data[['CODE', 'DESCRIPTION']], 
            left_on='OP_UNIQUE_CARRIER', 
            right_on='CODE', 
            how='left'
        )
        carriers_dim = carriers_dim.rename(columns={'DESCRIPTION': 'carrier_name'})
        carriers_dim = carriers_dim.drop('CODE', axis=1)
    else:
        carriers_dim['carrier_name'] = 'Unknown'
    dimensions['dim_aircraft_carriers'] = carriers_dim
    
    # 5. Airports Dimension
    dim_airports = filtered_airports_csv.copy()
    dim_airports['airport_id'] = range(1, len(dim_airports) + 1)
    dim_airports = dim_airports[['airport_id', 'iata_code', 'city', 'state']]
    dimensions['dim_airports'] = dim_airports
        
    # 6. Cancellation Dimension
    cancellation_data = {
        'cancellation_code': [0, 1, 2, 3, 4],
        'cancellation_status': [
            'Not Cancelled',
            'Carrier Cancellation',
            'Weather Cancellation',
            'National Air System Cancellation',
            'Security Cancellation'
        ]
    }
    dimensions['dim_cancellation'] = pd.DataFrame(cancellation_data)
    
    return dimensions


def create_fact_table(filtered_flights_csv, dimensions):
    """
    Create fact table with proper foreign keys
    """
    fact_flights = filtered_flights_csv.copy()
    
    # Round weather values to match dimension
    fact_flights['WIND_SPD'] = fact_flights['WIND_SPD'].round(0)
    fact_flights['TEMPERATURE'] = fact_flights['TEMPERATURE'].round(0)
    fact_flights['VISIBILITY'] = fact_flights['VISIBILITY'].round(1)
    
    # Generate flight_id as PK
    fact_flights['flight_id'] = range(1, len(fact_flights) + 1)
    
    # 1. Keep flight_date
    fact_flights['date'] = pd.to_datetime(fact_flights['FL_DATE'])
    
    # 2. Transform ACTIVE_WEATHER to description for joining
    weather_status_map = {
        0.0: 'No weather events present',
        1.0: 'Weather event(s) present',
        2.0: 'Significant weather event(s) present'
    }
    fact_flights['WEATHER_STATUS_DESCRIPTION'] = fact_flights['ACTIVE_WEATHER'].map(weather_status_map).fillna('Unknown')
    
    # Join with weather dimension using description instead of code
    weather_cols = ['WIND_SPD', 'TEMPERATURE', 'WEATHER_STATUS_DESCRIPTION', 'VISIBILITY']
    fact_flights = fact_flights.merge(
        dimensions['dim_weather'][['weather_id'] + weather_cols],
        on=weather_cols,
        how='left'
    )
    
    fact_flights = fact_flights.drop(weather_cols + ['ACTIVE_WEATHER'], axis=1)
    
    # 3. Add origin and destination airport FKs directly
    # Join origin airport
    fact_flights = fact_flights.merge(
        dimensions['dim_airports'][['iata_code', 'airport_id']], 
        left_on='ORIGIN', 
        right_on='iata_code', 
        how='left'
    )
    fact_flights = fact_flights.rename(columns={'airport_id': 'origin_airport_oid'})
    fact_flights = fact_flights.drop('iata_code', axis=1)
    
    # Join destination airport
    fact_flights = fact_flights.merge(
        dimensions['dim_airports'][['iata_code', 'airport_id']], 
        left_on='DEST', 
        right_on='iata_code', 
        how='left'
    )
    fact_flights = fact_flights.rename(columns={'airport_id': 'dest_airport_oid'})
    fact_flights = fact_flights.drop('iata_code', axis=1)

    # Somehow the airport IDs are not integers if I dont convert them
    fact_flights['origin_airport_oid'] = fact_flights['origin_airport_oid'].astype('Int64')
    fact_flights['dest_airport_oid'] = fact_flights['dest_airport_oid'].astype('Int64')

    # Drop ORIGIN and DEST after getting airport IDs
    fact_flights = fact_flights.drop(['ORIGIN', 'DEST'], axis=1)
    
    # 4. Rename columns to match fact table schema
    fact_flights = fact_flights.rename(columns={
        'DEP_HOUR': 'scheduled_dep_time',
        'DEP_DELAY': 'departure_delay',
        'CANCELLED': 'cancellation_code',  
        'TAIL_NUM': 'TAIL_NUM' 
    })
    
    # Add is_cancelled binary measure
    fact_flights['is_cancelled'] = (fact_flights['cancellation_code'] > 0).astype(int)
    
    # 5. Drop unnecessary columns
    if 'FL_DATE' in fact_flights.columns:
        fact_flights = fact_flights.drop('FL_DATE', axis=1)
    if 'CRS_DEP_TIME' in fact_flights.columns:
        fact_flights = fact_flights.drop('CRS_DEP_TIME', axis=1)
    
    # Drop aircraft-related columns
    aircraft_cols_to_drop = ['MANUFACTURER', 'ICAO TYPE', 'YEAR OF MANUFACTURE', 'OP_UNIQUE_CARRIER']
    for col in aircraft_cols_to_drop:
        if col in fact_flights.columns:
            fact_flights = fact_flights.drop(col, axis=1)
    
    # Select final columns in proper order
    fact_columns = [
        'flight_id',
        'date',
        'scheduled_dep_time',
        'departure_delay',
        'is_cancelled',
        'cancellation_code',
        'TAIL_NUM',
        'origin_airport_oid',
        'dest_airport_oid',
        'weather_id'
    ]
    
    fact_flights = fact_flights[fact_columns]
    
    return fact_flights


def transform_to_star_schema(filtered_flights_csv, filtered_airports_csv, carriers_data):
    """
    Main function to transform normalized data to star schema
    """
    print("\nCreating Star Schema...")
    
    dimensions = create_star_schema_dimensions(
        filtered_flights_csv, 
        filtered_airports_csv, 
        carriers_data
    )
    
    fact_flights = create_fact_table(filtered_flights_csv, dimensions)
    star_schema = {
        'fact_flights': fact_flights,
        **dimensions
    }
    
    print("\nStar Schema Created:")
    for table_name, df in star_schema.items():
        print(f"  {table_name}: {len(df)} rows, {len(df.columns)} columns")
        if len(df) > 0:
            print(f"    Columns: {list(df.columns)}")
        
    print("\nForeign Key Verification:")
    print(f"  Origin airports: {fact_flights['origin_airport_oid'].nunique()} unique airports")
    print(f"  Destination airports: {fact_flights['dest_airport_oid'].nunique()} unique airports")
    print(f"  Weather references {fact_flights['weather_id'].nunique()} unique weather conditions")
    print(f"  Aircraft (TAIL_NUM) references {fact_flights['TAIL_NUM'].nunique()} unique aircraft")
    print(f"  Cancellation codes: {fact_flights['cancellation_code'].value_counts().to_dict()}")
    
    return star_schema