import requests
import duckdb
import pandas as pd
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv() # Load environment variables from .env file

API_KEY = os.getenv("API_KEY") # Get key from .env
MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN")
CITIES = ["Abuja", "London", "Tokyo", "Lagos", "Paris", "New York", "Kaduna"]
BASE_URL = "http://api.openweathermap.org/data/2.5/weather"
DB_NAME = "weather_db"

def transform_data(raw_data):
    city_name = raw_data.get('name')
    country = raw_data.get('sys', {}).get('country')

    # Main weather details
    main_weather = raw_data.get('weather', [{}])[0].get('main')
    description = raw_data.get('weather', [{}])[0].get('description')

    # Get temperature (and convert from Kelvin to Celsius)
    temp_kelvin = raw_data.get('main', {}).get('temp')
    temp_celsius = temp_kelvin - 273.15 if temp_kelvin else None

    feels_like_kelvin = raw_data.get('main', {}).get('feels_like')
    feels_like_celsius = feels_like_kelvin - 273.15 if feels_like_kelvin else None

    humidity = raw_data.get('main', {}).get('humidity')
    wind_speed = raw_data.get('wind', {}).get('speed')

    report_timestamp_utc = raw_data.get('dt')
    report_datetime_utc = datetime.utcfromtimestamp(report_timestamp_utc) if report_timestamp_utc else None
    
    # Script run Timestamp
    record_datetime_utc = datetime.utcnow()

    clean_data = {
        'city': city_name,
        'country': country,
        'main_weather': main_weather,
        'description': description,
        'temp_celsius': temp_celsius,
        'feels_like_celsius': feels_like_celsius,
        'humidity_percent': humidity,
        'wind_speed_mps': wind_speed,
        'report_datetime_utc': report_datetime_utc,
        'record_datetime_utc': record_datetime_utc,
    }

    # Return a DataFrame for this one city
    df = pd.DataFrame(clean_data, index=[0])
    return df

print("Starting weather data pipeline...")

all_dataframes = [] # List to hold DataFrames for each city

print(f"Attempting to fetch data for {len(CITIES)} cities...")

for city in CITIES:
    request_url = f"{BASE_URL}?q={city}&appid={API_KEY}"
    
    try:
        response = requests.get(request_url)

        if response.status_code == 200:
            raw_data = response.json()
            print(f"Successfully fetched and transformed data for: {city}")
            clean_df = transform_data(raw_data)
            all_dataframes.append(clean_df) # Add the DataFrame to our list
            
        else:
            print(f"Error for {city}: Received status code {response.status_code} | {response.text}")

    except Exception as e:
        print(f"An error occurred fetching data for {city}: {e}")

#Load into Duckdb
if all_dataframes:
    final_df = pd.concat(all_dataframes, ignore_index=True)

    print("\n Combined Data (DataFrame)")
    print(final_df)

    connection_string = f"md:{DB_NAME}?motherduck_token={MOTHERDUCK_TOKEN}"
    
    print(f"\nConnecting to MotherDuck database: {DB_NAME}")
    conn = duckdb.connect(connection_string, read_only=False)
    
    table_name = "weather_reports"
    
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} AS 
        SELECT * FROM final_df LIMIT 0
    """)
    
    conn.execute(f"INSERT INTO {table_name} SELECT * FROM final_df")
    
    print(f"Successfully loaded {len(final_df)} records into '{table_name}' table.")
    
    # Verification Step
    print(f"\nVerifying data from '{table_name}' table (last 5 records):")
    result_df = conn.sql(f"SELECT * FROM {table_name} ORDER BY record_datetime_utc DESC LIMIT 5").df()
    print(result_df)
    
    conn.close()
else:
    print("No data was fetched. Database was not updated.")

print("\nPipeline run finished.")