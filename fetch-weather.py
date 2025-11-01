import requests
import duckdb
import pandas as pd
import os
from datetime import datetime
from dotenv import load_dotenv
import gspread
import gspread_dataframe

# Load environment variables from .env file
load_dotenv() 

API_KEY = os.getenv("API_KEY") 
# MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN")
CITIES = ["Abuja", "London", "Tokyo", "Lagos", "Paris", "New York", "Kaduna"]
BASE_URL = "http://api.openweathermap.org/data/2.5/weather"

CREDENTIALS_FILE = "credentials.json" 
SHEET_NAME = "Live Weather Data" 

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

    # Dictionary to hold weather details
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

# Main Exec
print("Starting weather data pipeline...")

# List to hold DataFrames for each city
all_dataframes = [] 

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

#Load into GoogleSheets
if all_dataframes:
    final_df = pd.concat(all_dataframes, ignore_index=True)
    
    # Re-order columns to be a bit more logical
    columns_order = [
        'record_datetime_utc',
        'report_datetime_utc',
        'city',
        'country',
        'main_weather',
        'description',
        'temp_celsius',
        'feels_like_celsius',
        'humidity_percent',
        'wind_speed_mps'
    ]
    final_df = final_df[columns_order]
    
    print("\n Combined Data (DataFrame)")
    print(final_df)
    
    try:
        print(f"\nConnecting to Google Sheets using {CREDENTIALS_FILE}...")
        # Authenticate using the .json service account file
        gc = gspread.service_account(filename=CREDENTIALS_FILE)
        
        sh = gc.open(SHEET_NAME)
        
        worksheet = sh.worksheet("Sheet1")
        
        print(f"Loading data into worksheet '{worksheet.title}'...")
        
        # Define the headers
        headers = final_df.columns.values.tolist()

        # Get the headers in the sheet
        headers_in_sheet = []
        try:
            headers_in_sheet = worksheet.row_values(1)
        except gspread.exceptions.APIError:
            pass 

        # Check if headers are missing or incorrect
        if not headers_in_sheet or headers_in_sheet != headers:
            print("Headers are missing or incorrect. Clearing sheet and writing new headers...")
            worksheet.clear()
    
            worksheet.append_row(headers, value_input_option='USER_ENTERED')
            print("Headers written successfully.")
        else:
            print("Headers are correct, skipping...")
        

        # Convert timestamp columns to strings
        final_df['record_datetime_utc'] = final_df['record_datetime_utc'].astype(str)
        final_df['report_datetime_utc'] = final_df['report_datetime_utc'].astype(str)

        # Now, append all the data rows
        print("Appending new data rows...")
        data_to_append = final_df.values.tolist()
        worksheet.append_rows(data_to_append, value_input_option='USER_ENTERED')
        
        print(f"Successfully loaded {len(final_df)} new records into '{SHEET_NAME}'.")
    
    except Exception as e:
        print(f"An error occurred loading data to Google Sheets: {e}")

else:
    print("No data was fetched. Database was not updated.")

print("\nPipeline run finished.")