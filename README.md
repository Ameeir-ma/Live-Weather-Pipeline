Live Weather Data Pipeline
A complete, automated ETL pipeline built to ingest, transform, and load live weather data from a public API into a Google Sheet for real-time analytics.

📊 Live Dashboard
➡️ https://lookerstudio.google.com/reporting/8e283f61-c5a2-4aac-93d0-b435ac657c99

(Note: Please allow a moment for the dashboard to load the latest data.)

<img width="754" height="569" alt="image" src="https://github.com/user-attachments/assets/0067f1c3-4f53-457b-a9df-b6fb6f4abf1a" />

🏗️ Project Architecture
This project follows a modern, cloud-based ETL (Extract, Transform, Load) architecture:

Extract: A Python script runs on an automated hourly schedule via GitHub Actions to fetch live weather data for multiple global cities from the OpenWeather API.

Transform: The raw JSON data is parsed, cleaned, and transformed into a clean, tabular format using the Pandas library. Key transformations include converting temperatures from Kelvin to Celsius and formatting timestamps.

Load: The transformed data is authenticated using a Google Cloud Service Account and loaded directly into a Google Sheet, where each new run appends the latest data.

Visualize: Google Looker Studio connects directly to the Google Sheet as a live data source to build an interactive, auto-refreshing dashboard.

🛠️ Tech Stack
Automation: GitHub Actions

Data Extraction: Python, OpenWeather API, requests

Data Transformation: Python, pandas

Data Loading: Google Sheets API, gspread

Authentication: Google Cloud Service Accounts, .env files

Visualization: Google Looker Studio

🚀 How It Works
1. Automation (.github/workflows/main.yml)
A GitHub Actions workflow is scheduled to run at the top of every hour. It performs the following steps:

Checks out the repository code.

Sets up a Python 3.11 environment.

Installs all required libraries from requirements.txt.

Creates a credentials.json file on the virtual runner by reading the GCP_SA_KEY repository secret.

Executes the fetch-weather.py script, securely passing the OPENWEATHER_API_KEY as an environment variable.

2. ETL Script (fetch-weather.py)
This script is the engine of the pipeline:

Extract: Loops through a list of cities and fetches data from the BASE_URL.

Transform: The transform_data function cleans the JSON response, converts timestamps, and formats the data into a pandas DataFrame.

Load:

The script authenticates with Google using the credentials.json file.

It opens the "Live Weather Data" Google Sheet.

It performs a robust check: it reads Row 1 of the sheet and compares it to the script's expected headers.

If the headers are missing or incorrect, it clears the sheet and writes the correct headers.

It then converts the pandas DataFrame to a list of lists and appends the new rows of data to the sheet.

⚙️ How to Run This Project Locally
Clone the repository:

Bash

git clone https://github.com/Ameeir-ma/Live-Weather-Pipeline.git
cd Live-Weather-Pipeline
Create a virtual environment:

Bash

python -m venv venv
source venv/bin/activate  # On Windows: .\venv\Scripts\activate
Install dependencies:

Bash

pip install -r requirements.txt
Create your secret files: You will need two secret files in your main project folder.

.env file (for your API Key): Create a file named .env and add your OpenWeather API key:

Code snippet

OPENWEATHER_API_KEY=your_openweather_api_key_here
credentials.json file (for Google Sheets):

Follow the Google Cloud documentation to create a Service Account.

Enable the Google Drive API and Google Sheets API.

Download the service account's JSON key file.

Rename it to credentials.json and place it in this folder.

Create a new Google Sheet (e.g., "Live Weather Data") and share it with the client_email found inside your credentials.json file, giving it Editor permission.

Run the pipeline:

Bash

python fetch-weather.py
