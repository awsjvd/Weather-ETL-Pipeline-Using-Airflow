from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
import requests
import snowflake.connector
from datetime import datetime
import os

# Define the latitude and longitude for Aarhus city
LATITUDE = '56.2639'
LONGITUDE = '10.0677'

# Snowflake Connection Details (Replace these with environment variables or secrets management)
SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER', 'your_user_here')  # Use environment variable
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD', 'your_password_here')  # Use environment variable
SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT', 'your_account_here')  # Use environment variable
SNOWFLAKE_WAREHOUSE = 'COMPUTE_WH'  
SNOWFLAKE_DATABASE = 'AARHUS'
SNOWFLAKE_SCHEMA = 'weather'
SNOWFLAKE_TABLE = 'weather_data'

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}

# DAG Definition
with DAG(dag_id='weather_etl_pipeline',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dags:

    @task()
    def extract_weather_data():
        """Extract weather data from Open-Meteo API using requests."""
        # API endpoint to fetch weather data
        endpoint = f'https://api.open-meteo.com/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true'
        
        # Send a GET request to the API
        response = requests.get(endpoint)

        # Check if the request was successful
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch weather data: {response.status_code}")

    @task()
    def transform_weather_data(weather_data):
        """Transform the extracted weather data."""
        current_weather = weather_data['current_weather']
        
        # Use timestamp from the weather data (if available)
        timestamp = current_weather.get('timestamp', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))  # Default to current time if no timestamp is provided
        
        transformed_data = {
            'latitude': LATITUDE,
            'longitude': LONGITUDE,
            'temperature': current_weather['temperature'],
            'windspeed': current_weather['windspeed'],
            'winddirection': current_weather['winddirection'],
            'weathercode': current_weather['weathercode'],
            'timestamp': timestamp  # Use the extracted timestamp
        }
        
        return transformed_data

    @task()
    def load_weather_data(transformed_data):
        """Load transformed data into Snowflake."""
        # Establish Snowflake connection using the Snowflake connector
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )

        # Create cursor
        cursor = conn.cursor()

        try:
            # Set the warehouse explicitly in the session
            cursor.execute(f"USE WAREHOUSE {SNOWFLAKE_WAREHOUSE};")

            # Create table if it doesn't exist
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {SNOWFLAKE_TABLE} (
                latitude FLOAT,
                longitude FLOAT,
                temperature FLOAT,
                windspeed FLOAT,
                winddirection FLOAT,
                weathercode INT,
                timestamp TIMESTAMP
            );
            """
            cursor.execute(create_table_sql)

            # Insert transformed data into Snowflake
            insert_sql = f"""
            INSERT INTO {SNOWFLAKE_TABLE} (latitude, longitude, temperature, windspeed, winddirection, weathercode, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_sql, (
                transformed_data['latitude'],
                transformed_data['longitude'],
                transformed_data['temperature'],
                transformed_data['windspeed'],
                transformed_data['winddirection'],
                transformed_data['weathercode'],
                transformed_data['timestamp']  # Insert the timestamp from the weather data
            ))

            # Commit the transaction
            conn.commit()

        finally:
            # Close cursor and connection
            cursor.close()
            conn.close()

    # DAG Workflow - ETL Pipeline
    weather_data = extract_weather_data()
    transformed_data = transform_weather_data(weather_data)
    load_weather_data(transformed_data)
