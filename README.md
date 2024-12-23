# Weather ETL Pipeline using Airflow

## Table of Contents
- [Introduction](#introduction)
- [Problem Statement](#problem-statement)
- [About the Challenge](#about-the-challenge)
- [Solution](#solution)

## Introduction
The Weather ETL Pipeline is an automated data pipeline built using Apache Airflow. The pipeline fetches weather data from the Open-Meteo API, transforms the data, and loads it into a Snowflake database for further analysis. The aim of this project is to showcase how ETL processes can be implemented using Airflow for daily data extraction, transformation, and loading tasks.

The project specifically extracts real-time weather data (such as temperature, wind speed, and weather code) for Aarhus, Denmark, and stores the data into a Snowflake table for future analysis.

## Problem Statement
The problem at hand is to automate the process of fetching weather data from an external API and loading it into a Snowflake database. Danny, a data engineer, needs a streamlined way to manage his weather data and have it ready for analysis every day. The challenge is to build an ETL pipeline that performs the following tasks:

1. **Extract**: Get weather data from the Open-Meteo API for Aarhus.
2. **Transform**: Clean and transform the data into a format that is suitable for storage in a database.
3. **Load**: Store the transformed data into Snowflake to make it accessible for future analysis.

The goal is to automate this process, allowing Danny to track weather patterns daily without having to manually fetch and store the data.

## About the Challenge
In this challenge, I built an **ETL pipeline using Airflow** that does the following:

1. **Extract weather data**: Using the Open-Meteo API, the pipeline fetches the latest weather data based on predefined coordinates for Aarhus city.
2. **Transform the data**: The pipeline then processes and formats the weather data, including details like temperature, wind speed, wind direction, and weather code. It also ensures that the timestamp for the data is accurate.
3. **Load data into Snowflake**: Finally, the transformed weather data is loaded into a Snowflake database, where it can be accessed and analyzed by the business.

This pipeline runs on a **daily schedule**, ensuring that the weather data is always up-to-date and ready for use.

## Solution
### Steps:
1. **Airflow DAG Setup**: The ETL process is automated through an Airflow DAG (Directed Acyclic Graph), which runs daily.
2. **API Request**: The pipeline sends a GET request to the Open-Meteo API to fetch the weather data for Aarhus.
3. **Data Transformation**: Once the data is fetched, it is transformed into a structured format and the necessary fields (latitude, longitude, temperature, etc.) are extracted.
4. **Data Insertion into Snowflake**: After transforming the data, it is inserted into the Snowflake table `weather_data`.

The code is divided into three main tasks:
1. **Extract**: Fetches weather data using the Open-Meteo API.
2. **Transform**: Transforms the fetched data into a structured format.
3. **Load**: Loads the transformed data into a Snowflake database.

### Technologies Used:
- **Apache Airflow**: For orchestrating and scheduling the ETL tasks.
- **Open-Meteo API**: For extracting the weather data.
- **Snowflake**: For storing the transformed weather data in a cloud database.

### DAG Workflow:
1. **Extract Data** from Open-Meteo API.
2. **Transform** the raw data into a clean and structured format.
3. **Load** the transformed data into a Snowflake table for future use.

---

### Example of the Airflow DAG:

```python
from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
import requests
import snowflake.connector
from datetime import datetime

LATITUDE = '56.2639'
LONGITUDE = '10.0677'

SNOWFLAKE_USER = 'your_username'
SNOWFLAKE_PASSWORD = 'your_password'
SNOWFLAKE_ACCOUNT = 'your_account'
SNOWFLAKE_WAREHOUSE = 'your_warehouse'
SNOWFLAKE_DATABASE = 'your_database'
SNOWFLAKE_SCHEMA = 'your_schema'
SNOWFLAKE_TABLE = 'weather_data'

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}

with DAG(dag_id='weather_etl_pipeline',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dags:

    @task()
    def extract_weather_data():
        """Extract weather data from Open-Meteo API."""
        endpoint = f'https://api.open-meteo.com/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true'
        response = requests.get(endpoint)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch weather data: {response.status_code}")

    @task()
    def transform_weather_data(weather_data):
        """Transform the extracted weather data."""
        current_weather = weather_data['current_weather']
        timestamp = current_weather.get('timestamp', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        
        transformed_data = {
            'latitude': LATITUDE,
            'longitude': LONGITUDE,
            'temperature': current_weather['temperature'],
            'windspeed': current_weather['windspeed'],
            'winddirection': current_weather['winddirection'],
            'weathercode': current_weather['weathercode'],
            'timestamp': timestamp
        }
        return transformed_data

    @task()
    def load_weather_data(transformed_data):
        """Load transformed data into Snowflake."""
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )
        cursor = conn.cursor()
        try:
            cursor.execute(f"USE WAREHOUSE {SNOWFLAKE_WAREHOUSE};")
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
                transformed_data['timestamp']
            ))

            conn.commit()
        finally:
            cursor.close()
            conn.close()

    # DAG Workflow
    weather_data = extract_weather_data()
    transformed_data = transform_weather_data(weather_data)
    load_weather_data(transformed_data)
