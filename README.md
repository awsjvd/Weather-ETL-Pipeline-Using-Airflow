# Weather ETL Pipeline

This project is an ETL (Extract, Transform, Load) pipeline that retrieves weather data from the Open-Meteo API, transforms it, and loads it into a Snowflake data warehouse using Apache Airflow. The pipeline runs daily and fetches the current weather data for Aarhus, Denmark.

## Technologies Used
- **Apache Airflow**
- **Python**
- **Requests**
- **Snowflake**

## How It Works
1. **Extract**: The pipeline fetches the weather data using the Open-Meteo API.
2. **Transform**: The raw weather data is cleaned and formatted.
3. **Load**: The transformed data is loaded into a Snowflake database.

## Running the Pipeline
To run this pipeline, make sure you have Apache Airflow and the required Python libraries installed. The pipeline runs daily, but you can manually trigger it through the Airflow UI or the CLI.

## License
This project is licensed under the MIT License.
