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


