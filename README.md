# Weather API Pipeline
## Overview
Using Python, create a simple data pipeline to extract, transform, and load weather data into a PostgreSQL database for easier analysis. The pipeline is orchestrated with Airflow and automically updates weather data in regular intervals.

### Assumptions
Before beginning this pipeline, the assumptions made in order to complete this workflow were fixed latitude and longitude inputs which can be changed in your .env file.

## Technologies Used
**Python**: The main language for ETL tasks
**Airflow**: Orchestrates and manages workflow on a schedule
**Open Weather Map API**: Main weather data source using "Current weather data" API. Documentation: https://openweathermap.org/current
**PostgreSQL**: Database that stores transformed API data
**Pandas/SQLAlchemy**: Handles data processing, transformation, and database connections
**Logging**: For tracking events as pipeline occurs (Info/Error)

## Structure
**Tasks**:
```fetch_weather_data()```: Grabs JSON data through requests library and stores result into a variable, data
```transform_weather_data()```: Transforms the response data into a dictionary, renaming and normalizing some columns
```load_weather_data()```: Converts dictionary to dataframe and loads data into Postgres table, **weather_data_histories**,  with provided insert query

## Prerequisites
* Python 3.9
* Airflow 2.2.3
* PostgreSQL
* OpenWeatherMap API
* ENVIRONMENT VARIABLES:
  - API_KEY: Provided from OpenWeatherMap
  - LAT and LON: Provided by user, in our case we used LAT = "40.7128", and LON = "74.0060"
  - POSTGRES_CONN: your postgres database.

## Database Schema
 <img width="1312" alt="Untitled" src="https://github.com/user-attachments/assets/3e223a64-0fcc-44e6-8bb8-2f772fb78f84">
