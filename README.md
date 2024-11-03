# Weather API Pipeline

## Overview

Using Python, create a simple data pipeline to extract, transform, and load weather data into a PostgreSQL database for easier analysis. The pipeline is orchestrated with Airflow and automatically updates weather data in regular intervals.

The code currently uses ```try-except``` blocks to catch exceptions when making API call. The pipeline runs on a 30-minute schedule which is within API rate limits, but will raise an error if too many calls are made.

### Assumptions

Before beginning this pipeline, the assumptions made in order to complete this workflow were fixed latitude and longitude inputs which can be changed in your .env file.

## Technologies Used

**Python**: The main language for ETL tasks\
**Airflow**: Orchestrates and manages workflow on a schedule\
**Open Weather Map API**: Main weather data source using "Current weather data" API. Documentation: https://openweathermap.org/current \
**PostgreSQL**: Database that stores transformed API data\
**Pandas/SQLAlchemy**: Handles data processing, transformation, and database connections\
**Logging**: For tracking events as pipeline occurs (Info/Error)

## Structure

**Tasks**:
```fetch_weather_data()```: Grabs JSON data through requests library and stores result into a variable, data

```transform_weather_data()```: Transforms the response data into a dictionary, renaming and normalizing some columns. Specific transformations include: Changing column tempurature from Kelvin to Farenheit, point_in_time to the country's local timezone

```load_weather_data()```: Converts dictionary to dataframe and loads data into Postgres table,**weather_data_histories**,  with provided insert query. We add a constraint on latitude, longitude, point_in_time to ensure duplicate entries will be rejected by the database.

## Prerequisites
* Python 3.9
* Airflow 2.2.3
* PostgreSQL
* OpenWeatherMap API
* ENVIRONMENT VARIABLES:
  - API_KEY: Provided from OpenWeatherMap
  - LAT and LON: Provided by user, in our case we used LAT = "40.7128", and LON = "-74.0060"
  - POSTGRES_CONN: your postgres database.

## Database Schema
 <img width="1312" alt="Untitled" src="https://github.com/user-attachments/assets/3e223a64-0fcc-44e6-8bb8-2f772fb78f84">

 ```
 CREATE TABLE weather_data_histories (
    id SERIAL PRIMARY KEY,
    latitude FLOAT NOT NULL,
    longitude FLOAT NOT NULL,
    country VARCHAR(100),
    city_id INT,
    city_name VARCHAR(100),
    weather_description VARCHAR(100),
    temperature FLOAT,
    humidity INT,
    visibility INT,
    wind_speed FLOAT,
    cloud_percent INT,
    point_in_time TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
ADD CONSTRAINT unique_weather_data UNIQUE (latitude, longitude, point_in_time);
COMMENT ON COLUMN weather_data_histories.temperature IS 'Temperature of location in Farenheit'
COMMENT ON COLUMN weather_data_histories.wind_speed IS 'Speed in MPH';
COMMENT ON COLUMN weather_data_histories.point_in_time IS 'Local time of the city';
COMMENT ON COLUMN weather_data_histories.cloud_percent IS 'Percent cloudiness';
COMMENT ON COLUMN weather_data_histories.visibility IS 'Visibility in meters';
 ```

## Unit Test considerations

To make this more robust, I would recommend diving further into the *pytest* python testing framework for unit testing. Here the goal would be to write small and readable tests for this entire workflow. \
I would consider using a sample JSON file for the transformation step and run the workflow individually and export the output to ensure it looks as intended. Run a similar process as the transformation step and load the data into your local setup/database. The steps to create the table are in this readME. Confirm all tables have been imported.



## Reflection

With more resources, I would have loved to implement an intermediate table that holds all the raw JSON API data. In case of failure, at least we would have the data stored historically in our database. This would be beneficial for error handling as well.

To better monitor errors, I would like to define a simple callback upon failure using Airflow's EmailOperator to send essential team members an update if a portion of the workflow failed.
