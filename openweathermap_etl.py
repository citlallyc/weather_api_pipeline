import os
import requests
import pandas as pd
from dotenv import load_dotenv
import logging
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from airflow import DAG
from datetime import timedelta
import pendulum
from airflow.decorators import task

# load api key credentials
load_dotenv()
api_key = os.getenv("API_KEY")
lat = os.getenv("LAT")
lon = os.getenv("LON")
base_url = "https://api.openweathermap.org/data/2.5/weather?lat={}&lon={}&appid={}"
db_connection = os.getenv("POSTGRES_CONN")

# create the logger
logger = logging.getLogger('openweathermap_etl')

# set up dag object
with DAG(
    dag_id = "openweathermap_etl",
    default_args={
        "start_date": pendulum.datetime(2024, 10, 31, 2, tz="UTC"),
        "retries": 3,
        "retry_delay": timedelta(minutes=3)},
    schedule_interval = "@once",
    description = "Open Weather Map Data ETL Pipeline"
 ) as dag:

    @task()
    # fetch current weather data
    def fetch_weather_data():
        url = base_url.format(lat, lon, api_key)
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            message = "API extraction successful"
            logging.info("Log: {}".format(message))
            print("Print: {}".format(message))
            return data

        except requests.exceptions.RequestException as e:
            logging.error("Log: EXCEPTION | Error on API {}".format(e))
            print("Print: EXCEPTION | Error on API : {} ".format(e))
            raise

    @task()
    def transform_weather_data(data):
        try:
            # Transform the response data
            transformed_weather = {
                "latitude": data["coord"]["lat"],
                "longitude": data["coord"]["lon"],
                "country": data["sys"]["country"],
                "city_id": data["id"],
                "city_name": data["name"],
                "weather": data["weather"][0]["description"],
                "temperature": data["main"]["temp"],
                "humidity": data["main"]["humidity"],
                "visibility": data["visibility"],
                "wind_speed": data["wind"]["speed"],
                "cloud_percent": data["clouds"]["all"],
                "point_in_time": data["dt"]
            }

            message = "Successfully transformed JSON API response into dictionary"
            logging.info("Log: {}".format(message))
            print("Print: {}".format(message))

            return transformed_weather

        except Exception as e:
            logger.error("Log: EXCEPTION transformation error: {}".format(e))
            print("Print: EXCEPTION transformation error: {}".format(e))
            raise

    @task()
    def load_weather_data(transformed_weather):
        try:
            # Load transformed data into Pandas dataframe
            loading_weather_df = pd.DataFrame([transformed_weather])

            # Create a connection to the PostgreSQL database
            engine = create_engine(db_connection)
            message = "Successfully connected to Postgres"
            logging.info("Log: {}".format(message))
            print("Print: {}".format(message))

            # Load the DataFrame into the database
            loading_weather_df.to_sql('hourly_weather_histories', con=engine, if_exists='append', index=False)

        except SQLAlchemyError as e:
            logger.error("Log: EXCEPTION | Error loading to Postgres {}".format(e))
            print("Print: EXCEPTION | Error loading to Postgres : {} ".format(e))
            raise

    open_weather_data = fetch_weather_data()
    transformed_data = transform_weather_data(open_weather_data)
    load_weather_data(transformed_data)
