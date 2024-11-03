import os
import requests
import pandas as pd
from dotenv import load_dotenv
import logging
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from airflow import DAG
import datetime
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
    schedule_interval = "*/30 * * * *",
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
            if response.status_code == 429:
                message = 'WARNING: Rate limit reached!'
                logger.error(message)
                print(message)

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
                "weather_description": data["weather"][0]["description"],
                "temperature": (data["main"]["temp"] - 273.15) * 9/5 + 32,
                "humidity": data["main"]["humidity"],
                "visibility": data["visibility"],
                "wind_speed": data["wind"]["speed"],
                "cloud_percent": data["clouds"]["all"],
                "point_in_time": (datetime.datetime.utcfromtimestamp(data["dt"]) + datetime.timedelta(seconds=data["timezone"])).strftime("%Y-%m-%d %H:%M:%S")#datetime.datetime.fromtimestamp(data["dt"]).strftime("%Y-%m-%d %H:%M:%S")
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
            # load transformed data into Pandas dataframe
            loading_weather_df = pd.DataFrame([transformed_weather])

            # create a connection to the PostgreSQL database
            engine = create_engine(db_connection)
            message = "Successfully connected to Postgres"
            logging.info("Log: {}".format(message))
            print("Print: {}".format(message))

            insert_query = """
            INSERT INTO weather_data_histories (
            latitude, longitude, country, city_id, city_name,
            weather_description, temperature, humidity, visibility, wind_speed,
            cloud_percent, point_in_time
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            # load data into database
            for index, row in loading_weather_df.iterrows():
                engine.execute(insert_query, tuple(row))

            message = "Successfully merged into Postgres"
            logging.info("Log: {}".format(message))
            print("Print: {}".format(message))

        except SQLAlchemyError as e:
            logger.error("Log: EXCEPTION | Error loading to Postgres {}".format(e))
            print("Print: EXCEPTION | Error loading to Postgres : {} ".format(e))
            raise

    open_weather_data = fetch_weather_data()
    transformed_data = transform_weather_data(open_weather_data)
    load_weather_data(transformed_data)
