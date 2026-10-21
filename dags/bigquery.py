from __future__ import annotations

from datetime import datetime
from airflow.sdk import DAG, task
import requests

from google.cloud import bigquery

import logging

client = bigquery.Client()

with DAG(dag_id="bigquery", schedule="0 8 * * *") as dag:
    @task(task_id="get_api_data", do_xcom_push=True, multiple_outputs=True)
    def get_api_data(**context):
        api_key = "f57cb202e5fcba5861f3fdc285440369"

        lat = 51.418460
        lon = -0.761565

        link = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}"

        response = requests.get(link, json=True).json()

        weather = response.get('weather')[0].get('description')
        feels_like = response.get('main').get('feels_like') - 273.15

        return {'weather': weather, 'feels_like': feels_like}

    @task(task_id="add_to_database")
    def add_to_database(**context):
        weather = context["ti"].xcom_pull(task_ids="get_api_data", key="weather")  # to pull key1
        feels_like = context["ti"].xcom_pull(task_ids="get_api_data", key="feels_like")  # to pull key2

        logging.info(f"weather: {weather}, feels like: {feels_like} to be added to database")

        query = (f"INSERT INTO MyWeather.Weather VALUES('{weather}', {feels_like}, '{datetime.now()}')")
        query_job = client.query(query)
        result = query_job.result()
        logging.info(result)

    get_api_data() >> add_to_database()