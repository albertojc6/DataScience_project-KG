from datetime import datetime, timedelta
from dotenv import load_dotenv
from airflow import DAG

import a_landing as landing_zone
import b_formatted as formatted_zone

load_dotenv(dotenv_path='/opt/airflow/.env')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=30),
}

with DAG(
    'ML_DataOps',
    default_args=default_args,
    schedule_interval='@weekly',
    catchup=False,
    tags=['ML_DataOps'],
    default_view='graph',
) as dag:

    # Ingestion tasks -> HDFS
    ingest_MovieTweetings, ingest_IMDb, ingest_TMDb = landing_zone.create_tasks(dag)

    # Formatting tasks -> PostgreSQL
    format_MovieTweetings, format_IMDB, format_TMDB = formatted_zone.create_tasks(dag)

    [ingest_MovieTweetings, ingest_IMDb] >> ingest_TMDb >> [format_MovieTweetings, format_IMDB, format_TMDB]