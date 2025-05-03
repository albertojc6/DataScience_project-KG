from datetime import datetime, timedelta
from dotenv import load_dotenv
from airflow import DAG

import a_landing as landing_zone
import b_formatted as formatted_zone
import c_trusted as trusted_zone

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

    # Trusted tasks -> PostgreSQL
    trusted_MovieTweetings, trusted_IMDb, trusted_TMDb = trusted_zone.create_tasks(dag)

    [ingest_MovieTweetings, ingest_IMDb] >> ingest_TMDb >> format_IMDB

    
    format_IMDB >> format_MovieTweetings >> format_TMDB >> trusted_IMDb >> trusted_MovieTweetings >> trusted_TMDb