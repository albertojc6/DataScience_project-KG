from datetime import datetime, timedelta
from dags.utils import HDFSClient
from dotenv import load_dotenv
from airflow import DAG

import a_landing as landing_zone
import b_formatted as formatted_zone
import c_trusted as trusted_zone
import d_explotation as explotation_zone

# Set environment variables
load_dotenv(dotenv_path='/opt/airflow/.env')

# Initializes shared HDFS Client
hdfs_client = HDFSClient()

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(seconds=10),
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
    ingest_MovieTweetings, ingest_IMDb, ingest_TMDb = landing_zone.create_tasks(dag, hdfs_client)

    # Formatting tasks -> HDFS
    format_MovieTweetings, format_IMDb, format_TMDb = formatted_zone.create_tasks(dag, hdfs_client)

    # Trusted tasks -> HDFS
    trusted_MovieTweetings, trusted_IMDb, trusted_TMDb = trusted_zone.create_tasks(dag, hdfs_client)

    [ingest_MovieTweetings, ingest_IMDb] >> ingest_TMDb

    # Process all data ONLY after final ingestion completes
    ingest_TMDb >> [format_MovieTweetings, format_IMDb, format_TMDb]

    # Independent processing chains per data source
    format_MovieTweetings >> trusted_MovieTweetings
    format_IMDb >> trusted_IMDb
    format_TMDb >> trusted_TMDb

    # MARTHA
    # [ingest_MovieTweetings, ingest_IMDb] >> ingest_TMDb
    # ingest_TMDb >> format_MovieTweetings >> format_IMDb >> format_TMDb >> trusted_MovieTweetings >> trusted_IMDb >> trusted_TMDb