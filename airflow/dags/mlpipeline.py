from datetime import datetime, timedelta
from dags.utils import HDFSClient
from dotenv import load_dotenv
from airflow import DAG

import a_landing as landing_zone
import b_formatted as formatted_zone
import c_trusted as trusted_zone
import d_explotation as explotation_zone
import e_analysis as analysis_zone

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
    schedule_interval=None,
    catchup=False,
    tags=['ML_DataOps'],
    default_view='graph',
    max_active_tasks=2
) as dag:

    # Ingestion tasks -> HDFS
    ingest_MovieTweetings, ingest_IMDb, ingest_TMDb = landing_zone.create_tasks(dag, hdfs_client)

    # Formatting tasks -> HDFS
    format_MovieTweetings, format_IMDb, format_TMDb = formatted_zone.create_tasks(dag, hdfs_client)

    # Trusted tasks -> HDFS
    trusted_MovieTweetings, trusted_IMDb, trusted_TMDb = trusted_zone.create_tasks(dag, hdfs_client)

    # Explotation tasks -> HDFS
    IMDB_crew_task,IMDB_name_task,IMDB_title_task,TMDB_task,MT_movies_task,MT_ratings_task = explotation_zone.create_tasks(dag)

    # Analysis tasks
    pattern_matching_task = analysis_zone.create_tasks(dag)

    [ingest_MovieTweetings, ingest_IMDb] >> ingest_TMDb

    # Process all data ONLY after final ingestion completes
    ingest_TMDb >> [format_MovieTweetings, format_IMDb, format_TMDb]

    # Independent processing chains per data source
    format_MovieTweetings >> trusted_MovieTweetings >> [MT_ratings_task,MT_movies_task]
    format_IMDb >> trusted_IMDb >> [IMDB_title_task,IMDB_crew_task,IMDB_name_task]
    format_TMDb >> trusted_TMDb >> [TMDB_task]

    # Add pattern matching task after all data processing is complete
    [MT_ratings_task, MT_movies_task, IMDB_title_task, IMDB_crew_task, IMDB_name_task, TMDB_task] >> pattern_matching_task
