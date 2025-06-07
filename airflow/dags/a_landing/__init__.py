from airflow.operators.python import PythonOperator # type:ignore
from .MovieTweetings_DL import load_MovieTweetings
from dags.utils import HDFSClient
from .IMDb_DL import load_IMDb
from .TMDb_DL import load_TMDb
from airflow import DAG

def create_tasks(dag: DAG, hdfs_client: HDFSClient):

    ingest_MovieTweetings = PythonOperator(
        task_id='ingest_MovieTweetings',
        python_callable=load_MovieTweetings,
        op_kwargs={
            'hdfs_client': hdfs_client
        },
        dag=dag
    )

    ingest_IMDb = PythonOperator(
        task_id='ingest_IMDb',
        python_callable=load_IMDb,
        op_kwargs={
            'hdfs_client': hdfs_client,
            'use_local': True
        },
        dag=dag
    )

    ingest_TMDb = PythonOperator(
        task_id='ingest_TMDb',
        python_callable=load_TMDb,
        op_kwargs={
            'hdfs_client': hdfs_client,
            'use_local': True
        },
        dag=dag
    )

    return ingest_MovieTweetings, ingest_IMDb, ingest_TMDb