from airflow.operators.python import PythonOperator # type:ignore
from .air_quality_DL import load_data_air
from dags.utils import HDFSManager
from airflow import DAG


hdfs_manager = HDFSManager()

def create_tasks(dag: DAG, air_start_date: str, air_end_date: str):

    ingest_air_task = PythonOperator(
        task_id='ingest_air',
        python_callable=load_data_air,
        op_kwargs={
            'start_date': air_start_date,
            'end_date': air_end_date,
            'hdfs_manager': hdfs_manager,
        },
        dag=dag
    )

    return ingest_air_task