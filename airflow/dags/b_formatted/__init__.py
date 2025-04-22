from airflow.operators.python import PythonOperator # type:ignore
from dags.utils.postgres_utils import PostgresManager
from .air_quality_FR import format_air_quality


# Initialize Postgres Manager
postgres_manager = PostgresManager()

def create_tasks(dag):

    format_air_task = PythonOperator(
        task_id='format_air_quality',
        python_callable=format_air_quality,
        op_kwargs={
            'postgres_manager': postgres_manager},
        dag=dag
    )

    return format_air_task