from airflow.operators.python import PythonOperator # type:ignore
from dags.utils.postgres_utils import PostgresManager
from .air_quality_QL import quality_air


# Initialize Postgres Manager
postgres_manager = PostgresManager()

def create_tasks(dag):

    quality_air_task = PythonOperator(
        task_id='quality_air',
        python_callable=quality_air,
        op_kwargs={
            'postgres_manager': postgres_manager},
        dag=dag
    )

    return quality_air_task