from airflow.operators.python import PythonOperator  
from dags.utils.postgres_utils import PostgresManager

from .MovieTweetings_TR import quality_MovieTweetings
from .IMDb_TR import quality_IMDb
from .TMDb_TR import quality_TMDb

# Initialize Postgres Manager
postgres_manager = PostgresManager()

def create_tasks(dag):

    trusted_MovieTweetings = PythonOperator(
        task_id='trusted_MovieTweetings',
        python_callable=quality_MovieTweetings,
        op_kwargs={
            'postgres_manager': postgres_manager
        },
        dag=dag
    )

    trusted_IMDb = PythonOperator(
        task_id='trusted_IMDb',
        python_callable=quality_IMDb,
        op_kwargs={
            'postgres_manager': postgres_manager
        },
        dag=dag
    )

    trusted_TMDb = PythonOperator(
        task_id='trusted_TMDb',
        python_callable=quality_TMDb,
        op_kwargs={
            'postgres_manager': postgres_manager
        },
        dag=dag
    )

    return trusted_MovieTweetings, trusted_IMDb, trusted_TMDb