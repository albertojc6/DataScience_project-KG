from airflow.operators.python import PythonOperator # type:ignore
from dags.utils.postgres_utils import PostgresManager

from .MovieTweetings_FR import format_MovieTweetings
from .IMDb_FR import format_IMDb
from .TMDb_FR import format_TMDb

# Initialize Postgres Manager
postgres_manager = PostgresManager()

def create_tasks(dag):

    format_MovieTweeting = PythonOperator(
        task_id='format_MovieTweetings',
        python_callable=format_MovieTweetings,
        op_kwargs={
            'postgres_manager': postgres_manager
        },
        dag=dag
    )

    format_IMDB = PythonOperator(
        task_id='format_IMDb',
        python_callable=format_IMDb,
        op_kwargs={
            'postgres_manager': postgres_manager
        },
        dag=dag
    )

    format_TMDB = PythonOperator(
        task_id='format_TMDb',
        python_callable=format_TMDb,
        op_kwargs={
            'postgres_manager': postgres_manager
        },
        dag=dag
    )

    return format_MovieTweeting, format_IMDB, format_TMDB