from airflow.operators.python import PythonOperator #type:ignore

from .initialize_graphdb import initialize_graphDB

def create_tasks(dag):

    initialize_graphDB_task = PythonOperator(
        task_id='trusted_TMDb',
        python_callable=initialize_graphDB(),
        dag=dag
    )

    return initialize_graphDB_task