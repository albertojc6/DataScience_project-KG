from airflow.providers.postgres.operators.postgres import PostgresOperator # type: ignore


def create_tasks(dag):
    
    air_electricity_weather_task = PostgresOperator(
        task_id='view_air_electricity_weather',
        postgres_conn_id='postgres_default',
        sql='exploitation/air_electricity_weather.sql',
        dag=dag,
    )

    return air_electricity_weather_task
