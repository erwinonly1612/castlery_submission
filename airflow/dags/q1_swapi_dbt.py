from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from datetime import datetime
default_args ={
    'owner' : 'airflow'
}

with DAG(
    dag_id = f'swapi_dbt_dag',
    default_args = default_args,
    description = f'Data pipeline to create data warehouse from swapi staging datasets',
    schedule_interval="@once",
    start_date=datetime(2022,6,5),
    catchup=False,
    tags=['swapi_dbt_dag']
) as dag:

    initate_dbt_task = BashOperator(
        task_id = 'dbt_swapi_initiate',
        bash_command = 'cd /dbt && dbt deps --profiles-dir . --target prod'
    )

    execute_dbt_task = BashOperator(
        task_id = 'dbt_swapi_run',
        bash_command = 'cd /dbt && dbt deps && dbt run --profiles-dir . --target prod'
    )

    initate_dbt_task >> \
    execute_dbt_task