from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1
}

with DAG(
    dag_id="employee_data_pipeline_v2",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    generate_data = BashOperator(
        task_id="generate_data",
        bash_command='export PYTHONPATH=$HOME/.local/lib/python3.8/site-packages && python3 /opt/airflow/dags/scripts/generate_data.py'    )

    load_delta = BashOperator(
        task_id="load_delta",
        bash_command='echo "Simulating load_delta step"'
    )

    transform = BashOperator(
        task_id="transform",
        bash_command='echo "Simulating transform step"'
    )

    load_scylla = BashOperator(
        task_id="load_scylla",
        bash_command='echo "Simulating load_scylla step"'
    )

    generate_data >> load_delta >> transform >> load_scylla