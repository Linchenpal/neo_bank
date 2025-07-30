from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess

# ---- DAG configuration ----
with DAG(
    dag_id="generate_synthetic_data_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",  # Run daily (can be manual too)
    catchup=False,
    tags=["synthetic_data"]
) as dag:

    # Task: Call external script to generate and upload data
    def run_synthetic_script():
        """
        This function calls the Python script in /scripts to:
        - Read base raw data from bucket
        - Generate monthly synthetic data for 2019-2020
        - Save back to the bucket
        """
        subprocess.run(["python3", "/opt/airflow/scripts/synthetic_data_generator.py"], check=True)

    generate_task = PythonOperator(
        task_id="generate_synthetic_data",
        python_callable=run_synthetic_script
    )

    generate_task
