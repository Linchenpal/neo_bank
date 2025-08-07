from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "start_date": datetime(2023, 1, 1),
    "catchup": False
}

with DAG(
    dag_id="dbt_staging_dag_test",
    schedule_interval=None,
    default_args=default_args,
    tags=["dbt", "staging", "test"]
) as dag:

    run_stg_users = BashOperator(
        task_id="run_stg_users",
        bash_command="cd /opt/airflow/dbt/dbt_neobank && dbt run --target test --select stg_users"
    )

    run_stg_devices = BashOperator(
        task_id="run_stg_devices",
        bash_command="cd /opt/airflow/dbt/dbt_neobank && dbt run --target test --select stg_devices"
    )

    run_stg_transactions = BashOperator(
        task_id="run_stg_transactions",
        bash_command="cd /opt/airflow/dbt/dbt_neobank && dbt run --target test --select stg_transactions"
    )

    run_stg_notifications = BashOperator(
        task_id="run_stg_notifications",
        bash_command="cd /opt/airflow/dbt/dbt_neobank && dbt run --target test --select stg_notifications"
    )

    run_stg_users >> run_stg_devices >> run_stg_transactions >> run_stg_notifications
