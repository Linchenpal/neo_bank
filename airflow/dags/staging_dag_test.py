from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Default args
default_args = {
    "start_date": datetime(2023, 1, 1),
    "catchup": False
}

# Define DAG
with DAG(
    dag_id="staging_dag_test",
    schedule_interval=None,  # Run manually
    default_args=default_args,
    tags=["dbt", "staging", "test"]
) as dag:

    # Run dbt for each test staging model
    run_stg_users = BashOperator(
        task_id="run_stg_users_test",
        bash_command="cd /opt/airflow/dbt/dbt_neobank && dbt run --select stg_users_test"
    )

    run_stg_devices = BashOperator(
        task_id="run_stg_devices_test",
        bash_command="cd /opt/airflow/dbt/dbt_neobank && dbt run --select stg_devices_test"
    )

    run_stg_transactions = BashOperator(
        task_id="run_stg_transactions_test",
        bash_command="cd /opt/airflow/dbt/dbt_neobank && dbt run --select stg_transactions_test"
    )

    run_stg_notifications = BashOperator(
        task_id="run_stg_notifications_test",
        bash_command="cd /opt/airflow/dbt/dbt_neobank && dbt run --select stg_notifications_test"
    )

    # ✅ Sequential execution
    run_stg_users >> run_stg_devices >> run_stg_transactions >> run_stg_notifications
