from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "start_date": datetime(2023, 1, 1),
    "catchup": False,
}

with DAG(
    dag_id="dbt_vault_dag_test",
    schedule_interval=None,
    default_args=default_args,
    tags=["dbt", "vault", "test"],
) as dag:

    run_hubs = BashOperator(
        task_id="run_hubs",
        bash_command="cd /opt/airflow/dbt/dbt_neobank && dbt run --target test_vault --select data_vault/hubs",
    )

    run_links = BashOperator(
        task_id="run_links",
        bash_command="cd /opt/airflow/dbt/dbt_neobank && dbt run --target test_vault --select data_vault/links",
    )

    run_satellites = BashOperator(
        task_id="run_satellites",
        bash_command="cd /opt/airflow/dbt/dbt_neobank && dbt run --target test_vault --select data_vault/satellites",
    )

    # Execution order (optional)
    run_hubs >> run_links >> run_satellites
