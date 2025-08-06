from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 1),
    'retries': 1,
}

with DAG(
    dag_id='dbt_vault_test_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['dbt', 'vault', 'test']
) as dag:

    # Run all vault test models (hubs, links, satellites)
    run_dbt_vault_test = BashOperator(
        task_id='run_dbt_vault_test',
        bash_command="""
        cd /opt/airflow/dbt &&
        dbt run --select data_vault_test
        """,
    )

    # Optionally, test them
    test_dbt_vault_test = BashOperator(
        task_id='test_dbt_vault_test',
        bash_command="""
        cd /opt/airflow/dbt &&
        dbt test --select data_vault_test
        """,
    )

    run_dbt_vault_test >> test_dbt_vault_test
