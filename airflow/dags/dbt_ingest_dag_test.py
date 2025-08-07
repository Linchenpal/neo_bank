from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime

# ---- Config ----
BUCKET_NAME = "neobank-raw-export"

# ✅ Test Dataset
DATASET = "test_synthetic"

# ✅ Mapping between file names and test tables
FILE_TABLE_MAP = {
    "raw_users_2019_02.csv": "raw_users_test",
    "raw_transactions_2019_02.csv": "raw_transactions_test",
    "raw_devices_2019_02.csv": "raw_devices_test",
    "raw_notifications_2019_02.csv": "raw_notifications_test"
}

default_args = {
    "start_date": datetime(2023, 1, 1),
    "catchup": False
}

with DAG(
    dag_id="dbt_ingest_dag_test",
    schedule_interval=None,  # manual trigger only
    default_args=default_args,
    tags=["gcs_ingestion", "prod"]
) as dag:

    load_tasks = []  # list to store all tasks

    for file_name, table_name in FILE_TABLE_MAP.items():
        task = GCSToBigQueryOperator(
            task_id=f"load_{table_name}",
            bucket=BUCKET_NAME,
            source_objects=[f"synthetic_data/{file_name}"],
            destination_project_dataset_table=f"sacred-choir-466017-s9.{DATASET}.{table_name}",
            source_format="CSV",
            skip_leading_rows=1,
            write_disposition="WRITE_APPEND",  # ✅ Append instead of overwrite
            autodetect=True,
            field_delimiter=","  # ✅ safer for CSV files
        )
        load_tasks.append(task)

    # ✅ Run tasks in sequence (optional safety step)
    for i in range(1, len(load_tasks)):
        load_tasks[i] << load_tasks[i-1]
