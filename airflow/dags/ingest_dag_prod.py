from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime

# ---- Config ----
BUCKET_NAME = "neobank-raw-export"

# ✅ Production Dataset
DATASET = "test_synthetic"

# Mapping between file names and real production tables
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
    dag_id="ingest_dag_prod",
    schedule_interval=None,  # manual trigger only
    default_args=default_args,
    tags=["gcs_ingestion", "prod"]
) as dag:

    for file_name, table_name in FILE_TABLE_MAP.items():
        GCSToBigQueryOperator(
            task_id=f"load_{table_name}",
            bucket=BUCKET_NAME,
            source_objects=[f"synthetic_data/{file_name}"],
            destination_project_dataset_table=f"sacred-choir-466017-s9.{DATASET}.{table_name}",
            source_format="CSV",
            skip_leading_rows=1,
            write_disposition="WRITE_APPEND",  # ✅ Append mode to avoid overwriting data
            autodetect=True
        )
