from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from google.cloud import storage
import os

def list_gcs_files():
    project_id = os.getenv("GCP_PROJECT_ID")
    bucket_name = os.getenv("RAW_BUCKET")
    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name)
    files = [blob.name for blob in bucket.list_blobs()]
    print("📂 Files in GCS bucket:", files)
    return files

with DAG(
    dag_id="test_gcs_connection",
    start_date=datetime.now() - timedelta(days=1),  # safe start date
    schedule_interval=None,
    catchup=False,
    tags=["test"]
) as dag:
    PythonOperator(
        task_id="list_gcs_files",
        python_callable=list_gcs_files
    )
