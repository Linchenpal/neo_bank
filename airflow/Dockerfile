# Start from the official Airflow image
FROM apache/airflow:2.8.1

# Switch to airflow user for installing python packages
USER airflow

# Install dbt-core and dbt-bigquery
RUN pip install --no-cache-dir dbt-core==1.8.7 dbt-bigquery==1.8.2
