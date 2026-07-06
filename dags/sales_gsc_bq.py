from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

from valid import validate_sales_csv
from audit import log_audit_success


with DAG(
    dag_id="sales_gcs_bq_daily",
    start_date=datetime(2026, 1, 1),
    description="Sales ETL deployed using Github actions",
    schedule="0 20 * * *",
    catchup=False,
    tags=["gcp", "gcs", "bigquery"],
) as dag:

    wait_for_file = GCSObjectExistenceSensor(
        task_id="wait_for_file",
        bucket="airflow-gcp-learning-rohith",
        object="inbound/sales{{ ds_nodash }}.csv",
        poke_interval=60,
        timeout=300,
        mode="reschedule",
    )

    validate = PythonOperator(
        task_id="validate_file",
        python_callable=validate_sales_csv,
        op_kwargs={
            "bucket_name": "airflow-gcp-learning-rohith",
            "object_name": "inbound/sales{{ ds_nodash }}.csv",
        },
    )

    load_to_bq = GCSToBigQueryOperator(
        task_id="load_to_bigquery",
        bucket="airflow-gcp-learning-rohith",
        source_objects=["inbound/sales{{ ds_nodash }}.csv"],
        destination_project_dataset_table="studied-limiter-501113-k0.sales.sales_details",
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )

    move_to_archive = GCSToGCSOperator(
        task_id="move_to_archive",
        source_bucket="airflow-gcp-learning-rohith",
        source_object="inbound/sales{{ ds_nodash }}.csv",
        destination_bucket="airflow-gcp-learning-rohith",
        destination_object="archive/sales{{ ds_nodash }}.csv",
        move_object=True,
    )

    log_audit = PythonOperator(
        task_id="log_audit_success",
        python_callable=log_audit_success,
        op_kwargs={
            "project_id": "studied-limiter-501113-k0",
            "dataset_id": "employee",
            "table_id": "etl_audit",
            "file_name": "sales{{ ds_nodash }}.csv",
            "validation_task_id": "validate_file",
        },
    )

    wait_for_file >> validate >> load_to_bq >> move_to_archive >> log_audit