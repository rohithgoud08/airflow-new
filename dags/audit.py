from datetime import datetime, timezone
from uuid import uuid4

from airflow.exceptions import AirflowException
from google.cloud import bigquery


def log_audit_success(
    project_id,
    dataset_id,
    table_id,
    file_name,
    validation_task_id,
    **context,
):
    ti = context["ti"]
    dag = context["dag"]
    dag_run = context["dag_run"]

    row_count = ti.xcom_pull(task_ids=validation_task_id)

    if row_count is None:
        row_count = 0

    audit_id = str(uuid4())
    dag_id = dag.dag_id
    run_id = context["run_id"]

    start_time = dag_run.start_date
    if start_time is None:
        start_time = datetime.now(timezone.utc)

    end_time = datetime.now(timezone.utc)

    dag_link = ti.log_url

    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    row = {
        "audit_id": audit_id,
        "dag_id": dag_id,
        "run_id": run_id,
        "file_name": file_name,
        "status": "SUCCESS",
        "row_count": row_count,
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "error_message": None,
        "dag_link": dag_link,
    }

    client = bigquery.Client(project=project_id)

    errors = client.insert_rows_json(table_ref, [row])

    if errors:
        raise AirflowException(f"Failed to insert audit record: {errors}")

    print(f"Audit record inserted successfully for file: {file_name}")

def log_audit_failure(
    project_id,
    dataset_id,
    table_id,
    file_name,
    validation_task_id,
    **context,
):
    ti = context["ti"]
    dag = context["dag"]
    dag_run = context["dag_run"]

    row_count = ti.xcom_pull(task_ids=validation_task_id)

    if row_count is None:
        row_count = 0

    audit_id = str(uuid4())
    dag_id = dag.dag_id
    run_id = context["run_id"]

    start_time = dag_run.start_date
    if start_time is None:
        start_time = datetime.now(timezone.utc)

    end_time = datetime.now(timezone.utc)

    dag_link = ti.log_url

    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    row = {
        "audit_id": audit_id,
        "dag_id": dag_id,
        "run_id": run_id,
        "file_name": file_name,
        "status": "FAILED",
        "row_count": row_count,
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "error_message": "Pipeline failed. Check Airflow task logs for details.",
        "dag_link": dag_link,
    }

    client = bigquery.Client(project=project_id)

    errors = client.insert_rows_json(table_ref, [row])

    if errors:
        raise AirflowException(f"Failed to insert failure audit record: {errors}")

    print(f"Failure audit record inserted for file: {file_name}")