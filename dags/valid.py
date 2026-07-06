from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.exceptions import AirflowException


def validate_sales_csv(bucket_name, object_name):
    hook = GCSHook()

    hook.download(
        bucket_name=bucket_name,
        object_name=object_name,
        filename="/tmp/sales{{ ds_nodash }}.csv",
    )

    with open("/tmp/sales{{ ds_nodash }}.csv", "r") as file:
        content = file.readlines()

    if not content:
        raise AirflowException("File is empty")

    header = content[0].strip()
    expected_header = "order_id,customer_name,product,quantity,price"

    if header != expected_header:
        raise AirflowException(
            f"Invalid header. Expected '{expected_header}', but found '{header}'."
        )

    row_count = 0

    for row in content[1:]:
        if not row.strip():
            continue

        columns = row.strip().split(",")

        if len(columns) != 5:
            raise AirflowException(
                f"Invalid row: {row.strip()}. Expected 5 columns but found {len(columns)}."
            )

        row_count += 1

    return row_count