import os
import logging
from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.parquet as pq

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

DATASET_FILE = "green_tripdata_{{execution_date.strftime(\'%Y-%m\')}}.parquet"
DATASET_URL = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{DATASET_FILE}"
PATH_TO_LOCAL_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trip_data_green')
LOCAL_FILE_TEMPLATE = f"{PATH_TO_LOCAL_HOME}/{DATASET_FILE}"
GCS_FILE_TEMPLATE=f"raw/{DATASET_FILE}"
TABLE_ID="green_{{execution_date.strftime(\'%Y-%m\')}}"
SOURCE_URI=[f"gs://{BUCKET}/raw/{DATASET_FILE}"]

# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    "start_date": datetime(2019,1,1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="green_tripdata_ingestion_dag",
    schedule_interval="0 6 2 * *",
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['dtc-de'],
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sSLf {DATASET_URL} > {LOCAL_FILE_TEMPLATE}"
    )

    # TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": GCS_FILE_TEMPLATE,
            "local_file": LOCAL_FILE_TEMPLATE,
        },
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": TABLE_ID,
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": SOURCE_URI,
            },
        },
    )

    remove_datasets_local = BashOperator(
        task_id="remove_datasets_local",
        bash_command=f"rm {LOCAL_FILE_TEMPLATE}"
    )

    download_dataset_task  >> local_to_gcs_task >> bigquery_external_table_task >> remove_datasets_local