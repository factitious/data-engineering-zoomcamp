import os
import logging
from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

DATASET_FILE = "fhv_tripdata_{{execution_date.strftime(\'%Y-%m\')}}.parquet"
DATASET_URL = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{DATASET_FILE}"
PATH_TO_LOCAL_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trip_data_fhv')
LOCAL_FILE_TEMPLATE = f"{PATH_TO_LOCAL_HOME}/{DATASET_FILE}"
GCS_FILE_TEMPLATE = f"raw/{DATASET_FILE}"
TABLE_ID = "fhv_{{execution_date.strftime(\'%Y-%m\')}}"
SOURCE_URI=[f"gs://{BUCKET}/raw/{DATASET_FILE}"]

def upload_to_gcs(bucket, object_name, local_file):

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

default_args = {
    "owner": "airflow",
    "start_date": datetime(2019, 1, 1),
    # "end_date": datetime(2021, 2, 1),
    "depends_on_past": False,
    "retries": 1,
}

gcs_workflow = DAG(
    dag_id='fhv_tripdata_ingestion_dag',
    schedule_interval='0 6 2 * *',
    default_args=default_args,
    max_active_runs=3,
    tags=['dtc-de'],
)

with gcs_workflow:

    curl_task = BashOperator(
        task_id='curl',
        bash_command=f"curl -sSLf {DATASET_URL} > {LOCAL_FILE_TEMPLATE}"
    )

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
        task_id='bigquery_external_table_task',
        table_resource={
            "tableReference":{
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": TABLE_ID,
            },
            "externalDataConfiguration":{
                "sourceFormat": "PARQUET",
                "sourceUris": SOURCE_URI,
            },
        },
    )

    remove_dataset_locally = BashOperator(
        task_id='remove_dataset_locally',
        bash_command=f"rm {LOCAL_FILE_TEMPLATE}",
    )

    curl_task >> local_to_gcs_task >> bigquery_external_table_task >> remove_dataset_locally