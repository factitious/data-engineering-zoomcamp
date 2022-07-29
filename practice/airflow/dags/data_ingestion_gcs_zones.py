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

# DATASET_URL = f"https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.parquet"
PATH_TO_LOCAL_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'zones')
LOCAL_CSV_TEMPLATE = f"{PATH_TO_LOCAL_HOME}/taxi_zone_lookup.csv"
LOCAL_PARQUET_TEMPLATE = f"{PATH_TO_LOCAL_HOME}/taxi_zone_lookup.parquet"
GCS_FILE_TEMPLATE = f"raw/taxi_zone_lookup.parquet"

TABLE_ID = "taxi_zone_lookup"
SOURCE_URI=[f"gs://{BUCKET}/raw/taxi_zone_lookup.parquet"]

def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))

def upload_to_gcs(bucket, object_name, local_file):

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

gcs_workflow = DAG(
    dag_id='zone_lookup_ingestion_dag',
    schedule_interval='@once',
    default_args=default_args,
    max_active_runs=3,
    tags=['dtc-de'],
)

with gcs_workflow:

    parquetize_task = PythonOperator(
        task_id="parquetize_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{LOCAL_CSV_TEMPLATE}",
        },
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": GCS_FILE_TEMPLATE,
            "local_file": LOCAL_PARQUET_TEMPLATE,
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
        bash_command=f"rm {LOCAL_CSV_TEMPLATE} && rm {LOCAL_PARQUET_TEMPLATE}",
    )

    parquetize_task >> local_to_gcs_task >> bigquery_external_table_task >> remove_dataset_locally