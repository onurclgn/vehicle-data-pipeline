
import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from datetime import datetime

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq


# Set constants for the URL prefix and suffix
URL_PREFIX="https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_"
URL_SUFFIX="{{execution_date.strftime(\'%Y-%m\')}}"
URL_TEMPLATE=URL_PREFIX+URL_SUFFIX

## Get environment variables
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
PROJECT_ID = os.environ.get("GCLOUD_PROJECT")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'vehicle_data_all')

## Uploads a local file to Google Cloud Storage
def upload_to_gcs(bucket_name, object_name, local_file):

    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)




## Define DAG
with DAG(
    'VehicleDag',
    description='Vehicle',
    schedule_interval="@monthly",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2019, 12, 1),
    tags=['VehicleAnalysis'],
    catchup=True,
    max_active_runs=3

) as local_workflow:

    ## Download data from URL and store it locally
    wget_task = BashOperator(
        task_id="wget_task",
        bash_command=f'wget {URL_TEMPLATE}.parquet -O {path_to_local_home}/vehicleP/{URL_SUFFIX}.parquet',
    )

    ## Upload data to GCS
    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket_name": BUCKET,
            "object_name": f"raw/{URL_SUFFIX}.parquet",
            "local_file": f"{path_to_local_home}/vehicleP/{URL_SUFFIX}.parquet",
        },
    )

    ## Create a empty BigQuery dataset if does not already exist
    create_dataset = BigQueryCreateEmptyDatasetOperator(
            task_id="create_dataset", 
            dataset_id=BIGQUERY_DATASET,
            exists_ok=False)
    

    ## Create an external BigQuery table that reads from uploaded data
    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": f"{URL_SUFFIX}",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/{URL_SUFFIX}.parquet"],
            },
        },
    )

    ## Send a notification that says process has finished.
    slack = SlackAPIPostOperator(
        task_id="post_hello",
        token="YOUR-SLACK-API-TOKEN",
        text=f"The vehicle data for {{execution_date.strftime('%Y-%m')}} has been loaded and processed. It is now available for use. ",
        channel="#general",
        username="airflow"
)

    wget_task >> local_to_gcs_task >> create_dataset >> bigquery_external_table_task >>slack