
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

URL_PREFIX="https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_"
URL_SUFFIX="{{execution_date.strftime(\'%Y-%m\')}}"
URL_TEMPLATE=URL_PREFIX+URL_SUFFIX

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
PROJECT_ID = os.environ.get("GCLOUD_PROJECT")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'vehicle_data_all')

def upload_to_gcs(bucket_name, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


with DAG(
    'VehicleDag01',
    description='Vehicle',
    schedule_interval="@monthly",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2019, 12, 1),
    tags=['VehicleAnalysis'],
    catchup=True,
    max_active_runs=3

) as local_workflow:

    wget_task = BashOperator(
        task_id="wget_task",
        bash_command=f'wget {URL_TEMPLATE}.parquet -O {path_to_local_home}/vehicleP/{URL_SUFFIX}.parquet',
        # env: Optional[Dict[str, str]] = None,
        # output_encoding: str = 'utf-8',
        # skip_exit_code: int = 99,
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket_name": BUCKET,
            "object_name": f"raw/{URL_SUFFIX}.parquet",
            "local_file": f"{path_to_local_home}/vehicleP/{URL_SUFFIX}.parquet",
        },
    )

    create_dataset = BigQueryCreateEmptyDatasetOperator(
            task_id="create_dataset", 
            dataset_id=BIGQUERY_DATASET,
            exists_ok=False)
    

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

    slack = SlackAPIPostOperator(
        task_id="post_hello",
        token="xoxb-3855656145619-4789584537413-pBxppPbJ70TC1V5i9Wu6QRKL",
        text=f"{URL_SUFFIX} yüklendi ve tablolara aktarildi. Kullanima Hazir :)))",
        channel="#general",
        username="airflow"
)

    wget_task >> local_to_gcs_task >> create_dataset >> bigquery_external_table_task >>slack