# Vehicle Analysis DAG

This DAG downloads monthly vehicle data from a cloudfront URL, stores it locally as a parquet file, uploads the file to Google Cloud Storage, and creates an external BigQuery table that reads from the uploaded data. Once the process finishes, it sends a notification to a Slack channel.

## Installation

1. Clone the repository to your local machine.

    ```
    git clone https://github.com/onurclgn/vehicle-data-pipeline.git
    ```

2. Install the required Python packages.

    ```
    pip install -r requirements.txt
    ```

3. Set the following environment variables:

    ```
    AIRFLOW_HOME: The path to your Airflow home directory.
    GCLOUD_PROJECT: Your Google Cloud project ID.
    GCP_GCS_BUCKET: The name of the Google Cloud Storage bucket you want to use.
    BIGQUERY_DATASET (optional): The name of the BigQuery dataset you want to create. If not set, it defaults to 'vehicle_data_all'.
    ```

## Usage

1. Start the Airflow web server and scheduler.

    ```
    airflow webserver --port 8080
    airflow scheduler
    ```

2. Open the Airflow web interface in your browser (http://localhost:8080 by default) and turn on the 'VehicleDag' DAG.

3. Once the DAG is running, it will automatically download, process, and upload the vehicle data every month.

## Configuration

You can modify the following parameters in the 'VehicleDag' DAG:

- URL_PREFIX and URL_SUFFIX: The URL prefix and suffix for the vehicle data download.
- schedule_interval: The frequency at which the DAG runs. Defaults to "@monthly".
- start_date and end_date: The start and end dates for the DAG. Defaults to January 1, 2019, and December 1, 2019, respectively.
- tags: The tags for the DAG.
- catchup: Whether to backfill the DAG for the time range between the start and end dates. Defaults to True.
- max_active_runs: The maximum number of active DAG runs at any given time. Defaults to 3.

You can also modify the `upload_to_gcs` function in the DAG file to change the Google Cloud Storage upload behavior.
