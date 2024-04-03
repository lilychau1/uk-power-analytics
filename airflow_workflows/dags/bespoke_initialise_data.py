"""
Ingest initial data as the basis of analysis
- Actual power generation per generation unit data from a specified duration prior to current date (JSON to parquet)
- Latest production capacity data per BM Unit (JSON to parquet)
- Latest Power Plant Locations (CSV)
- Power Plant ID Dictionary (CSV)
- Fuel category mapping (CSV)
"""
from datetime import datetime, timedelta
import os
import sys

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

current_file_path = os.path.abspath(__file__)
parent_directory = os.path.dirname(os.path.dirname(current_file_path))
sys.path.append(parent_directory)

from common.file_config import *
from common.bq_queries import *
from gcp_operations import upload_multiple_files_to_gcs
from ingest_bmrs_data import fetch_bmrs_data


# Configure Airflow
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.today(),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define workflow name, schedule and start date
local_workflow = DAG(
    'initialise_data2',
    schedule_interval='@once', # Only running once as initialisation
    start_date=datetime.today()
)

with local_workflow:
    # TODO: check existing and fetch anything after latest data if already exists
    
    fetch_bmrs_generation_task = PythonOperator(
        task_id='fetch_bmrs_generation',
        python_callable=fetch_bmrs_data,
        op_kwargs=dict(
            url_template=bmrs_generation_url_template, 
            request_from_datetime=datetime(2024, 2, 1),
            request_to_datetime=datetime(2024, 2, 1, 23, 59, 59), 
            json_raw_output_path=initialise_bmrs_generation_json_raw_output_filepath
        )
    )
    
    spark_bmrs_generation_job = SparkSubmitOperator(
        task_id="spark_transform_bmrs_generation",
        application="/opt/airflow/jobs/pyspark_transform_bmrs_generation.py", # Spark application path created in airflow and spark cluster
        name="spark-transform-bmrs-generation",
        conn_id="spark-conn",
        # total_executor_cores=4,
        # executor_cores=2,
        # executor_memory='10g',
    )
    
    bespoke_gcs_files = [
        {
            'table_name': bmrs_generation_table_name,
            'local_file': initialise_bmrs_generation_output_filepath,
            'partition_cols': bmrs_generation_partition_cols
        }
    ]
    
    local_to_gcs_task = PythonOperator(
        task_id='local_to_gcs_task',
        python_callable=upload_multiple_files_to_gcs,
        op_kwargs=dict(
            gcs_bucket=GCS_BUCKET,
            upload_files_config_list=bespoke_gcs_files,
        )
    )

        
fetch_bmrs_generation_task >>  \
spark_bmrs_generation_job >>  \
local_to_gcs_task
