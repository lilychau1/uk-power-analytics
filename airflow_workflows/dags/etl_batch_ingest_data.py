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
import re
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator

from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

current_file_path = os.path.abspath(__file__)
parent_directory = os.path.dirname(os.path.dirname(current_file_path))
sys.path.append(parent_directory)

from common.file_config import *
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
    'ingest_batch_data',
    schedule_interval='0 5 * * *', # Daily 5AM
    start_date=datetime.today()
)

with local_workflow:
    
    gcs_generation_files = GCSListObjectsOperator(
        task_id="gcs_generation_files",
        bucket="power-analytics",
        prefix="bmrs_generation/",
        match_glob="*/**/settlement_date=*/*",
    )
    
    fetch_bmrs_generation_task = PythonOperator(
        task_id='fetch_bmrs_generation',
        python_callable=fetch_bmrs_data,
        op_kwargs=dict(
            url_template=bmrs_generation_url_template, 
            request_from_datetime=batch_bmrs_generation_from_datetime,
            request_to_datetime=batch_bmrs_generation_to_datetime, 
            json_raw_output_path=batch_bmrs_generation_json_raw_output_filepath
        ), 
        provide_context=True,
    ) 

    fetch_bmrs_capacity_task = PythonOperator(
        task_id='fetch_bmrs_capacity',
        python_callable=fetch_bmrs_data,
        op_kwargs=dict(
            url_template=bmrs_capacity_url_template, 
            request_from_datetime=batch_bmrs_capacity_from_datetime,
            request_to_datetime=batch_bmrs_capacity_to_datetime, 
            json_raw_output_path=batch_bmrs_capacity_json_raw_output_filepath
        )
    ) 
    
    spark_bmrs_generation_job = SparkSubmitOperator(
        task_id="spark_transform_bmrs_generation",
        application="/opt/airflow/jobs/pyspark_batch_transform_bmrs_generation.py", # Spark application path created in airflow and spark cluster
        name="spark-transform-bmrs-generation",
        conn_id="spark-conn",
    )
        
    spark_bmrs_capacity_job = SparkSubmitOperator(
        task_id="spark_transform_bmrs_capacity",
        application="/opt/airflow/jobs/pyspark_batch_transform_bmrs_capacity.py", # Spark application path created in airflow and spark cluster
        name="spark-transform-bmrs-capacity",
        conn_id="spark-conn",
    )
    
    local_to_gcs_task = PythonOperator(
        task_id='local_to_gcs_task',
        python_callable=upload_multiple_files_to_gcs,
        op_kwargs=dict(
            gcs_bucket=GCS_BUCKET,
            upload_files_config_list=batch_upload_files_config_list,
        )
    )
    
gcs_generation_files >> fetch_bmrs_generation_task >> fetch_bmrs_capacity_task >> spark_bmrs_generation_job >> spark_bmrs_capacity_job >> local_to_gcs_task
