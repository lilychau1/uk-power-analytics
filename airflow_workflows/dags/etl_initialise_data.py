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
from ingest_mapping_data import transform_power_plant_id_mapping_callable, transform_power_plant_location_mapping_callable, \
    transform_bmrs_power_plant_info_mapping_callable


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
    'initialise_data',
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
            request_from_datetime=initialise_bmrs_from_datetime,
            request_to_datetime=initialise_bmrs_generation_to_datetime, 
            json_raw_output_path=initialise_bmrs_generation_json_raw_output_filepath
        )
    ) 

    fetch_bmrs_capacity_task = PythonOperator(
        task_id='fetch_bmrs_capacity',
        python_callable=fetch_bmrs_data,
        op_kwargs=dict(
            url_template=bmrs_capacity_url_template, 
            request_from_datetime=initialise_bmrs_capacity_from_datetime,
            request_to_datetime=initialise_bmrs_capacity_to_datetime, 
            json_raw_output_path=initialise_bmrs_capacity_json_raw_output_filepath
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
        
    spark_bmrs_capacity_job = SparkSubmitOperator(
        task_id="spark_transform_bmrs_capacity",
        application="/opt/airflow/jobs/pyspark_transform_bmrs_capacity.py", # Spark application path created in airflow and spark cluster
        name="spark-transform-bmrs-capacity",
        conn_id="spark-conn",
        # total_executor_cores=4,
        # executor_cores=2,
        # executor_memory='10g',
    )
    
    curl_power_plant_mappings_task = BashOperator(
        task_id='curl_power_plant_mappings', 
        bash_command=power_plant_mappings_download_command
    )
    
    transform_power_plant_id_mapping_task = PythonOperator(
        task_id='transform_power_plant_id',
        python_callable=transform_power_plant_id_mapping_callable,
        op_kwargs=dict(
            raw_filepath=initialise_power_plant_id_raw_output_file, 
            output_filepath=initialise_power_plant_id_processed_output_file
        )
    )
    
    transform_power_plant_location_mapping_task = PythonOperator(
        task_id='transform_power_plant_location',
        python_callable=transform_power_plant_location_mapping_callable,
        op_kwargs=dict(
            raw_filepath=initialise_power_plant_location_raw_output_file, 
            output_filepath=initialise_power_plant_location_processed_output_file
        )
    )
    
    transform_bmrs_power_plant_info_task = PythonOperator(
        task_id='transform_bmrs_power_plant_info',
        python_callable=transform_bmrs_power_plant_info_mapping_callable,
        op_kwargs=dict(
            raw_filepath=bmrs_power_plant_info_raw_output_file, 
            output_filepath=bmrs_power_plant_info_processed_output_file
        )
    )    
    
    local_to_gcs_task = PythonOperator(
        task_id='local_to_gcs_task',
        python_callable=upload_multiple_files_to_gcs,
        op_kwargs=dict(
            gcs_bucket=GCS_BUCKET,
            upload_files_config_list=initialise_upload_files_config_list,
        )
    )

    gcs_to_bq_ext_bmrs_generation_task = BigQueryCreateExternalTableOperator(
        task_id='gcs_to_bq_ext_bmrs_generation_task',
        table_resource={
            'tableReference': {
                'projectId': GCS_PROJECT_ID,
                'datasetId': BIGQUERY_DATASET,
                'tableId': bmrs_generation_table_name,
            },
            'externalDataConfiguration': {
                'autodetect': True,
                'sourceFormat': 'PARQUET',
                'sourceUris': [f'gs://{GCS_BUCKET}/{bmrs_generation_table_name}/*.parquet'],
            },
        },
    )
    
    gcs_to_bq_ext_bmrs_capacity_task = BigQueryCreateExternalTableOperator(
        task_id='gcs_to_bq_ext_bmrs_capacity_task',
        table_resource={
            'tableReference': {
                'projectId': GCS_PROJECT_ID,
                'datasetId': BIGQUERY_DATASET,
                'tableId': bmrs_capacity_table_name,
            },
            'externalDataConfiguration': {
                'autodetect': True,
                'sourceFormat': 'PARQUET',
                'sourceUris': [f'gs://{GCS_BUCKET}/{bmrs_capacity_table_name}/*.parquet'],
            },
        },
    )

    gcs_to_bq_ext_power_plant_location_task = BigQueryCreateExternalTableOperator(
        task_id='gcs_to_bq_ext_power_plant_location_task',
        table_resource={
            'tableReference': {
                'projectId': GCS_PROJECT_ID,
                'datasetId': BIGQUERY_DATASET,
                'tableId': power_plant_location_table_name,
            },
            'externalDataConfiguration': {
                'autodetect': True,
                'sourceFormat': 'PARQUET',
                'sourceUris': [f'gs://{GCS_BUCKET}/{power_plant_location_table_name}/*.parquet'],
            },
        },
    )


    gcs_to_bq_ext_power_plant_id_task = BigQueryCreateExternalTableOperator(
        task_id='gcs_to_bq_ext_power_plant_id_task',
        table_resource={
            'tableReference': {
                'projectId': GCS_PROJECT_ID,
                'datasetId': BIGQUERY_DATASET,
                'tableId': power_plant_id_table_name,
            },
            'externalDataConfiguration': {
                'autodetect': True,
                'sourceFormat': 'PARQUET',
                'sourceUris': [f'gs://{GCS_BUCKET}/{power_plant_id_table_name}/*.parquet'],
            },
        },
    )

    gcs_to_bq_ext_bmrs_power_plant_info_task = BigQueryCreateExternalTableOperator(
        task_id='gcs_to_bq_ext_bmrs_power_plant_info_task',
        table_resource={
            'tableReference': {
                'projectId': GCS_PROJECT_ID,
                'datasetId': BIGQUERY_DATASET,
                'tableId': bmrs_power_plant_info_table_name,
            },
            'externalDataConfiguration': {
                'autodetect': True,
                'sourceFormat': 'PARQUET',
                'sourceUris': [f'gs://{GCS_BUCKET}/{bmrs_power_plant_info_table_name}/*.parquet'],
            },
        },
    )


    gcs_to_bq_ext_psr_fuel_type_mapping_task = BigQueryCreateExternalTableOperator(
        task_id='gcs_to_bq_ext_psr_fuel_type_mapping_task',
        table_resource={
            'tableReference': {
                'projectId': GCS_PROJECT_ID,
                'datasetId': BIGQUERY_DATASET,
                'tableId': psr_fuel_type_mapping_table_name,
            },
            'externalDataConfiguration': {
                'autodetect': True,
                'sourceFormat': 'PARQUET',
                'sourceUris': [f'gs://{GCS_BUCKET}/{psr_fuel_type_mapping_table_name}/*.parquet'],
            },
        },
    )

    bq_ext_to_partitioned_generation_task = BigQueryInsertJobOperator(
        task_id='bq_ext_to_partitioned_generation_task',
        configuration={
            'query': {
                'query': create_partitioned_generation_table_query,
                'useLegacySql': False,
            }
        },
    )
    
    bq_partitioned_to_360_view_table_task = BigQueryInsertJobOperator(
        task_id='bq_partitioned_to_360_view_table_task',
        configuration={
            'query': {
                'query': create_aggregated_table_query,
                'useLegacySql': False,
            }
        },
    )
    
        
fetch_bmrs_generation_task >>  \
fetch_bmrs_capacity_task >>  \
spark_bmrs_generation_job >>  \
spark_bmrs_capacity_job >>  \
curl_power_plant_mappings_task >>  \
transform_power_plant_id_mapping_task >>  \
transform_power_plant_location_mapping_task >>  \
transform_bmrs_power_plant_info_task >>  \
local_to_gcs_task >>  \
gcs_to_bq_ext_bmrs_generation_task >>  \
gcs_to_bq_ext_bmrs_capacity_task >>  \
gcs_to_bq_ext_power_plant_location_task >>  \
gcs_to_bq_ext_power_plant_id_task >>  \
gcs_to_bq_ext_bmrs_power_plant_info_task >>  \
gcs_to_bq_ext_psr_fuel_type_mapping_task >>  \
bq_ext_to_partitioned_generation_task >>  \
bq_partitioned_to_360_view_table_task

