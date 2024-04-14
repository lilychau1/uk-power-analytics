"""
Data ingestion, processing, saving and loading configs
"""
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os
import yaml

# Load the YAML file
with open('common/config.yaml', 'r') as file:
    config = yaml.safe_load(file)

AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/opt/airflow')
SPARK_DATA_HOME = os.environ.get('SPARK_DATA_HOME', '/opt/bitnami/spark')
GCS_PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
GCS_BUCKET = os.environ.get('GCP_GCS_BUCKET')
BIGQUERY_DATASET = os.environ.get('BIGQUERY_DATASET')

INITIALISE_DATA_LENGTH_DAYS = int(os.getenv('INITIALISE_DATA_LENGTH_DAYS', 12))
BMRS_GENERATION_PUBLISH_DELAY_DAYS = int(os.getenv('BMRS_GENERATION_PUBLISH_DELAY_DAYS', 8))

# Data request dates configurations
current_datetime = datetime.now()

# Initialise data from self-defined number of days (initialise data length)
initialise_bmrs_from_datetime = (current_datetime - relativedelta(days=INITIALISE_DATA_LENGTH_DAYS)).replace(hour=0, minute=0, second=0)

# Initialise BMRS generation data to the days of delay 00:00:00 
# E.g.: If days of delay is 7, initialise data length is 10, today's date is 2024-03-26, 
# the initialised data will be from 2024-03-16 00:00:00 to 2024-03-18 23:59:59 
initialise_bmrs_generation_to_datetime = (current_datetime - relativedelta(days=BMRS_GENERATION_PUBLISH_DELAY_DAYS+1)).replace(hour=23, minute=59, second=59)

# Batch request fata from a day before last ingestion ended (also where data initialise stops)
# E.g.: Initialised data ended at 2024-03-18 23:59:59 
# Batch data starts at 2024-03-19 00:00:00 up till 2024-03-19 23:59:59 
batch_bmrs_generation_from_datetime = (current_datetime - relativedelta(days=BMRS_GENERATION_PUBLISH_DELAY_DAYS)).replace(hour=0, minute=0, second=0)
batch_bmrs_generation_to_datetime = (current_datetime - relativedelta(days=BMRS_GENERATION_PUBLISH_DELAY_DAYS)).replace(hour=0, minute=0, second=0)

# BMRS Capacity data has the earliest published time of 2023-01-01, which will be used to obtain all existing data
initialise_bmrs_capacity_from_datetime = datetime(2023, 1, 1, 0, 0, 0, 0)
# Initialise BMRS capacity data to current date 00:00:00
# E.g.: If today's date is 2024-03-26, the initialised data will be where
# published date ranges from 2023-01-01 00:00:00 to 2024-03-25 00:00:00 (without full day data 2024-03-25)
initialise_bmrs_capacity_to_datetime = (current_datetime - relativedelta(days=1)).replace(hour=0, minute=0, second=0)

# Batch request fata from a day before last ingestion ended (also where data initialise stops)
# E.g.: Initialised data ended at 2024-03-26 00:00:00
# Batch data starts at 2024-03-25 00:00:00 up till 2024-03-26 00:00:00
batch_bmrs_capacity_from_datetime = (current_datetime - relativedelta(days=1)).replace(hour=0, minute=0, second=0)
batch_bmrs_capacity_to_datetime = current_datetime.replace(hour=0, minute=0, second=0)

# Local data folder
initialise_output_folder = f'{AIRFLOW_HOME}/power_data/initialise'
batch_output_folder = f'{AIRFLOW_HOME}/power_data/batch'
batch_yearly_output_folder = f'{AIRFLOW_HOME}/power_data/batch_yearly'

##############################################
# BMRS Generation data
##############################################

bmrs_generation_url_template =  config['bmrs_generation']['url_template']
bmrs_generation_output_file_template = config['bmrs_generation']['output_file_template']

bmrs_generation_table_name = config['bmrs_generation']['table_name']
bmrs_generation_table_name_partitioned = f'{bmrs_generation_table_name}_partitioned'

# TODO: hard-coded column names, not ideal. 

# Initialise data
initialise_bmrs_generation_output_file = bmrs_generation_output_file_template.format(
    initialise_bmrs_from_datetime.strftime('%Y%m%d'), 
    initialise_bmrs_generation_to_datetime.strftime('%Y%m%d')
)

initialise_bmrs_generation_output_folder = f'{initialise_output_folder}/{bmrs_generation_table_name}'
initialise_bmrs_generation_json_raw_output_filepath = f'{initialise_bmrs_generation_output_folder}/{bmrs_generation_table_name}_raw.json'
initialise_bmrs_generation_output_filepath = f'{initialise_bmrs_generation_output_folder}/{initialise_bmrs_generation_output_file}'

# Batch daily data
batch_bmrs_generation_output_file = bmrs_generation_output_file_template.format(
    batch_bmrs_generation_from_datetime.strftime('%Y%m%d'), 
    batch_bmrs_generation_to_datetime.strftime('%Y%m%d')
)
batch_bmrs_generation_output_folder = f'{batch_output_folder}/{bmrs_generation_table_name}'
batch_bmrs_generation_json_raw_output_filepath = f'{batch_bmrs_generation_output_folder}/{bmrs_generation_table_name}_raw.json'
batch_bmrs_generation_output_filepath = f'{batch_bmrs_generation_output_folder}/{batch_bmrs_generation_output_file}'
bmrs_generation_partition_cols = config['bmrs_generation']['partition_cols']

##############################################
# BMRS Capacity data
##############################################

bmrs_capacity_url_template =  config['bmrs_capacity']['url_template']
bmrs_capacity_output_file_template = config['bmrs_capacity']['output_file_template']

bmrs_capacity_table_name = config['bmrs_capacity']['table_name']

# Initialise data
initialise_bmrs_capacity_output_file = bmrs_capacity_output_file_template.format(
    initialise_bmrs_capacity_from_datetime.strftime('%Y%m%d'), 
    initialise_bmrs_capacity_to_datetime.strftime('%Y%m%d')
)
initialise_bmrs_capacity_output_folder = f'{initialise_output_folder}/{bmrs_capacity_table_name}'
initialise_bmrs_capacity_json_raw_output_filepath = f'{initialise_bmrs_capacity_output_folder}/{bmrs_capacity_table_name}_raw.json'
initialise_bmrs_capacity_output_filepath = f'{initialise_bmrs_capacity_output_folder}/{initialise_bmrs_capacity_output_file}'

# Batch daily data
batch_bmrs_capacity_output_file = bmrs_capacity_output_file_template.format(
    batch_bmrs_capacity_from_datetime.strftime('%Y%m%d'), 
    batch_bmrs_capacity_to_datetime.strftime('%Y%m%d')
)

batch_bmrs_capacity_output_folder = f'{batch_output_folder}/{bmrs_capacity_table_name}'
batch_bmrs_capacity_json_raw_output_filepath = f'{batch_bmrs_capacity_output_folder}/{bmrs_capacity_table_name}_raw.json'
batch_bmrs_capacity_output_filepath = f'{batch_bmrs_capacity_output_folder}/{batch_bmrs_capacity_output_file}'

##############################################
# Power plant coordinates data
##############################################

power_plant_location_table_name = config['power_plant_location']['table_name']
power_plant_location_file_url = config['power_plant_location']['url']

# Initialise
initialise_power_plant_location_raw_output_file = f'{initialise_output_folder}/{power_plant_location_table_name}_raw.csv'
initialise_power_plant_location_processed_output_file = f'{initialise_output_folder}/{power_plant_location_table_name}.csv'

# Batch yearly
batch_yearly_power_plant_location_raw_output_file = f'{batch_yearly_output_folder}/{power_plant_location_table_name}_raw.csv'
batch_yearly_power_plant_location_processed_output_file = f'{batch_yearly_output_folder}/{power_plant_location_table_name}.csv'


##############################################
# UK regional coordinates data
##############################################
# Local data folder
resources_folder = f'{AIRFLOW_HOME}/resources'

uk_cities_coordinates_file = f'{resources_folder}/uk_cities_coordinates.csv'
uk_cities_list_file = f'{resources_folder}/uk_cities_list.csv'
uk_counties_coordinates_file = f'{resources_folder}/uk_counties_coordinates.csv'
uk_gsps_coordinates_file = f'{resources_folder}/uk_gsps_coordinates.csv'

coordinate_df_replace_mapping =  config['coordinate_df_replace_mapping']
new_coordinates_entries =  config['new_coordinates_entries']

##############################################
# BMRS BM Unit info overlay data
##############################################
bm_unit_info_overlay_table_name = 'bm_unit_info_overlay'
bm_unit_info_overlay_file = f'{resources_folder}/bm_unit_info_overlay.csv'

##############################################
# Power plant ID data
##############################################

power_plant_id_file_url = config['power_plant_id']['url']
power_plant_id_table_name = config['power_plant_id']['table_name']

# Initialise
initialise_power_plant_id_raw_output_file = f'{initialise_output_folder}/{power_plant_id_table_name}_raw.csv'
initialise_power_plant_id_processed_output_file = f'{initialise_output_folder}/{power_plant_id_table_name}.csv'

# Batch yearly
batch_yearly_power_plant_id_raw_output_file = f'{batch_yearly_output_folder}/{power_plant_id_table_name}_raw.csv'
batch_yearly_power_plant_id_processed_output_file = f'{batch_yearly_output_folder}/{power_plant_id_table_name}.csv'


##############################################
# BMRS power plant info data
##############################################

bmrs_power_plant_info_file_url = config['bmrs_power_plant_info']['url']
bmrs_power_plant_info_table_name = config['bmrs_power_plant_info']['table_name']
bmrs_power_plant_info_raw_output_file = f'{initialise_output_folder}/{bmrs_power_plant_info_table_name}.json'
bmrs_power_plant_info_processed_output_file = f'{initialise_output_folder}/{bmrs_power_plant_info_table_name}.csv'


##############################################
# BMRS power plant info data
##############################################

psr_fuel_type_mapping_table_name = config['psr_fuel_type_mapping']['table_name']
psr_fuel_type_mapping_file = f'{resources_folder}/{psr_fuel_type_mapping_table_name}.csv'

##############################################
# Download command
##############################################

power_plant_mappings_download_command = '&&'.join([
    f'curl -LsS {power_plant_location_file_url} > {initialise_power_plant_location_raw_output_file}', 
    f'curl -LsS {power_plant_id_file_url} > {initialise_power_plant_id_raw_output_file}', 
    f"curl -X 'GET' '{bmrs_power_plant_info_file_url}'  -H 'accept: text/json' > {bmrs_power_plant_info_raw_output_file}", 
])


##############################################
# Transformed Bigquery tables
##############################################
generation_by_unit_by_settlement_date_table_name = config['generation_by_unit_by_settlement_date']['table_name']
fuel_with_max_daily_gen_by_county_table_name = config['fuel_with_max_daily_gen_by_county']['table_name']

##############################################
# Configuration for saving data
##############################################
# Bespoke
bespoke_upload_files_config_list = [
    {
        'table_name': bmrs_capacity_table_name,
        'local_file': initialise_bmrs_capacity_output_filepath,
    },      
    {
        'table_name': power_plant_location_table_name,
        'local_file': initialise_power_plant_location_processed_output_file,
    }, 
    {
        'table_name': power_plant_id_table_name,
        'local_file': initialise_power_plant_id_processed_output_file,
    }, 
    {
        'table_name': bmrs_power_plant_info_table_name,
        'local_file': bmrs_power_plant_info_processed_output_file,
    }, 
    {
        'table_name': psr_fuel_type_mapping_table_name,
        'local_file': psr_fuel_type_mapping_file,
    }, 
]
# Initialise
initialise_upload_files_config_list = [
    {
        'table_name': bmrs_generation_table_name,
        'local_file': initialise_bmrs_generation_output_filepath,
        'partition_cols': bmrs_generation_partition_cols
    }, 
    {
        'table_name': bmrs_capacity_table_name,
        'local_file': initialise_bmrs_capacity_output_filepath,
    },      
    {
        'table_name': power_plant_location_table_name,
        'local_file': initialise_power_plant_location_processed_output_file,
    }, 
    {
        'table_name': power_plant_id_table_name,
        'local_file': initialise_power_plant_id_processed_output_file,
    }, 
    {
        'table_name': bmrs_power_plant_info_table_name,
        'local_file': bmrs_power_plant_info_processed_output_file,
    }, 
    {
        'table_name': psr_fuel_type_mapping_table_name,
        'local_file': psr_fuel_type_mapping_file,
    }, 
    {
        'table_name': bm_unit_info_overlay_table_name,
        'local_file': bm_unit_info_overlay_file,
    },     
]

# Batch daily
batch_upload_files_config_list = [
    {
        'table_name': bmrs_generation_table_name,
        'local_file': batch_bmrs_generation_output_filepath,
        'partition_cols': bmrs_generation_partition_cols
    }, 
    {
        'table_name': bmrs_capacity_table_name,
        'local_file': batch_bmrs_capacity_output_filepath,
    },
]

# Batch yearly
batch_yearly_upload_files_config_list = [     
    {
        'table_name': power_plant_location_table_name,
        'local_file': batch_yearly_power_plant_location_processed_output_file,
    }, 
    {
        'table_name': power_plant_id_table_name,
        'local_file': batch_yearly_power_plant_id_processed_output_file,
    }, 
]