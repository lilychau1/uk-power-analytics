"""
Transform and save BMRS data depending category (generation or capacity)
1. Select columns
2. Convert types
3. Rename column to snake case

Args:
    bmrs_data_category (BmrsDataCategory): GENERATION or CAPACITY
    data (Dict): JSON response data
    output_filepath (str): Destination path to save the fetched and processed data
"""
import os
import sys

from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

current_file_path = os.path.abspath(__file__)
print(f'current_file_path: {current_file_path}')
parent_directory = os.path.dirname(os.path.dirname(current_file_path))
sys.path.append(parent_directory)

from common.utils import rename_column_camel_case_to_snake_case
from common.namings import BmrsDataCategory
from common.schema import BMRS_SCHEMAS
from common.file_config import initialise_bmrs_generation_output_filepath, initialise_bmrs_generation_json_raw_output_filepath

bmrs_data_category = BmrsDataCategory.GENERATION

# Initialize Spark session
spark = SparkSession.builder \
    .master("local") \
    .appName("BmrsDataTransformation") \
    .getOrCreate()

# Read BMRS JSON data into Spark DataFrame
schema = BMRS_SCHEMAS[bmrs_data_category]

logger.info('Loading BMRS generation JSON data in PySpark...')
df = spark.read.json(initialise_bmrs_generation_json_raw_output_filepath, schema=schema)

# logger.info('Transforming BMRS generation data...')

# Convert halfHourEndTime to datetime type
# Convert settlementDate to datetime type
# Rename columns to snake case

df = df \
    .withColumn('halfHourEndTime', F.to_timestamp('halfHourEndTime')) \
    .withColumn('settlementDate', F.to_timestamp('settlementDate')) \
    .withColumn('settlement_date_partition', F.to_date("settlementDate", "yyyy-MM-dd"))

columns_snake_case = rename_column_camel_case_to_snake_case(df.columns)
df = df.toDF(*columns_snake_case)

logger.info(f'Saving BMRS {BmrsDataCategory.GENERATION.name} data to local folder {initialise_bmrs_generation_output_filepath}...')

# Save dataframe as parquet
df.write.mode('overwrite').parquet(initialise_bmrs_generation_output_filepath)
logger.info(f'BMRS {BmrsDataCategory.GENERATION.name} data saved.')