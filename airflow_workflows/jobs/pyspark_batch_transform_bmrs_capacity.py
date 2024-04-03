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
parent_directory = os.path.dirname(os.path.dirname(current_file_path))
sys.path.append(parent_directory)

from common.utils import rename_column_camel_case_to_snake_case
from common.namings import BmrsDataCategory
from common.schema import BMRS_SCHEMAS
from common.file_config import batch_bmrs_capacity_output_filepath, batch_bmrs_capacity_json_raw_output_filepath

bmrs_data_category = BmrsDataCategory.CAPACITY

# Initialize Spark session
spark = SparkSession.builder \
    .master("local") \
    .appName("BmrsDataTransformation") \
    .getOrCreate()

# Read BMRS JSON data into Spark DataFrame
schema = BMRS_SCHEMAS[bmrs_data_category]

logger.info('Loading BMRS capacity JSON data in PySpark...')

df = spark.read.json(batch_bmrs_capacity_json_raw_output_filepath, schema=schema)
    
logger.info('Transforming BMRS capacity data...')

# Convert halfHourEndTime to datetime type
# Convert settlementDate to datetime type
# Rename columns to snake case
columns_snake_case = rename_column_camel_case_to_snake_case(schema.names)

df = df \
    .withColumn('publishTime', F.to_timestamp('publishTime')) \
    .withColumn('effectiveFrom', F.to_timestamp('effectiveFrom')) \

df = df.toDF(*columns_snake_case) 

logger.info(f'Saving BMRS {BmrsDataCategory.CAPACITY.name} data to local folder {batch_bmrs_capacity_output_filepath}...')

# Save dataframe as parquet
df.write.mode('overwrite').parquet(batch_bmrs_capacity_output_filepath)
logger.info(f'BMRS {BmrsDataCategory.CAPACITY.name} data saved.')