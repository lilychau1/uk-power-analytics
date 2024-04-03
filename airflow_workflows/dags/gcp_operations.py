from loguru import logger
from typing import List, Dict, Any
import pyarrow.parquet as pq
import pyarrow.csv as pc
import pyarrow as pa

def upload_multiple_files_to_gcs(gcs_bucket: str, upload_files_config_list: List[Dict[str, Any]]) -> None: 
    for upload_config in upload_files_config_list:
        table_name = upload_config.get('table_name')
        
        logger.info(f'Writing {table_name} to GCS...')
        
        local_file = upload_config.get('local_file')
        partition_cols = upload_config.get('partition_cols')
        _upload_to_gcs(gcs_bucket, table_name, local_file, partition_cols)        

def _upload_to_gcs(bucket_name: str, table_name: str, local_file: str, partition_cols: list) -> None: 
    root_path = f'{bucket_name}/{table_name}'
    logger.info(f'Saving {table_name} to GCS bucket {root_path}...')
    print(f'partition_cols: {partition_cols}')
    if '.csv' in local_file: 
        table = pc.read_csv(local_file)
    else:
        table = pq.read_table(local_file)
    print(f'all columns: {table.column_names}')
    gcs = pa.fs.GcsFileSystem()

    pq.write_to_dataset(
        table,
        root_path=root_path,
        partition_cols=partition_cols,
        filesystem=gcs
    )