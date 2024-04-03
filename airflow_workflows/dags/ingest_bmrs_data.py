import re
from typing import Dict, List, Any

from datetime import datetime, timedelta
import json
from loguru import logger
import requests
from requests.exceptions import JSONDecodeError

# os.environ['PYSPARK_PYTHON'] = '/Users/lilychau/Library/Caches/pypoetry/virtualenvs/uk-clean-energy-generation-live-data-analy-dYb7QdSs-py3.9/bin/python'
# os.environ['PYSPARK_DRIVER_PYTHON'] = '/Users/lilychau/Library/Caches/pypoetry/virtualenvs/uk-clean-energy-generation-live-data-analy-dYb7QdSs-py3.9/bin/python'

def fetch_bmrs_data(
        url_template: str, 
        request_from_datetime: datetime, 
        request_to_datetime: datetime, 
        json_raw_output_path: str, 
        **kwargs, 
    ) -> None:
    """ Fetch data from BMRS Insights Solution platform via API

    Args:
        url_template (str): Request URL with placeholder keyword for request from and to datetime
        request_from_datetime (datetime): Start datetime of data request
        request_to_datetime (datetime): End datetime of data request
    """
    print(f'FOUND KWARGS: {kwargs}')
    gcs_generation_files = kwargs['ti'].xcom_pull(task_ids='gcs_generation_files')
    if gcs_generation_files:
        print(f'gcs_generation_files: {gcs_generation_files}')

        all_settlement_dates = set([re.findall(r'\/settlement_date=(\d\d\d\d-\d\d-\d\d)', f)[0] for f in gcs_generation_files])
        print(f'all_settlement_dates: {all_settlement_dates}')
        all_settlement_dates = [datetime.strptime(date, '%Y-%m-%d') for date in all_settlement_dates]

        # In case of prolonged delay or server failures from previous days, fetch data from where it was last updated
        # This is identified by comparing the latest generation partition to the day before generation batch request from date
        # E.g. If today's date is 2024-03-26, supposedly a batch will obtain data from 2024-03-19 00:00:00 up till 2024-03-19 23:59:59 
        # However, due to server failure data of 2024-03-17 and 2024-03-18 was published late and only today
        # The following code checks if the latest available data (Turns out only 2024-03-16 data is ingested) is smaller than 2023-03-18
        # If so, set batch_bmrs_generation_from_datetime variable to 2024-03-17 and ingest from there instead of 2024-03-19
        if max(all_settlement_dates) < (request_from_datetime - timedelta(days=1)): 
            request_from_datetime = max(all_settlement_dates) + timedelta(days=1)
        print(f'max(all_settlement_dates): {max(all_settlement_dates)}')
        print(f'request_from_datetime: {request_from_datetime}')
        print(f'request_from_datetime - timedelta(days=1): {request_from_datetime - timedelta(days=1)}')
        print(f'request_from_datetime (updated): {request_from_datetime}')
        
    request_from_datetime_str = request_from_datetime.strftime('%Y-%m-%dT%H%%3A%MZ')
    request_to_datetime_str = request_to_datetime.strftime('%Y-%m-%dT%H%%3A%MZ')

    # Replace {keyword} with the corresponding arguments. 
    url = url_template.format(
        request_from_datetime=request_from_datetime_str, 
        request_to_datetime=request_to_datetime_str, 
    ) 
    
    logger.info('Obtaining BMRS {} data from {} to {}...'.format(
        json_raw_output_path, 
        request_from_datetime.strftime('%Y-%m-%d %H:%M'), 
        request_to_datetime.strftime('%Y-%m-%d %H:%M'), 
        ))
    print(f'request from date: {request_from_datetime_str}')
    print(f'URL: {url}')
    json_data = make_request(url)

    # Save JSON data to file
    with open(json_raw_output_path, "w") as json_file:
        json.dump(json_data, json_file)
    
    logger.info(f'BMRS data saved to {json_raw_output_path}.')

def make_request(url: str) -> List[Dict[str, Any]]:
    """ Fetch data with requests

    Args:
        url (str): Request URL

    Raises:
        Exception: 400 Error status code: Error with query parameters
        Exception: 500 Error status code: Server error
        Exception: Any non-JSON requests
        e: Other errors (e.g. 404)

    Returns:
        List[Dict[str, Any]]: JSON response data
    """
    response = requests.get(url)
    
    logger.info('BMRS data obtained.')
    if response.status_code == 400:
        logger.error(f'Error with query parameters: {response.request.url}')
        raise Exception(f'Error with query parameters: {response.request.url}')
    
    elif response.status_code == 500: 
        logger.error(f'Server error: {response.request.url}')
        raise Exception(f'Server error: {response.request.url}')

    elif response.status_code == 200: 
        try:
            return response.json()
        except JSONDecodeError: 
            logger.error('Error decoding JSON data.')
            raise Exception('Only JSON format download is supported in this tool.')
        except Exception as e: 
            logger.error(f'An unexpected error occurred: {str(e)}')
            raise e
