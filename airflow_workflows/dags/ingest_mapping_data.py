from typing import Dict, Tuple

from math import radians, sin, cos, sqrt, atan2
import pandas as pd

from common.schema import POWER_PLANT_ID_SCHEMA
from common.file_config import *

def transform_power_plant_id_mapping_callable(raw_filepath: str, output_filepath: str) -> None: 
    """Transform power plant ID table and save to local path (in AIRFLOW)

    Args:
        df (pd.DataFrame): Local filepath of downloaded raw power plant ID dataframe
        output_filepath (str): Destination path to save the processed data
    """
    df = pd.read_csv(raw_filepath)
    df = _transform_id_df(df)
    df.to_csv(output_filepath, index=False)
    
def transform_power_plant_location_mapping_callable(raw_filepath: str, output_filepath: str) -> None: 
    """Transform power plant ID table and save to local path (in AIRFLOW)

    Args:
        df (pd.DataFrame): Local filepath of downloaded raw power plant ID dataframe
        output_filepath (str): Destination path to save the processed data
    """
    df = pd.read_csv(raw_filepath)
    df = _transform_location_df(df)
    df.to_csv(output_filepath, index=False)

def transform_bmrs_power_plant_info_mapping_callable(raw_filepath: str, output_filepath: str) -> None: 
    """Transform power plant ID table and save to local path (in AIRFLOW)

    Args:
        df (pd.DataFrame): Local filepath of downloaded raw power plant ID dataframe
        output_filepath (str): Destination path to save the processed data
    """
    df = pd.read_json(raw_filepath)
    df = df[['elexonBmUnit', 'fuelType']].rename(columns={'elexonBmUnit': 'bm_unit', 'fuelType': 'fuel_type'})
    df.to_csv(output_filepath, index=False)
    
def _transform_id_df(df: pd.DataFrame) -> pd.DataFrame:
    """Transform power plant ID table
    1. Filter columns
    2. Set schema/data types
    3. Rename columns

    Args:
        df (pd.DataFrame): Ingested power plant ID dataframe

    Returns:
        pd.DataFrame: Processed dataframe
    """
    df = df[POWER_PLANT_ID_SCHEMA.keys()]
    df = df.astype(POWER_PLANT_ID_SCHEMA)
    df = df.rename(columns={
        'sett_bmu_id': 'bm_unit', 
        'ngc_bmu_id': 'national_grid_bm_unit_id',
    })
    df = _unstack_delimited_bm_unit_ids(df)
    return df

def _unstack_delimited_bm_unit_ids(df: pd.DataFrame) -> pd.DataFrame:
    """Unstack dataframe based on comma delimited bm_unit
    E.g.: 
    dictionary_id   |bm_unit   |national_grid_bm_unit_id
    A               |A, B      |C, D
    E               |F         |G
    
    will become
    
   dictionary_id    |bm_unit   |national_grid_bm_unit_id
    A               |A         |C, D
    A               |B         |C, D
    E               |F         |G
    
    Args:
        df (pd.DataFrame): Power plant ID dataframe

    Returns:
        pd.DataFrame: Unstacked power plant ID dataframe
    """
    df = (
        df.set_index(['dictionary_id', 'name', 'national_grid_bm_unit_id'])
            .stack()
            .str.split(',', expand=True)
            .stack()
            .unstack(-2)
            .reset_index(-1, drop=True)
            .reset_index()
    )
    
    df['bm_unit'] = df['bm_unit'].str.strip()
    return df

def _transform_location_df(df: pd.DataFrame) -> pd.DataFrame:
    """Transform power plant location table
    Get city, county and gsp column given latitude and longitude of a power plant

    Args:
        df (pd.DataFrame): Ingested power plant location dataframe

    Returns:
        pd.DataFrame: Processed dataframe
    """
    uk_cities_coordinates_df = _get_uk_cities_coordinates_df()
    uk_counties_coordinates_df = pd.read_csv(uk_counties_coordinates_file)
    uk_gsps_coordinates_df = pd.read_csv(uk_gsps_coordinates_file)
    
    latitude_longitude_df = df[['latitude', 'longitude']].copy()
    
    df['city'] = _get_regional_columns_from_coordinates_df('city', uk_cities_coordinates_df, latitude_longitude_df)
    df['county'] = _get_regional_columns_from_coordinates_df('county', uk_counties_coordinates_df, latitude_longitude_df)
    df['gsp_name'] = _get_regional_columns_from_coordinates_df('gsp_name', uk_gsps_coordinates_df, latitude_longitude_df)
    return df

def _get_regional_columns_from_coordinates_df(region_type: str, region_coordinate_mapping_df: pd.DataFrame, lat_long_df: pd.DataFrame) -> pd.DataFrame:
    region_coordinate_mapping_dict = {item[region_type]: (item['latitude'], item['longitude']) for item in region_coordinate_mapping_df.to_dict('records')}
    return lat_long_df.apply(lambda x: _get_closest_region_by_coordinates(region_coordinate_mapping_dict, x['latitude'], x['longitude']), axis=1)

def _get_closest_region_by_coordinates(regional_coordinates_dict: Dict[str, Tuple[float, float]], latitude: float, longitude: float) -> str:
    """Get closest region to the given latitude and longitude of the power plant based on the dictionary of region to coordinates and haversine distance

    Args:
        regional_coordinates_dict (Dict[str, Tuple[float, float]]): Dictionary of region to coordinates
        latitude (float): Latitude of power plant
        longitude (float): longitude of power plant

    Returns:
        str: Closest region to the given latitude and longitude
    """
    # Find the closest region
    closest_region = None
    min_distance = float('inf')

    for region, (lat, lon) in regional_coordinates_dict.items():
        distance = haversine(latitude, longitude, lat, lon)
        if distance < min_distance:
            min_distance = distance
            closest_region = region
    return closest_region

def haversine(latitude_1: float, longitude_1: float, latitude_2: float, longitude_2: float) -> float:
    """Get haversine distance of two sets of coordinates

    Args:
        latitude_1 (float): Latitude of the first set of coordinates
        longitude_1 (float): Longitude of the second set of coordinates
        latitude_2 (float): Latitude of the first set of coordinates
        longitude_2 (float): Longitude of the second set of coordinates

    Returns:
        float: Calculated haversine distance
    """
    
    R = 6371.0  # Earth radius in kilometers

    # Convert latitude and longitude from degrees to radians
    latitude_1 = radians(latitude_1)
    longitude_1 = radians(longitude_1)
    latitude_2 = radians(latitude_2)
    longitude_2 = radians(longitude_2)

    # Calculate the differences
    d_longitude = longitude_2 - longitude_1
    d_latitude = latitude_2 - latitude_1

    # Calculate the distance using the Haversine formula
    a = sin(d_latitude / 2)**2 + cos(latitude_1) * cos(latitude_2) * sin(d_longitude / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    distance = R * c

    return distance

def _get_uk_cities_coordinates_df() -> pd.DataFrame:
    """Get UK cities coordinates dataframe from file

    Returns:
        pd.DataFrame: Filtered UK cities coordinates dataframe
    """
    df = _get_raw_uk_cities_coordinates_df()
    
    # Append manually added UK city entries
    df = _append_additional_city_coordinates(df)
    
    # Filter city coordinates df by list of cities
    uk_cities_list_df = _get_uk_cities_list_df()
    df = _reduce_city_coordinates_to_official_cities(df, uk_cities_list_df)

    return df

def _get_raw_uk_cities_coordinates_df() -> pd.DataFrame:
    """Get raw dataframe of UK cities coordinates mapping from file and clean the city column for further string matching
    
    Returns:
        pd.DataFrame: Retrived and cleaned UK cities coordinates mapping df
    """    
    df = pd.read_csv(uk_cities_coordinates_file)[['city', 'lat', 'lng']].rename(columns={'lat': 'latitude', 'lng': 'longitude'})
    # Clean city string
    df['city'] = _clean_city_string_column(df['city'])
    return df

def _get_uk_cities_list_df() -> pd.DataFrame:
    """Get list of official UK cities 

    Returns:
        pd.DataFrame: Dataframe of official UK cities 
    """
    df = pd.read_csv(uk_cities_list_file)
    df['city'] = _clean_city_string_column(df['city'])
    return df

def _reduce_city_coordinates_to_official_cities(df: pd.DataFrame, uk_cities_list_df: pd.DataFrame) -> pd.DataFrame:
    """Reduce city coordinates mapping df to only include those cities in the official cities list

    Args:
        df (pd.DataFrame): City coordinates df to be reduced
        uk_cities_list_df (pd.DataFrame): Official list of UK cities

    Returns:
        pd.DataFrame: City coordinates df reduced
    """
    # De-dupe cities and keep the first one
    df = df.drop_duplicates(subset='city')
    return df[df['city'].isin(uk_cities_list_df['city'].unique())]

def _append_additional_city_coordinates(df: pd.DataFrame) -> pd.DataFrame:
    """Append additional city coordinates to existing coordinates mapping

    Args:
        df (pd.DataFrame): existing city coordinates mapping df

    Returns:
        pd.DataFrame: City coordinates mapping df appended with additional entries
    """
    additional_city_coordinates_entries_df = _get_additional_city_coordinates_entries_df()
    uk_cities_coordinates_df = pd.concat([df, additional_city_coordinates_entries_df], axis=0)
    return uk_cities_coordinates_df

def _get_additional_city_coordinates_entries_df() -> pd.DataFrame:
    """Get a dataframe of additional entries for city coordinates not included in the original file

    Returns:
        pd.DataFrame: _description_
    """
    return pd.DataFrame.from_records(new_coordinates_entries, columns=['city', 'latitude', 'longitude'])

def _clean_city_string_column(ser: pd.Series) -> pd.Series:
    """Clean city column by removing '-' and converting 'St ' to 'Saint ' for accurate matching 

    Args:
        ser (pd.Series): city column

    Returns:
        pd.Series: Cleaned city column
    """
    return ser.str.replace('-', ' ').replace(coordinate_df_replace_mapping)