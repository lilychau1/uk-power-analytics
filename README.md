# Data Engineering Zoomcamp Project: UK Power Generation Data Analytics
Data pipeline for analysing Elexon electricity generation output per generation unit in the UK

## Background
This project analyses the electricity generation in the UK, segregated by region, production source and source type (clean, fossil, nuclear or others). The analytics will provide a good understanding of the overall electricity generation carbon footprint of the entire nation, and an exploratory start point for transmission system planning when gradually phasing out fossil-fuelled power generation under the Net Zero initiative.

## Scope
The geographical coverage is within the United Kingdom, including England, Scotland, Wales, and Northern Ireland. 

The analysis covers electricity generation from the following sources:
Fossil sources:
* Fossil Gas
* Fossil Hard Coal
* Fossil Oil

Renewable sources:
* Hydro Pumped Storage
* Hydro Run-of-river and Poundage Nuclear
* Offshore Wind
* Onshore Wind
* Solar
* Biomass

Other sources:
* Nuclear
* Other

## Technology
The project is developed using Python. 

* Cloud: GCP
* Workflow orchestration: Airflow
* Data Warehouse: BigQuery
* Batch processing: Spark
* Data viz: Looker

## Data sources
The power generation data comes from the Elexon Insights Solution platform (https://bmrs.elexon.co.uk/). 

## Data pipeline design

### Initialisation data ingestion
For initial set-up, a one-time DAG will be performed in Airflow to initialise base data, by extracting, loading and transforming the following data:

#### Actual power generation per generation unit
The Actual Generation Output Per Generation Unit (B1610)  stream data (https://bmrs.elexon.co.uk/api-documentation/endpoint/datasets/B1610/stream) is published five working days after the end of the operational period, available in 30-minute intervals. For example, data from 19 Mar 2024 00:15 to 20 Mar 2024 00:00 will all be published on 26 Mar 2024. 

The one-time DAG will ingest 3 days’ worth of generation data, from 4 days plus 5 days ago until current date minus 5 days, converting the data into parquet format which involves renaming and type conversion.

#### Latest production capacity data per BM Unit
The Installed Generation Capacity per Unit (IGCPU / B1420) data  (https://bmrs.elexon.co.uk/api-documentation/endpoint/datasets/IGCPU) is updated upon record addition or update. 

The initialisation DAG extracts all existing data from the site and stores it as a parquet file in the GCS bucket and loads it to BigQuery as a separate table. 

#### Latest Power Plant Locations
The Power Plant Locations dataset (https://osuked.github.io/Power-Station-Dictionary/attribute_sources/plant-locations/plant-locations.csv) is a one-off CSV produced by Ayrton Bourn, based on his project-defined identifier. The data consists of the identifier, latitude and longitude of the power station. 

Again, the initialisation DAG extracts the entire CSV from the URL and stores it as a parquet file in the GCS bucket and loads it to BigQuery as a separate table. 

#### Power Plant ID Dictionary
The Power Plant IDs dataset (https://github.com/OSUKED/Power-Station-Dictionary/blob/main/data/dictionary/ids.csv) is a one-off CSV produced by Ayrton Bourn, matching his project-defined identifier to the corresponding Elexon’s Settlement BMU ID(s). This will be used to map the above location information against a BM Unit. 

The DAG extracts the entire CSV from the URLand stores it as a parquet file, transforms it so that the unique identifier is the BMU ID and stores it as a processed parquet in the GCS bucket and loads it to BigQuery as a separate table. 

#### Fuel category mapping
This is a self-defined mapping CSV of each fuel to either one of the following energy source categories: Clean, Fossil and Other. 

The initialisation DAG creates it as a CSV in the GCS bucket and loads it to BigQuery as a separate table. 

### Incremental data ingestion: Batch Processing and Loading
#### Daily: Actual power generation per generation unit
The data is obtained via a daily batch performed in PySpark, triggered by Airflow. It is then processed, which involves conversion to megawatt-hours (MWh), renaming, and type conversion.

#### Daily: Latest production capacity data per BM Unit
The data is obtained via a daily batch performed in PySpark, triggered by Airflow. It is then processed and appended to the existing reference parquet. 

#### Yearly: Latest Power Plant Locations and Power Plant ID Dictionary
The data from Power-Station-Dictionary is obtained every year via a batch performed in python script, triggered by Airflow. It is then processed and appended to the existing reference parquet. 


## Storage and Data Warehouse
All processed data is stored in a Google Cloud Storage (GCS) bucket and loaded into BigQuery. 

For the batched per unit generation data, each batch of records will be stored as one parquet file in a specified GCS Bucket. 

For the reference tables, they will be stored as separate parquet files. 

All the data will be loaded to BigQuery for further transformation. 
