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

## Pre-requisites

### Docker
Install Docker via https://docs.docker.com/engine/install/

### git
Install git via https://git-scm.com/book/en/v2/Getting-Started-Installing-Git

### GCP

1. GCP account
Set up a GCP account via https://cloud.google.com. 

2. GCP bucket
Create a GCP bucket named 'uk-power-analytics'

3. GCP BigQuery dataset
Create a GCP BigQuery dataset named 'uk_power_analytics'

4. GCP credentials
Create a service account, assign the roles of BigQuery Admin, Storage Admin and Compute Admin. 

Generate a corresponding ssh credentials file and store it as json file in your local home directory: `~/.google_credentials/credentials/credentials.json`

## Getting Started

### Cloning Repository
Clone this repo to your local machine with one of the following commands: 

With HTTPS:
```
git clone https://github.com/lilychau1/uk-power-analytics.git
```

With ssh:
```
git clone git@github.com:lilychau1/uk-power-analytics.git
```

### Setting up environment

Navigate to the cloned repo workspace. Run the following command:

```
./setup.sh
```

Necessary directories will be created:
* airflow/workflows/power_data/initialise/bmrs_generation
* airflow/workflows/power_data/initialise/bmrs_capacity
* airflow/workflows/power_data/batch/bmrs_generation
* airflow/workflows/power_data/batch/bmrs_capacity

A bespoke docker image consisting of airflow and Spark will also be created

### Update .env
Create a file named `.env` and add GCP bucket configuration:

```
GCP_PROJECT_ID=<your_project_id>
```

### Run docker-compose
To start the container, run:

```
docker-compose up
```

### Set up spark connection on airflow
In your web browser, go to http://localhost:8080/. Enter username `airflow` and password `airflow` (unless otherwise specified in .env with `_AIRFLOW_WWW_USER_USERNAME` and `_AIRFLOW_WWW_USER_PASSWORD`). 

In the top navigation bar, go to Admin > Connections > + > Fill in the following: 
Connection Id: `spark-conn`
Connection Type: `spark`
Host: `spark://spark-master`
Port: `7077`

### Explore DAGs

In the DAGs tab, you will find two different workflows: `initialise_data` and `ingest_batch_data`. 

`initialise_data` is run once only, while `ingest_batch_data` is run daily. 

After initialisies `initialise_data` is run, you will observe the following:
1. New parquets, csv and json files in local `airflow/power_data` folder
2. New folders in your GCS bucket named `uk-power-analytics`: 
    * bmrs_capacity
    * bmrs_generation
    * bmrs_power_plant_info
    * power_plant_id
    * power_plant_location
    * psr_fuel_type_mapping
3. New tables in your GCS BigQuery dataset `uk_power_analytics`: 
    * bmrs_generation
    * bmrs_generation_partitioned
    * bmrs_power_plant_info
    * generation_capacity_360_view
    * power_plant_id
    * power_plant_location
    * psr_fuel_type_mapping

Every day, after `ingest_batch_data` is run at 5am, there will be incremental generation data ingested to both the GCS bucket `bmrs_generation` folder and BigQuery tables `bmrs_generation`, `bmrs_generation_partitioned` and `generation_capacity_360_view`. 

## Data Visualisation

Google Looker Studio was used to experiment with visualising UK per unit power generation data. 

![uk_power_analytics_data_viz](https://github.com/lilychau1/uk-power-analytics/assets/58731610/09dea2f6-f47b-4097-a3cc-faf35cba3d41)

The first chart (left) depicts the main fuel category (Clean, Fossil, Nuclear, Others) of each county from 1 Feb 2024 to 27 Mar 2024. 

The second chart (top right) illustrated the total generation by fuel category and county origin of generation. 

The third chart (bottom right) showed the average unit generation output per settlement period (0 - 00:00 to 00:30, ..., 48 - 23:30 to 00:00). 
