# Data Engineering Zoomcamp Project: UK Power Generation Data Analytics
Data pipeline for analysing Elexon electricity generation output per generation unit in the UK

![uk_power_analytics_flowchart](https://github.com/lilychau1/uk-power-analytics/assets/58731610/2f97db34-bab4-4900-8191-0ef55294ada9)

## Table of Contents

1. [Background](#background)
2. [Scope](#scope)
3. [Technology](#technology)
4. [Data Sources](#data-sources)
5. [Data Pipeline Design](#data-pipeline-design)
    - [Initialisation Data Ingestion](#initialisation-data-ingestion)
        - [Actual Power Generation Per Generation Unit](#actual-power-generation-per-generation-unit)
        - [Latest Production Capacity Data Per BM Unit](#latest-production-capacity-data-per-bm-unit)
        - [Latest Power Plant Locations](#latest-power-plant-locations)
        - [Power Plant ID Dictionary](#power-plant-id-dictionary)
        - [Fuel Category Mapping](#fuel-category-mapping)
    - [Incremental Data Ingestion: Batch Processing and Loading](#incremental-data-ingestion-batch-processing-and-loading)
        - [Daily: Actual Power Generation Per Generation Unit](#daily-actual-power-generation-per-generation-unit)
        - [Daily: Latest Production Capacity Data Per BM Unit](#daily-latest-production-capacity-data-per-bm-unit)
        - [Yearly: Latest Power Plant Locations and Power Plant ID Dictionary](#yearly-latest-power-plant-locations-and-power-plant-id-dictionary)
6. [Storage and Data Warehouse](#storage-and-data-warehouse)
7. [Pre-requisites](#pre-requisites)
    - [git](#git)
    - [GCP](#gcp)
    - [Terraform](#terraform)
8. [Getting Started](#getting-started)
    - [Clone Repository](#clone-repository)
    - [Add GCP Credential](#add-gcp-credential)
    - [Update .env](#update-env)
    - [Set Up Terraform](#set-up-terraform)
    - [Access Airflow](#access-airflow)
    - [Set Up Spark Connection on Airflow](#set-up-spark-connection-on-airflow)
    - [Explore DAGs](#explore-dags)
    - [Destroy Resources](#destroy-resources)
9. [Data Visualisation](#data-visualisation)
9. [Contact](#contact)

<br>

---

## Background
This project is a proof of concept showcasing an end-to-end data ingestion pipeline that utilises Terraform, Google Cloud Platform (GCP), Apache Airflow, and Docker to analyse the Elexon BMRS electricity generation data via its [Insights Solution data platform](https://bmrs.elexon.co.uk/). The analysis focuses on electricity output by individual generating units in the UK, segregated by region, production source, and source type (clean, fossil, nuclear, or others). 

The analysis aims to offer valuable insights into the carbon footprint associated with electricity generation on a national scale. Furthermore, it serves as an initial exploration for transmission system planning, particularly in the context of transitioning away from fossil-fueled power generation in alignment with the Net Zero initiative.

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

* Cloud Infrastructure: Google Cloud Platform (GCP)
* Infrastructure as Code (IaC): Terraform
* Workflow Orchestration: Apache Airflow
* Data Warehouse: Google BigQuery
* Batch Processing: Apache Spark
* Data Visualization: Looker Studio

![uk_power_analytics_flowchart](https://github.com/lilychau1/uk-power-analytics/assets/58731610/2f97db34-bab4-4900-8191-0ef55294ada9)

## File Hierarchy

```bash
├── README.md
├── airflow_workflows
│   ├── Dockerfile
│   ├── __init__.py
│   ├── common
│   │   ├── __init__.py
│   │   ├── bq_queries.py
│   │   ├── config.yaml
│   │   ├── file_config.py
│   │   ├── namings.py
│   │   ├── schema.py
│   │   └── utils.py
│   ├── config
│   │   └── airflow.cfg
│   ├── dags
│   │   ├── etl_batch_ingest_data.py
│   │   ├── etl_initialise_data.py
│   │   ├── gcp_operations.py
│   │   ├── ingest_bmrs_data.py
│   │   └── ingest_mapping_data.py
│   ├── docker-compose.yml
│   ├── jobs
│   │   ├── pyspark_batch_transform_bmrs_capacity.py
│   │   ├── pyspark_batch_transform_bmrs_generation.py
│   │   ├── pyspark_transform_bmrs_capacity.py
│   │   └── pyspark_transform_bmrs_generation.py
│   ├── logs
│   ├── plugins
│   ├── requirements.txt
│   └── resources
│       ├── bm_unit_info_overlay.csv
│       ├── psr_fuel_type_mapping.csv
│       ├── uk_cities_coordinates.csv
│       ├── uk_cities_list.csv
│       ├── uk_counties_coordinates.csv
│       └── uk_gsps_coordinates.csv
├── keys
├── main.tf
├── media
│   ├── uk_power_analytics_data_viz.jpg
│   ├── uk_power_analytics_data_viz.pdf
│   └── uk_power_analytics_flowchart.jpg
├── start_up_script.sh
└── variables.tf
```

Below is the file hierarchy of the project. 
1. `main.tf`, `variables.tf` and `start_up_script.sh` are the essential scripts for terraform.
2. The `keys` folder is required for GCP authentication. User will need to provide a json credential file named `my-creds.json`
3. The `airflow_workflows` folder consists of the `Dockerfile` and `docker-compose.yml` used to create docker images and containers for the batch operations with Airflow and Spark.
4. The `dags` folder contains the main data ingestion DAG scripts to be run by airflow.
5. The `common` folder contains all essential scripts of configurations and helper functions for the DAGs to utilise.
6. The `jobs` folder consists of the spark operation scripts to be executed by the DAGs.
7. The `resources` folder contains all supplementary CSV datasets that helps with data transformation and visualisation. 



## Data sources
The power generation data comes from the Elexon Insights Solution platform (https://bmrs.elexon.co.uk/). 

## Data pipeline design

### Initialisation data ingestion
For initial set-up, a one-time DAG will be performed in Airflow to initialise base data, by extracting, loading and transforming the following data:

#### Actual power generation per generation unit
The Actual Generation Output Per Generation Unit (B1610)  [stream data](https://bmrs.elexon.co.uk/api-documentation/endpoint/datasets/B1610/stream) is published five working days after the end of the operational period, available in 30-minute intervals. For example, data from 19 Mar 2024 00:15 to 20 Mar 2024 00:00 will all be published on 26 Mar 2024. 

The one-time DAG will ingest 3 days’ worth of generation data, from 4 days plus 5 days ago until current date minus 5 days, converting the data into parquet format which involves renaming and type conversion.

#### Latest production capacity data per BM Unit
The [Installed Generation Capacity per Unit (IGCPU / B1420) data](https://bmrs.elexon.co.uk/api-documentation/endpoint/datasets/IGCPU) is updated upon record addition or update. 

The initialisation DAG extracts all existing data from the site and stores it as a parquet file in the GCS bucket and loads it to BigQuery as a separate table. 

#### Latest Power Plant Locations
The [Power Plant Locations dataset](https://osuked.github.io/Power-Station-Dictionary/attribute_sources/plant-locations/plant-locations.csv) is a one-off CSV produced by [Ayrton Bourn](https://osuked.github.io/Power-Station-Dictionary/), based on his project-defined identifier. The data consists of the identifier, latitude and longitude of the power station. 

Again, the initialisation DAG extracts the entire CSV from the URL and stores it as a parquet file in the GCS bucket and loads it to BigQuery as a separate table. 

#### Power Plant ID Dictionary
The [Power Plant IDs dataset](https://github.com/OSUKED/Power-Station-Dictionary/blob/main/data/dictionary/ids.csv) is a one-off CSV produced by Ayrton Bourn, matching his project-defined identifier to the corresponding Elexon’s Settlement BMU ID(s). This will be used to map the above location information against a BM Unit. 

The DAG extracts the entire CSV from the URLand stores it as a parquet file, transforms it so that the unique identifier is the BMU ID and stores it as a processed parquet in the GCS bucket and loads it to BigQuery as a separate table. 

#### Fuel category mapping
This is a self-defined mapping CSV of each fuel to either one of the following energy source categories: Clean, Fossil, Nuclear and Other. 

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

<br>

---

## Pre-requisites

### git
Install git via https://git-scm.com/book/en/v2/Getting-Started-Installing-Git

### GCP

1. GCP account
Set up a GCP account via https://cloud.google.com. 

1. Enable Cloud Resource Manager API
Enable Cloud Resource Manager API [here](https://console.cloud.google.com/apis/library/cloudresourcemanager.googleapis.com). 

1. GCP credentials
Create a service account in [Service Account](https://console.cloud.google.com/iam-admin/serviceaccounts) by clicking `CREATE SERVICE ACCOUNT`.

Assign the following roles in [IAM & Admin](https://console.cloud.google.com/iam-admin) via the <img width="22" alt="image" src="https://github.com/lilychau1/uk-power-analytics/assets/58731610/00f6546a-012b-48b1-83e2-3ee607fa84ea"> icon next to your desired service account: 

      - BigQuery Admin
      - Compute Admin
      - Project IAM Admin
      - Service Account Admin
      - Service Account User
      - Storage Admin


> **_WARNING:_** As a proof of concept, the project creates a service account with the permission of 
`BigQuery Admin`, `Service Account Key Admin`, `Storage Insights Collector Service`, `Storage Object Creator` and `Storage Object Viewer`, which might not be the best security practice. Any suggestions welcome on connecting GCE with Airflow in a dockerised setting in Terraform for this specific use case. 

Generate a corresponding ssh credentials file and store it as json file named `my-creds.json` in the cloned repo under `/keys/`: 
`<repo-directory>/keys/my-creds.json`


### Terraform
Install Terraform with the following guide: [https://git-scm.com/book/en/v2/Getting-Started-Installing-Git](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli)

<br>

---

## Getting Started

### Clone Repository
Clone this repo to your local machine with one of the following commands: 

With HTTPS:
```
git clone https://github.com/lilychau1/uk-power-analytics.git
```

With ssh:
```
git clone git@github.com:lilychau1/uk-power-analytics.git
```

### Add GCP credential
If not done already, generate a corresponding ssh credentials file and store it as json file named `my-creds.json` in the cloned repo under `/keys/`: `<repo-directory>/keys/my-creds.json`

### Update .env
Create a file named `.env` in the cloned repo and add GCP bucket configuration:

`.env`: 
``` 
GCP_PROJECT_ID=<your_project_id>
GCP_ACCOUNT_ID=<your_account_id>
```

### Set up Terraform

1. Initialise Terraform with the following command:

```
terraform init
```

2. Preview the changes with the following command: 

```
terraform plan
```

3. Kick-start your project (execute the proposed plan) with the following command: 

```
terraform apply
```
A successful set up will show the following output:

```
Apply complete! Resources: 9 added, 0 changed, 0 destroyed.
```

To inspect the logs during GCP VM instance start-up, navigate to [VM Instances](https://console.cloud.google.com/compute/instances), click on `uk-power-analytics-vm`, under `Logs` section, click on `Serial port 1 (console)`. 

> **_NOTE:_** If no longer in use, destroy all remote objects to avoid unnecessary GCP costs with `terraform destroy`. This command will delete the VM instance, Terraform-created service account, GSC Bucket and Bigquery dataset specific for this project. 

### Access Airflow

Terraform has kick-started the airflow run, and the web server is already setup at `localhost:8080` of the GCP VM instance. To access it, users will need to forward the port to a specific localhost of the local machine. 

1. In the same [VM Instances](https://console.cloud.google.com/compute/instances) page, copy the External IP of the VM named `uk-power-analytics-vm`. 
<img width="1091" alt="image" src="https://github.com/lilychau1/uk-power-analytics/assets/58731610/dd580ec8-50b6-435b-915a-75bde732f7c6">

2. Go to your local terminal, insert the IP to your SSH known hosts with the following command: 

```
ssh-keyscan -H <external_ip> >> ~/.ssh/known_hosts
```

3. Connect to the airflow web server (localhost:8080) with the following command: 
```
gcloud compute ssh uk-power-analytics-vm -- -L 8080:localhost:8080 
```

### Set up spark connection on airflow
1. In your web browser, go to [http://localhost:8080/](http://localhost:8080/). Enter username `airflow` and password `airflow`.

<img width="1231" alt="image" src="https://github.com/lilychau1/uk-power-analytics/assets/58731610/766de522-bd41-4446-8520-08ecc6a4a7f2">

2. In the top navigation bar, go to Admin > Connections > + > Fill in the following: 
Connection Id: `spark-conn`
Connection Type: `spark`
Host: `spark://spark-master`
Port: `7077`

<img height="300" alt="image" src="https://github.com/lilychau1/uk-power-analytics/assets/58731610/cf98355c-88c7-49b5-8cbf-a7c33f0f0bf8">
<img height="300" alt="image" src="https://github.com/lilychau1/uk-power-analytics/assets/58731610/221d85f4-50a0-4dd2-a1b7-48b7c43f2285">
<img height="300" alt="image" src="https://github.com/lilychau1/uk-power-analytics/assets/58731610/d65109c3-04a5-4ecc-bd42-5640dc7e0f3f">


### Explore DAGs

In the DAGs tab, you will find two different workflows: `initialise_data` and `ingest_batch_data`. 
<img width="445" alt="image" src="https://github.com/lilychau1/uk-power-analytics/assets/58731610/07449e6f-f9d5-4280-9b9b-b3c55c42ce2a">

`initialise_data` is run once only, while `ingest_batch_data` is run daily. 

To initiate the data ingestion process, click both the toggle buttons <img width="45" alt="image" src="https://github.com/lilychau1/uk-power-analytics/assets/58731610/32e1807f-ddff-4b01-a9fd-778d846fe85b"> next to `initialise_data` and `ingest_batch_data`. 

To inspect the run process, click on the desired DAG name (`initialise_data` or `ingest_batch_data`. 

A successful run session will be suggested by green boxes in all steps of operations.

<img height="300" alt="image" src="https://github.com/lilychau1/uk-power-analytics/assets/58731610/86513f0b-3986-4839-8c4f-5b5a3fedfb1a"> <img height="300" alt="image" src="https://github.com/lilychau1/uk-power-analytics/assets/58731610/1ddadbf5-9aa1-4b2d-9e11-90f7d41dfb66">


After it runs successfully, you will observe the following:
1. New GCS bucket named `<your-project-id>-uk-power-analytics`: 
    * bmrs_capacity
    * bmrs_generation
    * bmrs_power_plant_info
    * power_plant_id
    * power_plant_location
    * psr_fuel_type_mapping
2. New tables in your GCS BigQuery dataset `uk_power_analytics_dataset`: 
    * bmrs_generation
    * bmrs_generation_partitioned
    * bmrs_power_plant_info
    * generation_capacity_360_view
    * power_plant_id
    * power_plant_location
    * psr_fuel_type_mapping

Every day, after `ingest_batch_data` is run at 5am, there will be incremental generation data ingested to both the GCS bucket `bmrs_generation` folder and BigQuery tables `bmrs_generation`, `bmrs_generation_partitioned` and `generation_capacity_360_view`. 

### Destroy resources
If no longer in use, destroy all remote objects to avoid unnecessary GCP costs with 

```
terraform destroy
```

This command will delete the VM instance, Terraform-created service account, GSC Bucket and Bigquery dataset specific for this project. 

<br>

---

## Data Visualisation

Google Looker Studio was used to experiment with visualising UK per unit power generation data. 

![uk_power_analytics_data_viz](https://github.com/lilychau1/uk-power-analytics/assets/58731610/09dea2f6-f47b-4097-a3cc-faf35cba3d41)

The first chart (left) depicts the main fuel category (Clean, Fossil, Nuclear, Others) of each county from 1 Feb 2024 to 27 Mar 2024. 

The second chart (top right) illustrated the total generation by fuel category and county origin of generation. 

The third chart (bottom right) showed the average unit generation output per settlement period (0 - 00:00 to 00:30, ..., 48 - 23:30 to 00:00). 

<br>

---

## Contact

If you have any questions, suggestions, or feedback regarding this project, feel free to reach out to me:

- **Name:** Lily Chau
- **GitHub:** [lilychau1](https://github.com/lilychau1/)
- **LinkedIn:** www.linkedin.com/in/lilychau1

I welcome any contributions, bug reports, or comments. Your input is valuable and helps improve the project for everyone. Thank you for your interest and support!
