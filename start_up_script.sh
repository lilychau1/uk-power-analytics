#!/bin/bash

# 1. Create a folyesr named uk_power_analytics
sudo mkdir -p uk_power_analytics

# 2. Install Git and Docker Compose

sudo apt-get update
sudo sudo apt-get install git

# Add Docker's official GPG key:
sudo apt-get install ca-certificates curl
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc

# Add the repository to Apt sources:
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update

yes | sudo apt-get install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# 3. Git clone a GitHub repo
git clone https://github.com/lilychau1/uk-power-analytics.git uk_power_analytics

# 4. Get environment variables: GCP_PROJECT_ID, AIRFLOW_UID, _PIP_ADDITIONAL_REQUIREMENTS
export GCP_PROJECT_ID=$(curl "http://metadata.google.internal/computeMetadata/v1/instance/attributes/GCP_PROJECT_ID" -H "Metadata-Flavor: Google")
export AIRFLOW_UID=$(curl "http://metadata.google.internal/computeMetadata/v1/instance/attributes/AIRFLOW_UID" -H "Metadata-Flavor: Google")
export _PIP_ADDITIONAL_REQUIREMENTS=$(curl "http://metadata.google.internal/computeMetadata/v1/instance/attributes/_PIP_ADDITIONAL_REQUIREMENTS" -H "Metadata-Flavor: Google")
export _AIRFLOW_WWW_USER_USERNAME=$(curl "http://metadata.google.internal/computeMetadata/v1/instance/attributes/_AIRFLOW_WWW_USER_USERNAME" -H "Metadata-Flavor: Google")
export _AIRFLOW_WWW_USER_PASSWORD=$(curl "http://metadata.google.internal/computeMetadata/v1/instance/attributes/_AIRFLOW_WWW_USER_PASSWORD" -H "Metadata-Flavor: Google")

# 5. Write environment variables to .env file
echo "GCP_PROJECT_ID=${GCP_PROJECT_ID}" > uk_power_analytics/airflow_workflows/.env
echo "AIRFLOW_UID=${AIRFLOW_UID}" >> uk_power_analytics/airflow_workflows/.env
echo "_PIP_ADDITIONAL_REQUIREMENTS=${_PIP_ADDITIONAL_REQUIREMENTS}" >> uk_power_analytics/airflow_workflows/.env
echo "_AIRFLOW_WWW_USER_USERNAME=${_AIRFLOW_WWW_USER_USERNAME}" >> uk_power_analytics/airflow_workflows/.env
echo "_AIRFLOW_WWW_USER_PASSWORD=${_AIRFLOW_WWW_USER_PASSWORD}" >> uk_power_analytics/airflow_workflows/.env

# 5. Build image
cd uk_power_analytics
./setup.sh

# 6. Build containers
./run.sh

