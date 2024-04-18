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

# 4. Create service account key
PROJECT_ID=$(gcloud config get-value project)
SERVICE_ACCOUNT_EMAIL="${AIRFLOW_SERVICE_ACCOUNT_ID}@$PROJECT_ID.iam.gserviceaccount.com"
GOOGLE_CREDENTIAL_FOLDER="uk_power_analytics/airflow_workflows/.google"
GOOGLE_CREDENTIAL_PATH="credentials/google_credentials.json"

gcloud iam service-accounts keys create "$GOOGLE_CREDENTIAL_FOLDER/$GOOGLE_CREDENTIAL_PATH" --iam-account="$SERVICE_ACCOUNT_EMAIL"
chmod -R 755 "$GOOGLE_CREDENTIAL_FOLDER"
chmod 644 "$GOOGLE_CREDENTIAL_FOLDER/$GOOGLE_CREDENTIAL_PATH"

# 5. Get environment variables: GCP_PROJECT_ID, AIRFLOW_UID, _PIP_ADDITIONAL_REQUIREMENTS
export GCP_PROJECT_ID=$(curl "http://metadata.google.internal/computeMetadata/v1/instance/attributes/GCP_PROJECT_ID" -H "Metadata-Flavor: Google")
export GCP_GCS_BUCKET="$GCP_PROJECT_ID-${GCS_BUCKET_SUFFIX}"
export AIRFLOW_UID=$(curl "http://metadata.google.internal/computeMetadata/v1/instance/attributes/AIRFLOW_UID" -H "Metadata-Flavor: Google")
export _PIP_ADDITIONAL_REQUIREMENTS=$(curl "http://metadata.google.internal/computeMetadata/v1/instance/attributes/_PIP_ADDITIONAL_REQUIREMENTS" -H "Metadata-Flavor: Google")

# 6. Write environment variables to .env file
echo "GCP_PROJECT_ID=$GCP_PROJECT_ID" > uk_power_analytics/airflow_workflows/.env
echo "GCP_GCS_BUCKET=$GCP_GCS_BUCKET" >> uk_power_analytics/airflow_workflows/.env
echo "AIRFLOW_UID=$AIRFLOW_UID" >> uk_power_analytics/airflow_workflows/.env
echo "_PIP_ADDITIONAL_REQUIREMENTS=$_PIP_ADDITIONAL_REQUIREMENTS" >> uk_power_analytics/airflow_workflows/.env

# 7. Create folders for proper volume mounting
cd uk_power_analytics/airflow_workflows

mkdir -p power_data/initialise/bmrs_generation
mkdir -p power_data/initialise/bmrs_capacity
mkdir -p power_data/batch/bmrs_generation
mkdir -p power_data/batch/bmrs_capacity

# 8. Set permissions
# Change group ownership
chown -R "$(whoami)" dags logs plugins power_data
# Grant write permissions to the group
chmod -R g+w dags logs plugins power_data

# 9. Build docker image
docker build --no-cache -t airflow-uk-power-analytics .

# 10. Build containers
docker compose up
