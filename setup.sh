cd airflow_workflows

# Create folders for proper volume mounting
mkdir -p power_data/initialise/bmrs_generation
mkdir -p power_data/initialise/bmrs_capacity
mkdir -p power_data/batch/bmrs_generation
mkdir -p power_data/batch/bmrs_capacity

# Build docker image
docker build --no-cache -t airflow-power-analytics .