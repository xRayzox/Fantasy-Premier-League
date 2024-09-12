#!/usr/bin/env bash

# Move to the AIRFLOW HOME directory
cd $AIRFLOW_HOME

export AIRFLOW__CORE__LOAD_EXAMPLES=False

# Ensure permissions for Spark directories
chmod -R 755 /opt/bitnami/spark/tmp

# Initialize the metadatabase
airflow db init

# Create User
airflow users create -e "admin@airflow.com" -f "airflow" -l "airflow" -p "airflow" -r "Admin" -u "airflow"

# Run the scheduler in background
airflow scheduler &> /dev/null &

# Run the web server in foreground (for docker logs)
exec airflow webserver
