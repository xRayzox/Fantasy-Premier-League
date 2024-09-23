#!/bin/bash
set -e

cd $AIRFLOW_HOME

# Disable loading example DAGs and set necessary configurations
export AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION='true'
export AIRFLOW__CORE__LOAD_EXAMPLES='false'
export AIRFLOW__API__AUTH_BACKENDS='airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session'
export AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK='true'
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow_db
export AIRFLOW__CORE__EXECUTOR=LocalExecutor
export AIRFLOW__WEBSERVER__SECRET_KEY=553bb2cf226ca1dcb336fa9032fb93bb

# Check if the Airflow DB is already initialized, else initialize
airflow db init

# Create an admin user if not exists
airflow users create \
    --username "airflow" \
    --firstname "Airflow" \
    --lastname "Admin" \
    --role "Admin" \
    --email "admin@airflow.com" \
    --password "airflow" || echo "User airflow already exists, skipping creation."

# Start the Airflow scheduler in the background and log output
airflow scheduler &> $AIRFLOW_HOME/logs/scheduler.log &

# Start the Airflow webserver in the foreground (to capture logs)
exec airflow webserver
