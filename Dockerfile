# Dockerfile for Airflow

FROM puckel/docker-airflow:latest

# Copy the wait-for-it.sh script into the container
COPY wait-for-it.sh /usr/local/airflow/wait-for-it.sh

# Make the script executable
RUN chmod +x /usr/local/airflow/wait-for-it.sh