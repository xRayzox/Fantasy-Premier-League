# Use official Airflow image as base
FROM apache/airflow:2.9.2-python3.9

# Set Airflow and Python versions dynamically
ARG AIRFLOW_VERSION=2.9.2
ARG PYTHON_VERSION=3.9

# Define Airflow home
ENV AIRFLOW_HOME=/opt/airflow

# Set environment variable for additional pip requirements
ENV _PIP_ADDITIONAL_REQUIREMENTS="confluent-kafka[avro] kafka-python pyspark pandas"

# Install dependencies and subpackages
RUN pip install --upgrade pip

# Switch to root user to modify file permissions
USER root

# Copy entrypoint script, set ownership, and make it executable
COPY entrypoint.sh ${AIRFLOW_HOME}/entrypoint.sh
RUN chown airflow: ${AIRFLOW_HOME}/entrypoint.sh && chmod +x ${AIRFLOW_HOME}/entrypoint.sh

# Switch back to airflow user for safer operations
USER airflow

# Install Airflow with the necessary extras and Python version constraints
RUN pip install --no-cache-dir "apache-airflow[postgres,aws,kafka,celery]==${AIRFLOW_VERSION}" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
    ${_PIP_ADDITIONAL_REQUIREMENTS}

# Expose the webserver port
EXPOSE 8080

# Use the custom entrypoint script
ENTRYPOINT ["/opt/airflow/entrypoint.sh"]

# Default command to start Airflow webserver
CMD ["webserver"]
