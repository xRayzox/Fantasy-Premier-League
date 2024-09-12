FROM python:3.9-slim

# Arguments that can be set with docker build
ARG AIRFLOW_VERSION=2.10.1
ARG AIRFLOW_HOME=/opt/airflow

# Export the environment variable AIRFLOW_HOME where airflow will be installed
ENV AIRFLOW_HOME=${AIRFLOW_HOME}
USER root

# Install dependencies and tools
RUN apt-get update && \
    apt-get install -y \
    python3-dev \
    wget \
    curl \
    bash \
    git \
    build-essential \
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Upgrade pip and install dependencies
RUN pip install --upgrade pip && \
    useradd -ms /bin/bash -d ${AIRFLOW_HOME} airflow && \
    PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')" && \
    CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" && \
    pip install apache-airflow==${AIRFLOW_VERSION} --constraint ${CONSTRAINT_URL} && \
    pip install apache-airflow-providers-apache-spark==2.0.1 && \
    pip install apache-airflow-providers-slack==4.0.1 && \
    pip install apache-airflow-providers-http==2.0.1 && \
    pip install kafka-python && \
    pip install pyspark

# Ensure permissions for directories
RUN mkdir -p /opt/bitnami/spark/tmp && \
    chmod -R 777 /opt/bitnami/spark/tmp && \
    mkdir -p ${AIRFLOW_HOME}/dags && \
    chmod -R 777 ${AIRFLOW_HOME}/dags

# Copy the entrypoint.sh from host to container (at path AIRFLOW_HOME)
COPY ./start-airflow.sh ${AIRFLOW_HOME}/start-airflow.sh

# Set the owner of the files in AIRFLOW_HOME to the user airflow
RUN chown -R airflow: ${AIRFLOW_HOME}

# Set the entrypoint.sh file to be executable
RUN chmod +x ${AIRFLOW_HOME}/start-airflow.sh

# Switch to airflow user
USER airflow

# Expose ports (just to indicate that this container needs to map port)
EXPOSE 8080

# Execute start-airflow.sh
CMD ["./start-airflow.sh"]
