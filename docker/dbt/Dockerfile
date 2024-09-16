# Use a base image with Python
FROM python:3.10-slim

# Set environment variables to reduce output verbosity
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev && \
    rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR /app

# Install dbt with postgres adapter (or other adapters you need)
RUN pip install dbt-core dbt-postgres

# Optional: Add dbt project files if necessary
# COPY . .

# Set entry point (optional, dbt can be run via 'docker-compose run')
ENTRYPOINT ["dbt"]
