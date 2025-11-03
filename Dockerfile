# Dockerfile

# 1. Base Airflow Image
FROM apache/airflow:2.8.1

# 2. Install Airflow Providers (CRITICAL FIX)
# Install the Docker provider for the DockerOperator
RUN pip install --no-cache-dir apache-airflow-providers-docker==3.8.1

# 3. Install Python Dependencies (for ETL scripts)
# Assuming requirements.txt contains: pandas, psycopg2-binary, requests, python-dotenv
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# 4. Set Airflow Home (Standard practice)
ENV AIRFLOW_HOME=/opt/airflow

# 5. Create necessary directories and copy scripts
# CRITICAL FIX: Create the 'scripts' directory inside the Airflow HOME
# This ensures the 'airflow' user has permission to write files.
RUN mkdir -p ${AIRFLOW_HOME}/scripts

# Copy ETL scripts and .env to the new, writable path
COPY extract_data.py transform_data.py load_data.py .env ${AIRFLOW_HOME}/scripts/

# Set working directory for ETL scripts
# The DAG will now target the scripts in this location: /opt/airflow/scripts
WORKDIR ${AIRFLOW_HOME}/scripts