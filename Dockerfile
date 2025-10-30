# Dockerfile

FROM python:3.11-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the dependencies file and install them
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create a 'scripts' directory and copy all essential project files into it.
# The Airflow DAG will reference this path: /app/scripts/
RUN mkdir -p /app/scripts
COPY extract_data.py transform_data.py load_data.py .env /app/scripts/

# Set the scripts directory as the working directory for convenience
WORKDIR /app/scripts