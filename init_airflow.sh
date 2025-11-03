#!/usr/bin/env bash
# init_airflow.sh

# Wait for the PostgreSQL service to be available
echo "Waiting for PostgreSQL to be ready..."
/usr/bin/dumb-init bash -c '
    until pg_isready -h postgres -p 5432; do
        sleep 1
    done
'

# Initialize the Airflow database (Migrate)
airflow db migrate

# Create the Airflow user if it doesn't exist
airflow users create \
    --username airflow \
    --firstname Airflow \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password airflow

echo "Airflow initialization complete."