# airflow_dags/movie_etl_dag.py

from airflow.models.dag import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta

# --- Configuration ---
# The name of the custom image we built (movie-etl-worker:latest)
WORKER_IMAGE = "movie-etl-worker:latest" 
# The directory *inside the worker container* where the scripts are mounted
SCRIPT_DIR = "/app/scripts/" 
# The volume mount defined in docker-compose.yaml, needed for the DockerOperator
HOST_VOLUME = "/usr/local/airflow/dags:/opt/airflow/dags" 
# CRITICAL: This allows the worker to reach your local PostgreSQL server
HOST_NETWORK = "host" 

with DAG(
    dag_id="tmd_movie_etl_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval=timedelta(hours=24),
    catchup=False,
    tags=["etl", "tmdb", "data-engineering"],
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    }
) as dag:
    
    extract_task = DockerOperator(
        task_id="extract_data_from_api",
        image=WORKER_IMAGE,
        command=f"python {SCRIPT_DIR}extract_data.py", 
        network_mode=HOST_NETWORK, # Allows reaching host.docker.internal
        volumes=[f"./:/app/scripts"], # Mount the project directory to the container
    )

    transform_task = DockerOperator(
        task_id="transform_data_filter_popularity",
        image=WORKER_IMAGE,
        command=f"python {SCRIPT_DIR}transform_data.py",
        network_mode=HOST_NETWORK,
        volumes=[f"./:/app/scripts"],
    )
    
    load_task = DockerOperator(
        task_id="load_data_to_postgres",
        image=WORKER_IMAGE,
        command=f"python {SCRIPT_DIR}load_data.py",
        network_mode=HOST_NETWORK,
        volumes=[f"./:/app/scripts"],
    )

    # Define the execution order
    extract_task >> transform_task >> load_task