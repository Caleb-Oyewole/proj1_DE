# airflow_dags/movie_etl_dag.py
from airflow.models.dag import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta

WORKER_IMAGE_NAME = "movie-etl-worker:latest"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='tmd_movie_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline using the DockerOperator',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['docker', 'etl'],
) as dag:
    
    # CRITICAL: Volume Mounts for DockerOperator
    # We use a simple relative path mount: ./ (project root) -> /app/scripts (worker WORKDIR)
    PROJECT_MOUNT = './:/app/scripts'
    
    # Environment variables for the DockerOperator
    # CRITICAL: DB_HOST MUST be 'postgres' (the service name) for inter-container communication
    docker_env = {
        "DB_HOST": "postgres",
        # Pass through other environment variables (API Key, etc. are handled by env_file in worker)
    }
    
    docker_operator_kwargs = dict(
        image=WORKER_IMAGE_NAME,
        auto_remove=True,
        volumes=[PROJECT_MOUNT],
        # CRITICAL: Use the same network as the Airflow and Postgres containers
        network_mode="container:airflow_scheduler", # Use the scheduler's network to talk to DB
        environment=docker_env,
    )

    # 1. Extract Data
    extract_task = DockerOperator(
        task_id='extract_data_from_api',
        command="python extract_data.py", 
        **docker_operator_kwargs,
    )

    # 2. Transform Data
    transform_task = DockerOperator(
        task_id='transform_and_filter_data',
        command="python transform_data.py",
        **docker_operator_kwargs,
    )

    # 3. Load Data
    load_task = DockerOperator(
        task_id='load_data_to_postgres',
        command="python load_data.py",
        **docker_operator_kwargs,
    )

    # Set the task dependencies
    extract_task >> transform_task >> load_task