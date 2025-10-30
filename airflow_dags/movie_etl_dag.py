# airflow_dags/movie_etl_dag.py

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from datetime import timedelta

# Set the path to the directory containing your ETL scripts inside the Docker container
# This path is mapped in docker-compose.yaml as the project root (.:/app/scripts)
PYTHON_SCRIPT_DIR = "/app/scripts/"
PYTHON_CMD = "python " + PYTHON_SCRIPT_DIR 

with DAG(
    dag_id="tmd_movie_etl_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval=timedelta(hours=24), # Run once a day
    catchup=False,
    tags=["etl", "tmdb", "data-engineering"],
    default_args={
        "owner": "airflow",
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    }
) as dag:
    
    extract_task = BashOperator(
        task_id="extract_data_from_api",
        bash_command=f"{PYTHON_CMD}extract_data.py", 
    )

    transform_task = BashOperator(
        task_id="transform_data_filter_popularity",
        bash_command=f"{PYTHON_CMD}transform_data.py",
    )
    
    load_task = BashOperator(
        task_id="load_data_to_postgres",
        bash_command=f"{PYTHON_CMD}load_data.py",
    )

    # Define the execution order
    extract_task >> transform_task >> load_task