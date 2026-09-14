"""
Pokemon daily orchestration DAG
Runs at 6am UTC daily:
  1. Loads Pokemon data via dlt pipeline
  2. Transforms data with dbt models
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable

# Default arguments for all tasks
default_args = {
    "owner": "pokemon_analytics",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1, tzinfo=None),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    "pokemon_daily",
    default_args=default_args,
    description="Daily Pokemon load and transformation",
    schedule_interval="0 6 * * *",  # 6am UTC daily
    catchup=False,
    tags=["pokemon", "daily"],
)

# Task 1: Run dlt pipeline to load Pokemon data
run_dlt_pipeline = BashOperator(
    task_id="load_pokemon_data",
    bash_command="""
    cd /workspace && \
    python -m dlt pipeline run ingestion.pokemon_pipeline
    """,
    dag=dag,
)

# Task 2: Run dbt build to transform data
run_dbt_build = BashOperator(
    task_id="transform_pokemon_data",
    bash_command="""
    cd /workspace/transformation && \
    dbt build --profiles-dir . --project-dir .
    """,
    dag=dag,
)

# Set up task dependencies
run_dlt_pipeline >> run_dbt_build
