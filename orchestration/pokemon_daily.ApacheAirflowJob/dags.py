"""
Pokemon Analytics Daily Orchestration — 6am UTC

Runs pokemon_pipeline.py (dlt, bronze load) → dbt build (staging + marts).
Logs to stdout; Fabric captures audit events and task outcomes.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import sys
import os

# DAG parameters
DAG_ID = "pokemon_daily"
SCHEDULE_INTERVAL = "0 6 * * *"  # 6am UTC daily
DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
    "email_on_failure": False,
}

dag = DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    schedule_interval=SCHEDULE_INTERVAL,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["pokemon", "bronze", "transformation", "fabric"],
)


def run_dlt_pipeline(**context):
    """Run the pokemon_pipeline.py dlt workload."""
    import subprocess
    
    result = subprocess.run(
        [sys.executable, "ingestion/pokemon_pipeline.py"],
        cwd="/workspace",
        capture_output=True,
        text=True,
        check=False,
    )
    
    print(f"DLT Pipeline stdout:\n{result.stdout}")
    if result.stderr:
        print(f"DLT Pipeline stderr:\n{result.stderr}")
    
    if result.returncode != 0:
        raise RuntimeError(f"DLT pipeline failed with exit code {result.returncode}")
    
    return {"status": "success", "exit_code": result.returncode}


task_dlt = PythonOperator(
    task_id="run_dlt_pokemon_load",
    python_callable=run_dlt_pipeline,
    dag=dag,
)

task_dbt = BashOperator(
    task_id="run_dbt_build",
    bash_command="cd /workspace/transformation && dbt build --profiles-dir .",
    dag=dag,
)

# Set dependency: dbt runs after dlt
task_dlt >> task_dbt
