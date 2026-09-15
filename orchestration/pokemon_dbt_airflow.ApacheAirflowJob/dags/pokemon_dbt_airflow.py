"""Orchestration DAG for pokemon_dbt_airflow — authored by Build, deployed by customer CI/CD.

Contract: docs/design/orchestration/fabric/airflow-job.md

No workspace id, item id, or credential is committed here. The workspace is
injected at apply time as an Airflow Variable; the dlt runner notebook and the
dbt job are resolved by display name over the Fabric REST API; every credential
resolves through the Airflow connection named by FABRIC_CONN_ID.

Both workloads run as their own Fabric job — the same runner notebook and the
same managed dbt job the Data Pipeline target invokes. Airflow only triggers
them, so nothing executes in the Airflow worker and there is no second dbt
execution host to configure.

orchestration_support lives in the item's plugins/ folder — Fabric discards a
non-DAG .py placed at the top level of dags/.
"""
import json
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.fabric.operators.run_item import MSFabricRunJobOperator

from orchestration_support import resolve_runner_coordinates, write_orchestration_run

# One source for the connection name: it is passed to BOTH the operator and the
# resolver. Letting the resolver fall back to its own default made the DAG look
# for a connection the operator was not using.
FABRIC_CONN_ID = "fabric_default"

# The runner's four parameters, as literal Domain coordinates — the same values
# the Data Pipeline path writes into its TridentNotebook activity, never variable
# names. Fabric passes a notebook parameter through verbatim, so a committed
# "$VD_DOMAIN_SLUG" would reach the runner as those characters.
# Sent as the run-item POST body; the operator forwards job_params unchanged.
DLT_JOB_PARAMS = json.dumps({"executionData": {"parameters": {
    "DOMAIN_SLUG": {"value": "pokemon-analytics", "type": "string"},
    "PIPELINE": {"value": "pokemon_pipeline", "type": "string"},
    "SECRET_STORE_LOCATION": {"value": "https://data-eng-key-vault.vault.azure.net/", "type": "string"},
    "CODE_LAKEHOUSE_NAME": {"value": "pokemon_wh", "type": "string"},
}}})
DLT_RUNNER_NAME = "dlt_notebook_runner"
DBT_JOB_NAME = "pokemon_dbt"

with DAG(
    dag_id="pokemon_dbt_airflow",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
) as dag:

    # Workspace and item id are resolved in their own task, never at module
    # level: everything outside a task body runs on every scheduler parse loop,
    # so a REST lookup here would be re-issued every few seconds, and a missing
    # workspace id would surface as a DAG import error rather than a task
    # failure. `workspace_id` and `item_id` are template_fields on the operator,
    # so the pulls below render at execution time.
    resolve_dlt = PythonOperator(
        task_id="resolve_dlt",
        python_callable=resolve_runner_coordinates,
        op_kwargs={"display_name": DLT_RUNNER_NAME, "item_type": "Notebook",
                   "conn_id": FABRIC_CONN_ID},
    )

    # job_type is case-sensitive. deferrable defaults to True and is not
    # restated; wait_for_termination exists only for backwards compatibility and
    # is deliberately not passed.
    dlt_load = MSFabricRunJobOperator(
        task_id="dlt_load",
        fabric_conn_id=FABRIC_CONN_ID,
        workspace_id="{{ ti.xcom_pull(task_ids='resolve_dlt')['workspace_id'] }}",
        item_id="{{ ti.xcom_pull(task_ids='resolve_dlt')['item_id'] }}",
        job_type="RunNotebook",
        job_params=DLT_JOB_PARAMS,
        timeout=3600,
    )

    resolve_dbt = PythonOperator(
        task_id="resolve_dbt",
        python_callable=resolve_runner_coordinates,
        op_kwargs={"display_name": DBT_JOB_NAME, "item_type": "DataBuildToolJob",
                   "conn_id": FABRIC_CONN_ID},
    )

    # No job_params: the committed .DataBuildToolJob carries its own operation
    # and selector in dbt-content.json, exactly as it does when a Data Pipeline
    # invokes it. Restating a selector here would let the two disagree.
    dbt_build = MSFabricRunJobOperator(
        task_id="dbt_build",
        fabric_conn_id=FABRIC_CONN_ID,
        workspace_id="{{ ti.xcom_pull(task_ids='resolve_dbt')['workspace_id'] }}",
        item_id="{{ ti.xcom_pull(task_ids='resolve_dbt')['item_id'] }}",
        job_type="DataBuildToolJob",
        timeout=3600,
    )

    record_run = PythonOperator(
        task_id="record_run",
        python_callable=write_orchestration_run,
        op_kwargs={"conn_id": FABRIC_CONN_ID,
                   "code_lakehouse_name": "pokemon_wh",
                   "domain_slug": "pokemon-analytics"},
        trigger_rule="all_done",
    )

    # The DAG run's own state is computed from its LEAF tasks. record_run is
    # all_done and swallows its failures, so with it as the only leaf a run whose
    # dlt or dbt task failed still reported `success` — run-verified, VD-5578.
    # This leaf inherits the workload's fate (default all_success trigger rule),
    # so the DAG run fails when the workload does. Never give it all_done, and
    # never make record_run the last task again.
    workload_succeeded = EmptyOperator(task_id="workload_succeeded")

    resolve_dlt >> dlt_load >> resolve_dbt >> dbt_build >> [record_run, workload_succeeded]
