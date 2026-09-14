"""Runtime helpers for a Fabric Apache Airflow Job DAG.

Contract: docs/design/orchestration/fabric/airflow-job.md

Nothing here accepts a committed workspace id, item id, or credential. The
workspace is injected at apply time as an Airflow Variable, the dlt runner
notebook and the dbt job are resolved by display name, and credentials resolve
through the Airflow connection named by VD_FABRIC_CONN_ID.

Copied verbatim into every materialized item — it carries no placeholders,
because every value it needs is resolved at run time.

Lives in the item's plugins/ folder, NOT dags/: Fabric silently discards a
top-level .py in dags/ that defines no DAG, and Airflow puts plugins/ on
sys.path (both verified live, VD-5578).
"""
import json
import logging
import os
import re
from datetime import datetime, timezone

log = logging.getLogger(__name__)

ORCHESTRATION_RUNS_TABLE = "orchestration_runs"
FABRIC_API_HOST = "https://api.fabric.microsoft.com"
FABRIC_API_SCOPE = "https://api.fabric.microsoft.com/.default"
ONELAKE_BLOB_HOST = "https://onelake.blob.fabric.microsoft.com"
ONELAKE_SCOPE = "https://storage.azure.com/.default"
RECORD_RUN_TASK_ID = "record_run"
DLT_TASK_ID = "dlt_load"
DBT_TASK_ID = "dbt_build"

# MSFabricRunJobOperator pushes NO return_value. It pushes discrete XCom keys —
# run_id, item_id, item_name, item_type, run_status (and run_link for a
# notebook) — and the Fabric job instance id is run_id. Pulling "return_value"
# returns None on every run and silently empties the fields this relation exists
# to carry; run-verified against a fully green run (VD-5578), which is the only
# kind of run that can expose it.
JOB_INSTANCE_XCOM_KEY = "run_id"
ITEM_ID_XCOM_KEY = "item_id"


WORKSPACE_ID_VAR = "vd_fabric_workspace_id"


def current_workspace_id():
    """The workspace this Airflow Job runs in, from an Airflow Variable.

    Fabric publishes no workspace id to the DAG at all — verified live by dumping
    the whole environment: the only FABRIC_* entries are the secret backend's URL
    and scope, AZURE_CLIENT_ID is all zeros, and AZURE_TENANT_ID is Fabric's own
    service tenant. The item's `airflowEnvironmentVariables` were also verified
    NOT to reach the worker's task process, so that route is unavailable.

    An Airflow Variable is the remaining mechanism, and the better one: it is
    stored in the Airflow metadata DB, so the apply step can set it over the
    Airflow REST API and it takes effect immediately — where a requirements or
    environment change costs a full rebuild measured in tens of minutes.

    Set per environment, never committed. There is deliberately no fallback: a
    wrong workspace id would run the DAG against someone else's data rather than
    fail.
    """
    from airflow.models import Variable

    workspace_id = Variable.get(WORKSPACE_ID_VAR, default_var=None)
    if not workspace_id:
        raise RuntimeError(
            f"Airflow Variable {WORKSPACE_ID_VAR!r} is not set; the apply step must set it "
            "to the target workspace id (Admin > Variables, or the Airflow REST API). "
            "The DAG must not guess one."
        )
    return workspace_id


def resolve_item_id(display_name, item_type="Notebook", conn_id=None):
    """Resolve a committed item's id by display name, over the Fabric REST API.

    Name-indirection is what lets one committed DAG serve every workspace: the
    runner's name is a Domain coordinate, its id is not.

    The provider offers no name-to-id lookup of its own — its only item helper
    resolves the other direction, id to name — so this calls the list endpoint
    directly, borrowing the provider connection purely for the access token.
    """
    import requests
    from airflow.providers.microsoft.fabric.hooks.connection.rest_connection import (
        MSFabricRestConnection,
    )

    connection = MSFabricRestConnection(
        conn_id=conn_id or os.environ.get("VD_FABRIC_CONN_ID", "fabric_default")
    )
    headers = connection.get_headers(FABRIC_API_SCOPE)
    workspace_id = current_workspace_id()

    response = requests.get(
        f"{FABRIC_API_HOST}/v1/workspaces/{workspace_id}/items",
        params={"type": item_type},
        headers=headers,
        timeout=60,
    )
    response.raise_for_status()

    matches = [
        item for item in response.json().get("value", [])
        if item.get("displayName") == display_name
    ]
    if not matches:
        raise RuntimeError(
            f"no {item_type} named {display_name!r} in workspace {workspace_id}; "
            "the committed DAG references an item this workspace does not hold"
        )
    if len(matches) > 1:
        raise RuntimeError(
            f"{len(matches)} items named {display_name!r} in workspace {workspace_id}; "
            "the name must be unique for id resolution to be deterministic"
        )
    return matches[0]["id"]


def resolve_runner_coordinates(display_name, item_type="Notebook", conn_id=None, **_):
    """Resolve the runner's workspace and item id, as a task.

    Returned through XCom so the operator's templated `workspace_id` and
    `item_id` render at execution time. Calling `resolve_item_id` directly in an
    operator argument would run it on every scheduler parse loop instead, and
    would turn a missing workspace id into a DAG import error rather than a task
    failure.
    """
    return {
        "workspace_id": current_workspace_id(),
        "item_id": resolve_item_id(display_name, item_type=item_type, conn_id=conn_id),
    }


def write_orchestration_run(conn_id=None, code_lakehouse_name=None, domain_slug=None,
                            **context):
    """Write one orchestration_runs row correlating this DAG run to its workloads.

    Evidence, never a gate: this task carries an all-done trigger rule and this
    function swallows its own failures, so a recording problem can never turn a
    successful workload into a failed run.
    """
    try:
        task_instance = context["ti"]
        dag_run = context["dag_run"]

        dbt_job_instance_id = task_instance.xcom_pull(
            task_ids=DBT_TASK_ID, key=JOB_INSTANCE_XCOM_KEY)
        dbt_item_id = task_instance.xcom_pull(
            task_ids=DBT_TASK_ID, key=ITEM_ID_XCOM_KEY)
        log.info("correlating dag_run=%s dlt_job=%s dbt_job=%s dbt_item=%s",
                 dag_run.run_id,
                 task_instance.xcom_pull(task_ids=DLT_TASK_ID, key=JOB_INSTANCE_XCOM_KEY),
                 dbt_job_instance_id, dbt_item_id)
        row = {
            "dag_id": dag_run.dag_id,
            "dag_run_id": dag_run.run_id,
            "dbt_job_instance_id": dbt_job_instance_id,
            "dbt_invocation_id": _dbt_invocation_id(
                dbt_job_instance_id, item_id=dbt_item_id, conn_id=conn_id),
            "dlt_job_instance_id": task_instance.xcom_pull(
                task_ids=DLT_TASK_ID, key=JOB_INSTANCE_XCOM_KEY),
            "git_sha": os.environ.get("VD_GIT_SHA"),
            "status": _workload_status(dag_run),
            "failed_tasks": _failed_task_ids(dag_run),
            "started_at": dag_run.start_date,
            "ended_at": datetime.now(timezone.utc),
        }
        _insert_run_row(row, code_lakehouse_name=code_lakehouse_name,
                        domain_slug=domain_slug, conn_id=conn_id)
        log.info("recorded orchestration run %s", row["dag_run_id"])
    except Exception:
        log.exception(
            "failed to record the orchestration run; the workload result is unaffected"
        )


def _dbt_invocation_id(job_instance_id, item_id=None, conn_id=None):
    """dbt's own invocation id, read from the managed job's own run output.

    dbt runs as its own Fabric job here, not in the Airflow worker, so there is
    no local target/ to read. The job writes its artifacts to the item's OneLake
    storage under Output/<job instance id>/target/, which is the same
    deterministic chain get-dbtjob-run-evidence.py follows on the Data Pipeline
    path — the job instance id, never a timestamp match.

    The invocation id is the join key every mart already stamps as
    _dbt_invocation_id, so the row carries it alongside the job instance id
    rather than instead of it. A failure here returns None and is logged: this
    is evidence, and the caller must not fail the run over it.
    """
    if not job_instance_id or not item_id:
        log.warning("dbt job instance or item id unavailable; invocation id not recorded")
        return None

    import requests
    from airflow.providers.microsoft.fabric.hooks.connection.rest_connection import (
        MSFabricRestConnection,
    )

    connection = MSFabricRestConnection(
        conn_id=conn_id or os.environ.get("VD_FABRIC_CONN_ID", "fabric_default")
    )
    url = (
        f"{ONELAKE_BLOB_HOST}/{current_workspace_id()}/{item_id}"
        f"/Output/{job_instance_id}/target/run_results.json"
    )
    try:
        response = requests.get(
            url, headers=connection.get_headers(ONELAKE_SCOPE), timeout=60
        )
        response.raise_for_status()
        return response.json().get("metadata", {}).get("invocation_id")
    except Exception:  # noqa: BLE001 — evidence, never a gate
        log.warning("could not read run_results.json for dbt job instance %s",
                    job_instance_id)
        return None


def _peer_task_instances(dag_run):
    """Every task instance in this run except the recorder itself."""
    return [ti for ti in dag_run.get_task_instances()
            if ti.task_id != RECORD_RUN_TASK_ID]


def _failed_task_ids(dag_run):
    return sorted(ti.task_id for ti in _peer_task_instances(dag_run)
                  if ti.state in ("failed", "upstream_failed"))


def _workload_status(dag_run):
    """The workload's outcome, derived from its peers.

    `dag_run.get_state()` is read while this task is still executing, so it is
    always "running" and records nothing. The workload's real outcome is whether
    any non-recorder task failed.
    """
    peers = _peer_task_instances(dag_run)
    if not peers:
        return "unknown"
    if any(ti.state in ("failed", "upstream_failed") for ti in peers):
        return "failed"
    if all(ti.state in ("success", "skipped") for ti in peers):
        return "success"
    return "partial"


def _run_blob_name(dag_run_id):
    """A dag_run_id contains `:` and `+`, which do not belong in a blob path."""
    return re.sub(r"[^A-Za-z0-9._-]", "-", dag_run_id) + ".json"


def _insert_run_row(row, code_lakehouse_name=None, domain_slug=None, conn_id=None):
    """Append the run as one JSON document in OneLake, for dbt to expose.

    Deliberately NOT a TDS insert from the worker. Nothing else on this target
    executes in the Airflow worker — that is the whole reason Cosmos was dropped
    — and a direct warehouse write would put a SQL driver and a warehouse
    credential back in it, for one row. The worker already holds a storage token
    and already reads the dbt job's own run output through it, so this reuses a
    path that is proven rather than opening a second one.

    One immutable document per DAG run, at

        Files/<domain slug>/orchestration_runs/<dag id>/<dag run id>.json

    in the code Lakehouse — the same Lakehouse the dlt runner reads its code
    from, named by the DAG rather than resolved from a committed id. The domain's
    dbt project exposes that folder as the `orchestration_runs` relation; a mart
    joins to it on `dbt_invocation_id`, which every mart already stamps.
    """
    if not code_lakehouse_name or not domain_slug:
        raise RuntimeError(
            "record_run needs code_lakehouse_name and domain_slug to place the row; "
            "the DAG passes both as op_kwargs"
        )

    import requests
    from airflow.providers.microsoft.fabric.hooks.connection.rest_connection import (
        MSFabricRestConnection,
    )

    workspace_id = current_workspace_id()
    lakehouse_id = resolve_item_id(code_lakehouse_name, item_type="Lakehouse", conn_id=conn_id)
    connection = MSFabricRestConnection(
        conn_id=conn_id or os.environ.get("VD_FABRIC_CONN_ID", "fabric_default")
    )
    url = (
        f"{ONELAKE_BLOB_HOST}/{workspace_id}/{lakehouse_id}/Files/{domain_slug}"
        f"/{ORCHESTRATION_RUNS_TABLE}/{row['dag_id']}/{_run_blob_name(row['dag_run_id'])}"
    )
    headers = {
        **connection.get_headers(ONELAKE_SCOPE),
        "x-ms-blob-type": "BlockBlob",
        "Content-Type": "application/json",
    }
    # default=str renders the timestamps; they are datetimes, not strings.
    body = json.dumps(row, default=str, indent=1).encode()
    response = requests.put(url, data=body, headers=headers, timeout=60)
    response.raise_for_status()
    log.info("wrote %s", url)
