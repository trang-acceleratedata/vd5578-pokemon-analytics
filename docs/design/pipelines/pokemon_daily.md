---
artifacts: [pokemon_daily]
---

# pokemon_daily

Durable design record for the Pokemon Analytics **Fabric Apache Airflow Job** that runs the PokeAPI dlt load and the `pokemon_analytics` dbt project as one daily unit.

Native items authored under this design:

- `orchestration/pokemon_daily.ApacheAirflowJob/` — the DAG, `apacheairflowjob-content.json`, `plugins/orchestration_support.py`, `.platform`.
- `orchestration/pokemon_daily.DataBuildToolJob/` — the managed dbt job the DAG triggers (`dbt-content.json`, config only; the packaged dbt project is added at apply time).
- `orchestration/pokemon_dlt_runner.Notebook/` — the shared dlt runner notebook the DAG triggers. Reused byte-for-byte from the Data Pipeline path per ADR-0026.

The orchestration playbook and `_shared/references/dialects/fabric/airflow-job.md` are cited by section; nothing here restates them.

## Grain

One DAG run per calendar day, triggered once at 06:00 UTC in production. One row (`dag_id`, `dag_run_id`) per run; each run refreshes `bronze.pokemon` with `write_disposition=replace` and rebuilds every downstream dbt model (`stg_pokemon`, `pokemon_weight_bands`). A second run in the same day is prevented by `max_active_runs=1` at the DAG and by the daily cron at the customer's CI-managed schedule.

## Decisions

- **Orchestration target = Apache Airflow Job** — the user named Airflow as the domain's standard (R-01@1). The precondition — an `azure_key_vault` Domain — is satisfied (`VD_DOMAIN_SECRET_STORE_KIND=azure_key_vault`). Recorded per playbook: the target is per-domain, and once this item is committed under `orchestration/` no later intent may commit a `.DataPipeline` alongside it.
- **One DAG, two Fabric-native jobs it triggers** — one `dlt_load` task (`MSFabricRunJobOperator`, `job_type="RunNotebook"`) followed by one `dbt_build` task (`MSFabricRunJobOperator`, `job_type="DataBuildToolJob"`, **no** `job_params`), each preceded by its own `resolve_*` name-lookup task per ADR-0026's coordinate-injection rule. The DAG's terminal state must reflect the workload's fate, so `workload_succeeded` is the DAG's leaf; `record_run` is `all_done` and swallows failure and cannot be the leaf (validator's `recorder-only-leaf` rule) (R-02@1).
- **Committed `schedule=None`, cadence applied at deployment** — the committed DAG carries `schedule=None`, `catchup=False`, `max_active_runs=1`. The daily 06:00 UTC production cadence is applied by customer CI/CD against the deployed DAG (Airflow REST API, portal, or the customer's own deployment automation); nothing in the repository ever activates a schedule, and the ephemeral import never carries one. This satisfies the `committed-active-schedule` validator rule and preserves the Data Pipeline / Airflow parity established by ADR-0026 (R-03@2). The intended cadence — `0 6 * * *` UTC — is recorded here so a customer CI/CD reader has one authoritative source, alongside `Consumers` below.
- **DAG name = `pokemon_daily`** — snake_case, matches the item directory prefix `pokemon_daily.ApacheAirflowJob/` and `pokemon_daily.DataBuildToolJob/`, and reads as its own purpose (daily Pokemon refresh) without duplicating "airflow" or "orchestration". Same name is used as the item display name, DAG id, and Fabric job display name so the correlation surface has one identifier.
- **dbt selector = whole project (`dbt build` with no `--select`)** — the project has only `stg_pokemon` (view) and `pokemon_weight_bands` (table); a selector would add drift risk (`_shared/references/dialects/fabric/airflow-job.md` warns that restating the selector in the DAG lets the two disagree — the same principle applies to restating a partial selector in the job when the whole project is the intended set). Rerun order is dbt's own (stg → mart). Recorded in the committed `dbt-content.json`, never in the DAG.
- **Airflow connection id string = `fabric_default`** — the template default. The connection GUID is environment-specific and injected at apply time (Airflow Variable `vd_fabric_workspace_id` plus the per-environment `fabric_default` connection created by whoever deploys). The DAG carries only the name string; the validator's `committed-guid` rule enforces that.
- **dlt runner reused by display name = `pokemon_dlt_runner`** — one Notebook item shared by every orchestration in this domain, materialized once here if not already committed. The DAG resolves it by display name via `resolve_dlt`; the runner itself carries no environment binding (see `materialize-dlt-runner.py`), so the same committed item is correct in the ephemeral workspace and in production.
- **Per-run timeout = 3600s (1h)** — Fabric's own form default of 600s is too short for the dlt load (50 pokemon detail calls plus warehouse writes), and the operator's own 3600s is enough on this workload profile. Recorded explicitly in the materialize call rather than inherited.
- **Correlation write = OneLake JSON per run** — the template's `record_run` writes `Files/<domain-slug>/orchestration_runs/<dag_id>/<dag_run_id>.json`, i.e. `Files/pokemon-analytics/orchestration_runs/pokemon_daily/<run_id>.json`. The sandbox run may be `spn`-connection-only for the row to land (ADR-0026); a `token`-only run still runs green but the correlation row does not write. This is an acknowledged sandbox-verification caveat, not a defect (R-04@1).

## Rejected

- **Data Pipeline target.** Playbook default and general recommendation, but the user explicitly named Airflow as the standard. Recording so a later intent does not re-propose migrating: swapping target is out of scope for a single intent per the playbook.
- **Multiple DAGs (one per workload).** Rejected — dlt and dbt share one interval, one owner, one failure boundary, and one recovery contract. The failure isolation gained from splitting would create a two-DAG dependency that Airflow's cross-DAG signalling handles worse than a two-task DAG.
- **Astronomer Cosmos rendering one Airflow task per dbt model in the worker.** Rejected by ADR-0026 (crashlooped Fabric Celery worker, would have required packaging the dbt project into `dags/`). This design commits nothing dbt-flavoured to `airflowRequirements` beyond the Fabric provider, and the validator's `worker-side-dbt` rule enforces it.
- **`BashOperator`-based DAG legs.** Rejected — the "shell-shaped" workaround the validator's `worker-side-workload` rule exists to catch; produces name-shaped compliance with no real orchestration.
- **Committing the daily cron on `schedule=`.** Rejected — would fail the `committed-active-schedule` validator rule and produce an active schedule the ephemeral import would try to fire. Cadence lives at deployment time; the desired value is recorded in this design.
- **A dedicated dbt job per Airflow DAG (e.g. `pokemon_daily_dbt`).** Rejected — the playbook's "a domain never commits a second dbt job" holds; if a later orchestration is added it reuses the same `.DataBuildToolJob` with a different selector inside its own `dbt-content.json` copy, following whatever pattern that intent chooses. This design commits the first `.DataBuildToolJob` for the domain.
- **Cross-workload `TriggerDagRunOperator` chain.** Rejected — a single DAG with two tasks is sufficient and produces a single dag-run-id for correlation.

## Rerun behaviour

- **Idempotent within a day.** A second on-demand trigger with the same calendar day replaces `bronze.pokemon` (dlt `write_disposition="replace"`) and rebuilds `stg_pokemon` (view; free) and `pokemon_weight_bands` (table; full replace). No historical rows accumulate.
- **No cross-day dependency.** Every run is a full refresh from PokeAPI; a missed day loses no state.
- **No backfill.** `catchup=False` in the committed DAG. If a customer wants backfill they change it at deployment; the code does not care.
- **On failure.** A failed `dlt_load` sets `dbt_build` to `upstream_failed`, which sets `workload_succeeded` to `upstream_failed`, which makes the DAG run `failed` (validator's `recorder-only-leaf` rule ensures the leaf reflects reality). `record_run` still writes its correlation row with `status=failed` and `failed_tasks` populated. Recovery is a manual re-trigger; the same run replaces state, so nothing needs cleanup.
- **On transient PokeAPI 5xx.** No in-DAG retry today (the dlt resource itself has none). Recovery is a manual re-trigger. Recorded here so a later intent that wants retry logic knows the current baseline was deliberate.

## Consumers

- **Downstream data consumer:** the Fabric warehouse's `pokemon_analytics_mart.pokemon_weight_bands` (production `VD_DOMAIN_FABRIC_WAREHOUSE_*`; test/ephemeral `VD_EPHM_FABRIC_WAREHOUSE_*`). No named external consumer today; this intent's Requirement records "whoever queries `marts.pokemon_weight_bands`".
- **Customer CI/CD (deployment consumer):** the deployment automation that installs this Airflow Job. Contract given to it:
  - **DAG id / display name:** `pokemon_daily`.
  - **Cadence to apply:** `schedule="0 6 * * *"` in UTC on the deployed DAG, `catchup=False` (already committed), `max_active_runs=1` (already committed). Repository never activates this cron.
  - **Environment inputs to inject at apply time:** (a) an Airflow **Connection** named `fabric_default` bound to the target workspace with workspace-read + warehouse access; (b) an Airflow **Variable** `vd_fabric_workspace_id` holding the target workspace GUID; (c) the `.DataBuildToolJob` bound to the target warehouse via Fabric's connection resolution (`resolve-dbtjob-connection.py` on ephemeral; the customer's equivalent step in production).
  - **What must NOT be injected:** any GUID into a committed file; any credential into the DAG or into `airflowRequirements`.

## Supporting evidence

- Platform contract: `_shared/references/dialects/fabric/airflow-job.md` (`airflowRequirements` + restart rule, `committed-active-schedule`, `committed-guid`, `recorder-only-leaf`, `worker-side-dbt`, `worker-side-workload`, `dropped-dag-module`).
- Playbook: `_shared/playbooks/kinds/orchestration.md` — Airflow-target step tables and gates.
- ADR-0026 — decision record for the Airflow target and its rejected Cosmos alternative.
- Project memory (`.openhands/memory/MEMORY.md`) — VD-5578 live-verified Airflow endpoint discovery, variable injection, restart behaviour, and sandbox IDs. Not restated; cited so the runtime notes survive with the record.
- Existing product code: `ingestion/pokemon_pipeline.py` and `transformation/` (models `stg_pokemon.sql`, `pokemon_weight_bands.sql` + schemas).

## Gotchas

- **A change to `airflowRequirements` (any pin move, any package add or remove) requires the Apache Airflow Job to be **restarted**** — writing the definition installs nothing and the DAG then fails `ModuleNotFoundError` on a package the item plainly declares. Only the Fabric provider is committed here (`apache-airflow-providers-microsoft-fabric==0.1.1`) to make this rare; a domain that adds anything else pays a full environment rebuild the first time, and must not read the resulting `ModuleNotFoundError` as a wrong pin.
- **Adding a new file to a RUNNING environment does not reach the workers.** DAG or `orchestration_support.py` edits reach workers within about two minutes without restart, but the **first** upload of either file needs a restart. This bites first-apply only.
- **A `.py` at the top level of `dags/` that defines no DAG is silently dropped** — `orchestration_support.py` therefore lives in `plugins/`. The validator's `dropped-dag-module` rule catches a regression.
- **Deferrable operator without triggerers = task never resumes.** `apacheairflowjob-content.json` sets `enableTriggerers: true`; the validator rejects an item that does not.
- **`token` Airflow connections trigger Fabric jobs but cannot write correlation.** A sandbox run built from a broker-minted Fabric token runs green end-to-end, and `record_run` reports success because it does not gate, but its OneLake write returns `401`. That is expected for the sandbox; production uses an `spn` connection provisioned by the deployment operator.
- **Airflow's default scheduleless DAG is a daily interval, not `None`.** The template sets `schedule=None` explicitly; never delete that line as "unnecessary".
- **Starter pools auto-pause when idle.** A DAG triggered into a paused pool queues silently rather than failing. During sandbox iteration, an always-on pool is worth the cost; production choice is deferred to the customer.

## History

- **2026-09-15** — `new-intent-3142244a`: initial record. Establishes the Airflow target for the Pokemon Analytics domain, one DAG (`pokemon_daily`) triggering the reused dlt runner and the first domain `.DataBuildToolJob`, `schedule=None` committed with the cadence contract for customer CI/CD applied to the deployment, and the ephemeral on-demand run as the proof gate.
