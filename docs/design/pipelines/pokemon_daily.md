---
artifacts: [pokemon_daily]
platform: fabric_warehouse
target: airflow
---

# pokemon_daily

Fabric Apache Airflow Job orchestrating the daily pokemon analytics pipeline. Runs ingestion from PokeAPI via dlt, then transformation via dbt, on a fixed daily schedule at 6:00 UTC.

## Purpose

Automate the pokemon analytics pipeline to ensure fresh data every morning. Combines bronze ingestion (pokemon_pipeline.py, dlt) and silver/gold transformation (pokemon_analytics dbt project) in a single schedulable unit.

## Schedule

- **Cadence**: Daily
- **Time**: 06:00 UTC
- **Timezone**: UTC
- **Retry policy**: No automatic retries; single attempt per scheduled run
- **Execution timeout**: 1 hour maximum

## Workloads in dependency order

1. **dlt ingestion**: `ingestion/pokemon_pipeline.py` 
   - Loads pokemon data from PokeAPI into bronze layer
   - Runner notebook: `orchestration/pokemon_bronze_dlt_runner.Notebook/` (materialized on first generate)
   
2. **dbt transformation**: `transformation/pokemon_analytics` with selector `build` (all models)
   - Transforms bronze pokemon data to silver staging and gold marts
   - Artifact: `orchestration/pokemon_daily.DataBuildToolJob/`

Dependency: dbt runs only after dlt completes successfully.

## Platform requirements

- **Fabric environment type**: Apache Airflow Job on Fabric Warehouse
- **Triggers**: Manual on-demand run; automatic run at scheduled cadence once deployed
- **Execution identity**: Fabric workspace default execution context
- **Workspace-specific**: Ephemeral import carries no schedule; production deployment via Fabric Git integration activates the schedule

## Design decisions

- **Airflow target over Data Pipeline**: User stated domain standard is Airflow (supplied decision, Requirement R-03)
- **Single pipeline**: One Airflow job covering both ingestion and transformation, since they share one interval, owner (analytics team), failure boundary, and recovery contract
- **All dbt models**: No dbt selector filtering applied; full `dbt build` executes (supplied decision, Requirement R-02)
- **No conditional logic**: Pipeline is deterministic; no partial runs, branch logic, or gating
- **Timeout conservative**: 1 hour is sufficient for PokeAPI's 50-pokemon sample and staging/mart models, with headroom for network variance

## Rejected alternatives

- **Separate Airflow jobs for dlt and dbt**: Would complicate deployment and dependency management without isolation benefit; single job is simpler and has no failure isolation need since both use the same connection/identity
- **Partial dbt selection or specific model list**: User approved all models; no business case for filtering; selective runs remain possible via manual DAG trigger override if needed

## Rerun behaviour

- **Idempotent**: dlt pokemon resource has `write_disposition="replace"`, so reruns fully reload bronze without duplicates; dbt is configured to handle reruns cleanly with view/table materialization
- **No time-dependent state**: Pokemon data source is static (no deletion, only new/updated entries rare); reruns produce identical output for the same source state
- **Safe backfill**: A past day can be rerun by triggering the DAG manually with execution date set to that day

## Consumers

- **Data consumers**: Internal analytics team querying silver staging views and gold marts in pokemon_analytics project
- **Downstream models**: Any future models added to pokemon_analytics depend on bronze and staging layers staying consistent
- **SLAs**: None explicitly defined yet; 6am UTC daily is operational commitment

## Supporting evidence

- **Requirement artifact**: `docs/requirement/2026-09-14-pokemon-daily-orchestration-4fb12fe8.md` (R-01@1, R-02@1, R-03@1 approved)
- **Source inventory**: pokemon_pipeline.py confirmed present at `ingestion/pokemon_pipeline.py`; pokemon_analytics dbt project confirmed present at `transformation/` with dbt_project.yml

## Gotchas

- **Airflow restart requirement**: Changes to `airflowRequirements` in the job definition require an Airflow environment restart before taking effect; DAG changes do not (ref: `airflow-job.md`)
- **Triggerers must be enabled**: MSFabricRunJobOperator uses deferrable tasks by default, so triggerers must be enabled in the Airflow environment for tasks to resume
- **Schedule is ephemeral-agnostic**: Ephemeral imports carry no schedule and cannot be tested for schedule correctness; only production deployment activates the cadence

## History

- 2026-09-14: Initial design for pokemon daily Airflow orchestration; Requirement R-01@1, R-02@1, R-03@1
