---
kinds: [orchestration]
---

# Intent: Pokemon Daily Orchestration

## Classification

| Kind | Action | Objective | Destination | Rationale |
| --- | --- | --- | --- | --- |
| orchestration | create | Schedule daily pokemon ingestion and analytics pipeline | Fabric Warehouse (Airflow) | Automate pokemon data load and transformation daily at 6am UTC |

## Goal

Enable automated daily execution of the pokemon analytics pipeline, combining ingestion from PokeAPI and dbt transformation into a single scheduled Airflow job running at 6am UTC.

## Source system

- **PokeAPI** (public, unauthenticated): Pokemon data source
- **Local dbt project** (pokemon_analytics): Transformation layer already exists

## Target

- **Platform**: Fabric Warehouse
- **Orchestration Engine**: Airflow
- **Execution schedule**: Daily at 6:00 UTC
- **Retry policy**: No retries (single attempt)
- **Timeout**: 1 hour maximum execution time

## Deliverables inventory

| # | Deliverable | Kind | Requirement refs | Notes |
| --- | --- | --- | --- | --- |
| 1 | Pokemon daily Airflow job | orchestration | R-01@1, R-02@1, R-03@1 | Fabric Apache Airflow Job artifact scheduled for 6am UTC |

## Requirements

New IDs are local to this intent; revisions preserve earlier wording, provenance and approval history.

| ID | Revision | Requirement | Acceptance criteria | Source | Resolution | Status |
| --- | --- | --- | --- | --- | --- | --- |
| R-01 | 1 | Schedule daily execution of pokemon ingestion pipeline (dlt) and dbt transformation | Airflow job runs daily at exactly 6:00 UTC; pokemon_pipeline.py executes successfully; dbt models build without failures | User request: "setup daily run on 6am UTC" | supplied decision | pending |
| R-02 | 1 | Execute all dbt models without filtering or selection | DBT job runs `dbt build` on all models in pokemon_analytics project | User answer: dbt_selector=all | supplied decision | pending |
| R-03 | 1 | Use Airflow as orchestration engine on Fabric Warehouse platform | Committed artifact is Fabric Apache Airflow Job (.ApacheAirflowJob directory) | User statement: "We use Airflow" + platform context (VD_DOMAIN_DATA_PLATFORM: fabric_warehouse) | supplied decision | pending |

## Out of scope

- Airflow DAG modifications or custom operators beyond standard dlt/dbt integration
- Modifications to existing pokemon_pipeline.py or dbt project configuration
- Scheduling of partial runs or conditional execution paths
- Airflow-specific monitoring or alerting (beyond platform defaults)

## Open questions

None — all material decisions have been provided by the user.

## Design pending

Technical decisions deferred to Design phase:
- Dependency graph ordering between dlt and dbt activities
- Exact retry/timeout configuration syntax for Fabric Airflow
- Whether to include data validation or quality gates in the pipeline

## Change history

Initial capture: 2026-09-14, requirements R-01, R-02, R-03 from user interaction.

## Change impact

No pending or approved changes. This is the initial Requirement for a new orchestration artifact.

## Approvals

**Requirement approval pending** — awaiting user confirmation that R-01@1, R-02@1, R-03@1 accurately capture the requested work before design phase proceeds.
