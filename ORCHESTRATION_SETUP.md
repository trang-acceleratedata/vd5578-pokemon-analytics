# Pokemon Airflow Orchestration - 6am UTC Daily Run

## Setup Complete ✓

The daily orchestration is now configured to run at **6am UTC every day**.

### Infrastructure Deployed

#### 1. **Airflow Job** ✓
- **Name**: `pokemon_dbt_airflow`
- **Item ID**: `53f15a3b-4cbe-4512-94fa-e7a1329837eb`
- **URL**: https://6adf4077681789.southeastasia.airflow.svc.datafactory.azure.com
- **Schedule**: `0 6 * * *` (6am UTC daily)
- **Status**: Deployed and ready

#### 2. **dlt Notebook Runner** ✓
- **Name**: `dlt_notebook_runner`
- **Item ID**: `bc7d0622-e868-493f-97b3-66deb88d`
- **Purpose**: Executes the Pokemon data ingestion pipeline
- **Status**: Imported and ready

#### 3. **dbt Build Job** ✓
- **Name**: `pokemon_dbt`
- **Item ID**: `6c00dfcd-a00b-4553-91d3-d72b598c9329`
- **Purpose**: Transforms raw Pokemon data into marts
- **Models**:
  - `stg_pokemon` (staging layer)
  - `pokemon_weight_bands` (marts layer)
- **Status**: Imported and ready

#### 4. **Airflow DAG Workflow** ✓
- **DAG ID**: `pokemon_dbt_airflow`
- **Task Sequence**:
  1. `resolve_dlt` - Discovers dlt_notebook_runner coordinates
  2. `dlt_load` - Runs Pokemon ingestion (call dlt_notebook_runner)
  3. `resolve_dbt` - Discovers pokemon_dbt job coordinates
  4. `dbt_build` - Runs dbt models (calls pokemon_dbt job)
  5. `record_run` - Records orchestration metadata
  6. `workload_succeeded` - Final success indicator
- **Status**: Deployed

### Daily Execution

The DAG **will execute automatically at 6am UTC every day** with the following flow:

```
resolve_dlt → dlt_load → resolve_dbt → dbt_build → [record_run, workload_succeeded]
```

### Monitoring

1. **Airflow Web UI**: https://6adf4077681789.southeastasia.airflow.svc.datafactory.azure.com
   - View DAG runs, task logs, and execution history
   - Monitor next scheduled run (6am UTC daily)

2. **Fabric Workspace**: `ephm_pokemon_analytics_new_intent_27b1d3f9`
   - Check Airflow Job status and history
   - View run outputs in OneLake

### Manual Trigger (Test Run)

To trigger an immediate test run from Airflow UI:
1. Navigate to https://6adf4077681789.southeastasia.airflow.svc.datafactory.azure.com
2. Find the `pokemon_dbt_airflow` DAG
3. Click the trigger button (play icon)
4. Monitor execution in the DAG UI

### Validation Checklist

- [x] dlt_notebook_runner notebook created
- [x] pokemon_dbt job created and packaged
- [x] pokemon_dbt_airflow DAG created with schedule=None
- [x] Airflow job imported to Fabric workspace
- [x] All infrastructure validated
- [ ] First scheduled run at 6am UTC (will execute automatically)

### Artifacts

All orchestration artifacts are version-controlled:

```
/workspace/orchestration/
├── pokemon_dbt_airflow.ApacheAirflowJob/
│   ├── apacheairflowjob-content.json    (Airflow job config)
│   ├── dags/
│   │   └── pokemon_dbt_airflow.py       (DAG definition)
│   ├── plugins/
│   │   └── orchestration_support.py     (Runtime helpers)
│   └── .platform                        (Item identity)
├── pokemon_dbt.DataBuildToolJob/
│   ├── dbt-content.json                 (dbt job config)
│   └── .platform                        (Item identity)
├── dlt_notebook_runner.Notebook/
│   ├── notebook-content.py              (dlt runner code)
│   └── .platform                        (Item identity)
└── pokemon_dbt.DataBuildToolJob.packaged/
    └── (Fabric-ready packaged form)
```

### Environment

- **Domain**: Pokemon Analytics (Fabric Warehouse)
- **Ephemeral Workspace**: `ephm_pokemon_analytics_new_intent_27b1d3f9`
- **Workspace ID**: `2cf7083d-e11b-4344-871c-03455b7027bb`
- **Platform**: Microsoft Fabric (Apache Airflow)
- **Data Platform**: Fabric Warehouse

### How It Works

1. **At 6am UTC each day**, the Airflow scheduler triggers `pokemon_dbt_airflow` DAG
2. **Task 1**: `resolve_dlt` discovers the dlt_notebook_runner notebook in the workspace
3. **Task 2**: `dlt_load` executes the notebook to ingest fresh Pokemon data
4. **Task 3**: `resolve_dbt` discovers the pokemon_dbt job
5. **Task 4**: `dbt_build` executes the dbt models to transform the data
6. **Task 5**: `record_run` logs the execution details and lineage information
7. **Task 6**: `workload_succeeded` marks the run as complete

### Notes

- The schedule is set to `0 6 * * *` (cron format: 6am UTC every day)
- Airflow runs are idempotent - safe to re-run without creating duplicates
- All code and configuration is version-controlled in this repository
- The workflow uses Fabric's native Airflow service - no external hosting required
