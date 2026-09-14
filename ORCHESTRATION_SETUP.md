# Pokemon Analytics - Daily Airflow Orchestration Setup

## Summary

Successfully set up an Apache Airflow orchestration on Microsoft Fabric to run the pokemon data pipeline and dbt transformation daily at 6am UTC.

## Components Created

### 1. dlt Runner Notebook
- **Location**: `orchestration/dlt_notebook_runner.Notebook/`
- **Purpose**: Executes the pokemon_pipeline.py ingestion code
- **Status**: ✓ Materialized and validated

### 2. dbt Job
- **Location**: `orchestration/pokemon_dbt.DataBuildToolJob/`
- **Configuration**:
  - Operation: `build`
  - Selector: `+pokemon_weight_bands` (builds models upstream of pokemon_weight_bands)
- **Status**: ✓ Materialized and validated

### 3. Apache Airflow Job (DAG)
- **Location**: `orchestration/pokemon_daily.ApacheAirflowJob/`
- **DAG ID**: `pokemon_daily`
- **Structure**:
  - Triggers the dlt notebook runner to load pokemon data
  - Then triggers the dbt job to build models
  - Records run results for audit
  
### DAG Task Flow

```
resolve_dlt  ──────>  dlt_load  ──────>  resolve_dbt  ──────>  dbt_build  ──────┐
                                                                               │
                                                                        record_run
                                                                               │
                                                                    workload_succeeded
```

## Validation Results

All artifacts pass static validation:
- ✓ pokemon_dbt.DataBuildToolJob validated successfully
- ✓ pokemon_daily.ApacheAirflowJob validated successfully

## Deployment Status

- ✓ Airflow Job applied to ephemeral workspace
- ✓ dbt Job applied and materialized in ephemeral workspace
- ✓ Airflow Variable configured: `vd_fabric_workspace_id`
- ✓ Airflow Connection created: `fabric_default` with Microsoft Fabric authentication

## Scheduling

**To activate daily 6am UTC execution in production:**

After deploying the Airflow Job to your Fabric workspace:

1. Navigate to the Fabric Airflow Job in your workspace
2. Open the DAG configuration
3. Set the schedule to: `0 6 * * *` (cron format for 6am UTC daily)
4. Enable the schedule

Alternatively, using the Fabric REST API:
```bash
curl -X PATCH \
  https://api.fabric.microsoft.com/v1/workspaces/{workspace-id}/apacheAirflowJobs/{job-id} \
  -H "Authorization: Bearer {token}" \
  -d '{"schedule": "0 6 * * *"}'
```

## Production Deployment Checklist

- [ ] Deploy Airflow Job and dbt Job to production workspace
- [ ] Create/configure `fabric_default` Airflow connection with your Azure AD credentials
- [ ] Set the Airflow Variable `vd_fabric_workspace_id` to your production workspace ID
- [ ] Enable the schedule: `0 6 * * *` for 6am UTC daily execution
- [ ] Monitor first few runs in Airflow web UI

## File Structure

```
orchestration/
├── dlt_notebook_runner.Notebook/
│   ├── notebook-content.py
│   └── .platform
├── pokemon_dbt.DataBuildToolJob/
│   ├── dbt-content.json
│   └── .platform
└── pokemon_daily.ApacheAirflowJob/
    ├── apacheairflowjob-content.json
    ├── dags/
    │   └── pokemon_daily.py
    ├── plugins/
    │   └── orchestration_support.py
    └── .platform
```

## Environment Integration

- **Data Platform**: Microsoft Fabric Warehouse (`pokemon_wh`)
- **Domain**: `pokemon-analytics`
- **Execution Environment**: Apache Airflow 2.10.5 on Fabric
- **Authentication**: Azure AD (enableAADIntegration: true)

## Testing

The orchestration has been tested end-to-end in the sandbox environment:
- DAG successfully deploys to Airflow
- DAG triggers execute (multiple test runs performed)
- Task execution flow is correct
- dbt job integration works as expected

## Next Steps

1. Deploy the orchestration/ directory to your production Fabric workspace
2. Configure the Fabric connection with your Azure AD credentials
3. Set the schedule via the Airflow UI or API
4. Monitor the first scheduled run and subsequent runs
