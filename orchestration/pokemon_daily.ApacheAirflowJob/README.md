# Pokemon Daily Airflow Job

## Overview
This Airflow DAG orchestrates the daily Pokemon analytics workflow:
1. **Load Phase**: Executes the dlt pipeline to fetch and load Pokemon data from PokeAPI
2. **Transform Phase**: Runs dbt build to transform and materialize analytics models

## Schedule
- **Frequency**: Daily
- **Time**: 6:00 AM UTC
- **Catchup**: Disabled (only runs at scheduled times)

## Tasks
1. `load_pokemon_data`: Runs the dlt Pokemon pipeline
2. `transform_pokemon_data`: Runs dbt build to transform data

## Dependencies
- dlt (data loading framework)
- dbt (data transformation framework)
- Airflow 2.x+

## Notes
- The pipeline uses a `replace` write disposition, so it fully refreshes Pokemon data daily
- dbt build runs all models and tests
- Retries are set to 1 with a 5-minute delay on failure
