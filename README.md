# Pokemon Analytics

VD-5578 live test domain (Microsoft Fabric, Warehouse).

Ingestion and transformation are already committed. `orchestration/` is empty on
purpose — authoring it is what the test exercises.

| Path | What it is |
| --- | --- |
| `ingestion/pokemon_pipeline.py` | dlt pipeline loading PokeAPI into `bronze.pokemon`. Public API, no credential. |
| `transformation/` | dbt project over that bronze table: one staging model, one mart. |
| `orchestration/` | empty |

`ingestion/.dlt/config.toml` IS committed, naming the Domain's workspace and
warehouse. An Intent run retargets it to the ephemeral resources in the uploaded
copy only. It was omitted at first on the assumption Studio seeds it into an
existing repo — it does not, and the dlt runner then fails inside Spark with only
`System cancelled the Spark session due to statement execution failures` to show
for it (VD-5578).

`transformation/profiles.yml` is not committed: the dbt job item carries its own
profile.
