# Pokemon Analytics

VD-5578 live test domain (Microsoft Fabric, Warehouse).

Ingestion and transformation are already committed. `orchestration/` is empty on
purpose — authoring it is what the test exercises.

| Path | What it is |
| --- | --- |
| `ingestion/pokemon_pipeline.py` | dlt pipeline loading PokeAPI into `bronze.pokemon`. Public API, no credential. |
| `transformation/` | dbt project over that bronze table: one staging model, one mart. |
| `orchestration/` | empty |

`ingestion/.dlt/config.toml` and `transformation/profiles.yml` are seeded by Vibe
Data Studio when the Domain is created; they are deliberately not committed here,
because they carry per-Domain coordinates.
