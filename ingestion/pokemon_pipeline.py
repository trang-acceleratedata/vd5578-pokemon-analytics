# ingestion/pokemon_pipeline.py — Fabric Warehouse domain (VD_DOMAIN_DATA_PLATFORM: fabric_warehouse)
#
# PokeAPI is public and unauthenticated, so this pipeline declares no connection
# section and reads no secret.
import dlt
import requests
from vibedata.dlt.fabric_warehouse import setup_environment, finalize, onelake_staging

PIPELINE_NAME = "pokemon_bronze"
BASE_URL = "https://pokeapi.co/api/v2"
POKEMON_COUNT = 50

setup_environment()


@dlt.resource(name="pokemon", write_disposition="replace", primary_key="id")
def pokemon():
    """One flat row per pokemon, so the load produces a single clean table."""
    listing = requests.get(f"{BASE_URL}/pokemon",
                           params={"limit": POKEMON_COUNT}, timeout=60)
    listing.raise_for_status()
    for entry in listing.json()["results"]:
        detail = requests.get(entry["url"], timeout=60)
        detail.raise_for_status()
        record = detail.json()
        yield {
            "id": record["id"],
            "name": record["name"],
            "height": record["height"],
            "weight": record["weight"],
            "base_experience": record.get("base_experience"),
        }


pipeline = dlt.pipeline(
    pipeline_name=PIPELINE_NAME,
    destination="fabric",
    staging=onelake_staging(),
    dataset_name="bronze",
)

try:
    load_info = pipeline.run(pokemon())
except Exception as exc:
    finalize(pipeline, error_message=str(exc))
    raise
print(load_info)
print("audit:", finalize(pipeline))
