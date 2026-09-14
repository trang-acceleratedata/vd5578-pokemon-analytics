# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   }
# META }

# MARKDOWN ********************

# # VibeData dlt runner
#
# Runs one committed dlt pipeline in whichever workspace the notebook itself lives in,
# on either Fabric platform. The committed `.dlt/config.toml` decides which: a
# `[destination.filesystem]` table is a Lakehouse pipeline, a `[destination.fabric]`
# table is a Warehouse pipeline. Nothing else selects the platform.
#
# **This notebook deliberately has no default Lakehouse attached.** Nothing here needs
# one: the destination comes from the committed config (Domain ids in Git;
# `upload-to-onelake.py` rewrites them to the ephemeral ids in the uploaded copy the
# sandbox run reads). Attaching one would add a second, contradictory binding — and one
# Fabric applies only on item CREATE, so it cannot be corrected by a Git sync.
# The Lakehouse *name* is the same in the Domain and ephemeral workspaces (Studio names
# the ephemeral Lakehouse after the Domain's, and a Warehouse's bridge Lakehouse after
# the Domain Warehouse), which is why a committed name resolves correctly in both and no
# id is ever passed in.
#
# Parameters (set by the invoking DataPipeline activity):
# - `DOMAIN_SLUG`: the Domain slug; names the OneLake code folder.
# - `PIPELINE`: the pipeline script inside `ingestion/`, e.g. `pokemon_pipeline.py`.
# - `SECRET_STORE_LOCATION`: the Domain's Azure Key Vault URL (a non-secret Domain coordinate).
# - `CODE_LAKEHOUSE_NAME`: the Lakehouse holding the ingestion code — used to LOCATE THE CODE
#   ONLY (on a Warehouse Domain it is the bridge Lakehouse, which carries the Domain
#   Warehouse's name). It never sets the destination; the pipeline's own config owns that.

# PARAMETERS CELL ********************

DOMAIN_SLUG = ""
PIPELINE = ""
SECRET_STORE_LOCATION = ""
CODE_LAKEHOUSE_NAME = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

import json
import os
import runpy
import subprocess
import sys
import tempfile
import tomllib
import urllib.request
from pathlib import Path

"""Finite ingestion wire contract, inlined into the materialized Fabric notebook.

Only stdlib imports: the notebook kernel has no plugin filesystem or Azure SDK.
"""
import hashlib
import json
import re
from concurrent.futures import CancelledError

POINTER_LIMIT = 4096
MANIFEST_LIMIT = 2 * 1024 * 1024
FILE_LIMIT = 32 * 1024 * 1024
TOTAL_LIMIT = 256 * 1024 * 1024
FILE_COUNT_LIMIT = 10000
ENTRY_LIMIT = 20000
_HEX256 = re.compile(r"[0-9a-f]{64}\Z")
_COMPONENT = re.compile(r"[A-Za-z0-9_.-]{1,255}\Z")


def canonical_json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def safe_path(path):
    if not isinstance(path, str) or len(path) > 1024:
        raise ValueError("invalid ingestion path")
    parts = path.split("/")
    if len(parts) > 32 or any(p in (".", "..") or not _COMPONENT.fullmatch(p) for p in parts):
        raise ValueError("unsafe ingestion path")
    return path


def safe_component(value):
    safe_path(value)
    if "/" in value:
        raise ValueError("expected one path component")
    return value


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _json(data, limit):
    if not data or len(data) > limit:
        raise ValueError("ingestion metadata size exceeds bounds")
    obj = json.loads(data, object_pairs_hook=_unique_object)
    if not isinstance(obj, dict) or type(obj.get("schemaVersion")) is not int or obj["schemaVersion"] != 1:
        raise ValueError("unsupported ingestion schemaVersion")
    return obj


def parse_pointer(data):
    obj = _json(data, POINTER_LIMIT)
    if set(obj) not in ({"schemaVersion", "revision"}, {"schemaVersion", "revision", "sourceCommit"}):
        raise ValueError("invalid ingestion pointer fields")
    if not isinstance(obj["revision"], str) or not _HEX256.fullmatch(obj["revision"]):
        raise ValueError("invalid ingestion revision")
    if "sourceCommit" in obj and (
        not isinstance(obj["sourceCommit"], str)
        or not re.fullmatch(r"(?:[0-9a-f]{40}|[0-9a-f]{64})", obj["sourceCommit"])
    ):
        raise ValueError("invalid ingestion sourceCommit")
    return obj


def parse_manifest(data, revision):
    if sha256(data) != revision:
        raise ValueError("ingestion manifest digest mismatch")
    obj = _json(data, MANIFEST_LIMIT)
    if set(obj) != {"schemaVersion", "files"} or not isinstance(obj["files"], list):
        raise ValueError("invalid ingestion manifest fields")
    if not 1 <= len(obj["files"]) <= FILE_COUNT_LIMIT:
        raise ValueError("ingestion file count exceeds bounds")
    files, total = {}, 0
    for item in obj["files"]:
        if not isinstance(item, dict) or set(item) != {"path", "size", "sha256"}:
            raise ValueError("invalid ingestion inventory entry")
        path = safe_path(item["path"])
        if not path.startswith("ingestion/") or path in files:
            raise ValueError("invalid or duplicate ingestion inventory path")
        size = item["size"]
        if type(size) is not int or not 0 <= size <= FILE_LIMIT:
            raise ValueError("ingestion file size exceeds bounds")
        if not isinstance(item["sha256"], str) or not _HEX256.fullmatch(item["sha256"]):
            raise ValueError("invalid ingestion file digest")
        total += size
        if total > TOTAL_LIMIT:
            raise ValueError("ingestion total bytes exceeds bounds")
        files[path] = item
    if list(files) != sorted(files) or canonical_json(obj) != data:
        raise ValueError("ingestion manifest is not canonical")
    # A file cannot also be another file's ancestor.
    for path in files:
        parts = path.split("/")
        if any("/".join(parts[:i]) in files for i in range(1, len(parts))):
            raise ValueError("ingestion path collides with a directory")
    return files


def confirmed_absence(exc):
    # Never infer absence from a message, exists(False), or an authorization failure.
    return isinstance(exc, FileNotFoundError) or (
        getattr(exc, "status_code", None) == 404
        and getattr(exc, "error_code", None) in ("BlobNotFound", "PathNotFound", "ResourceNotFound")
    )


def provider_call(operation, *args, **kwargs):
    """Authenticated provider bodies are not safe notebook terminal output."""
    try:
        return operation(*args, **kwargs)
    except CancelledError:
        raise
    except FileExistsError:
        raise FileExistsError("conditional ingestion write conflict") from None
    except Exception as exc:
        if confirmed_absence(exc):
            raise FileNotFoundError("ingestion provider path not found") from None
        status = getattr(exc, "status_code", None)
        status = status if type(status) is int else "unknown"
        raise RuntimeError(
            f"ingestion provider operation failed ({type(exc).__name__}, HTTP {status})"
        ) from None


for name, value in (
    ("DOMAIN_SLUG", DOMAIN_SLUG),
    ("PIPELINE", PIPELINE),
    ("SECRET_STORE_LOCATION", SECRET_STORE_LOCATION),
    ("CODE_LAKEHOUSE_NAME", CODE_LAKEHOUSE_NAME),
):
    if not value:
        raise SystemExit(f"Notebook parameter {name} is empty; the invoking pipeline activity must set it.")

safe_component(DOMAIN_SLUG)
safe_path(PIPELINE)

import notebookutils  # noqa: E402  (provided by the Fabric kernel; not a pip package)

# `currentWorkspaceId` is populated in every context and needs no Lakehouse
# attachment; `defaultLakehouse*` would be the ones that require one, and this
# runner never reads them.
workspace_id = notebookutils.runtime.context["currentWorkspaceId"]

# Resolve by NAME, in this notebook's own workspace. The same committed name is
# correct in the Domain workspace and in an ephemeral one, so nothing is rebound
# per workspace. This id locates the CODE; it is never fed to the destination.
try:
    lakehouse = provider_call(notebookutils.lakehouse.get, CODE_LAKEHOUSE_NAME, workspace_id)
except CancelledError:
    raise
except Exception as exc:
    raise SystemExit(
        f"No Lakehouse named '{CODE_LAKEHOUSE_NAME}' in workspace {workspace_id} ({exc}). "
        "The runner reads its ingestion code from that Lakehouse's Files area. Check the "
        "CODE_LAKEHOUSE_NAME the pipeline activity passes — it must be the DOMAIN Lakehouse "
        "name (or, on a Warehouse Domain, the Domain Warehouse name its bridge Lakehouse "
        "carries), which Studio also gives the ephemeral one. Do not attach a default "
        "Lakehouse to this notebook and do not pass a Lakehouse id."
    ) from exc

lakehouse_id = lakehouse["id"] if isinstance(lakehouse, dict) else lakehouse.id
safe_component(workspace_id)
safe_component(lakehouse_id)

# Absolute abfss, not a relative path: in a Python notebook a relative path resolves
# against the local working directory, not against any Lakehouse.
files_root = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Files"


def read_metadata(path, limit, allow_absent=False):
    try:
        entries = provider_call(notebookutils.fs.ls, path)
    except FileNotFoundError:
        if allow_absent:
            return None
        raise
    if not isinstance(entries, list):
        raise ValueError("ingestion metadata listing is not supported")
    if allow_absent and not entries:
        # Native ls returns [] for both missing paths and empty directories.
        # Only a typed property-lookup absence permits the migration path.
        try:
            provider_call(notebookutils.fs.getProperties, path)
        except FileNotFoundError:
            return None
        raise ValueError("ingestion pointer absence could not be confirmed")
    if len(entries) != 1 or not entries[0].isFile or entries[0].isDir:
        raise ValueError("ingestion metadata must be one regular file")
    size = entries[0].size
    if type(size) is not int or not 0 < size <= limit:
        raise ValueError("ingestion metadata size exceeds bounds")
    data = provider_call(notebookutils.fs.head, path, limit + 1).encode("utf-8")
    if len(data) != size or len(data) > limit:
        raise ValueError("ingestion metadata was truncated or changed during read")
    return data


def remote_inventory(root, expected):
    directories = {"/".join(p.split("/")[:i]) for p in expected
                   for i in range(1, len(p.split("/")))}
    pending, seen, found, count = [""], set(), {}, 0
    while pending:
        rel = pending.pop()
        for entry in provider_call(notebookutils.fs.ls, f"{root}/{rel}".rstrip("/")):
            count += 1
            if count > ENTRY_LIMIT:
                raise ValueError("ingestion remote inventory exceeds bounds")
            name = entry.name.rstrip("/") if entry.isDir else entry.name
            safe_component(name)
            child = f"{rel}/{name}" if rel else name
            safe_path(child)
            if child in seen or entry.path.rstrip("/") != f"{root}/{child}":
                raise ValueError("invalid ingestion remote inventory path")
            seen.add(child)
            if getattr(entry, "isSymlink", False):
                raise ValueError("ingestion symlink rejected")
            if entry.isDir and not entry.isFile:
                if child not in directories:
                    raise ValueError("extra ingestion directory")
                pending.append(child)
            elif entry.isFile and not entry.isDir:
                if child not in expected or entry.size != expected[child]:
                    raise ValueError("extra or changed ingestion file")
                found[child] = entry.size
            else:
                raise ValueError("ingestion entry must be a regular file or directory")
    if found != expected:
        raise ValueError("missing ingestion file")


release_root = f"{files_root}/{DOMAIN_SLUG}/.vibedata"
pointer_path = f"{release_root}/ingestion-current.json"
pointer_bytes = read_metadata(pointer_path, POINTER_LIMIT, allow_absent=True)

# A run owns its directory: concurrent notebooks never delete each other's source.
dest = Path(tempfile.mkdtemp(prefix="vd-dlt-runner-"))
work = dest / "ingestion"
if pointer_bytes is None:
    # Migration only on confirmed absence; an invalid present pointer never gets here.
    code_root = f"{files_root}/{DOMAIN_SLUG}/ingestion"
    if not provider_call(notebookutils.fs.cp, code_root, f"file:{dest}", recurse=True):
        raise ValueError("legacy ingestion copy failed; upload code to this Lakehouse")
else:
    pointer = parse_pointer(pointer_bytes)
    code_root = f"{release_root}/ingestion-releases/{pointer['revision']}"
    manifest_bytes = read_metadata(f"{code_root}/manifest.json", MANIFEST_LIMIT)
    inventory = parse_manifest(manifest_bytes, pointer["revision"])
    expected = {p: item["size"] for p, item in inventory.items()}
    expected["manifest.json"] = len(manifest_bytes)
    remote_inventory(code_root, expected)
    for path, item in inventory.items():
        target = dest / path
        target.parent.mkdir(parents=True, exist_ok=True)
        if not provider_call(notebookutils.fs.cp, f"{code_root}/{path}", f"file:{target}", recurse=False):
            raise ValueError("immutable ingestion copy failed")
        if target.is_symlink() or not target.is_file() or target.stat().st_size != item["size"]:
            raise ValueError("invalid copied ingestion file")
        with target.open("rb") as stream:
            data = stream.read(item["size"] + 1)
        if len(data) != item["size"] or sha256(data) != item["sha256"]:
            raise ValueError("ingestion file digest mismatch")
    found = set()
    for directory, dirs, filenames in os.walk(work, followlinks=False):
        for name in dirs + filenames:
            if (Path(directory) / name).is_symlink():
                raise ValueError("copied ingestion symlink rejected")
        for name in filenames:
            found.add((Path(directory) / name).relative_to(dest).as_posix())
    if found != set(inventory):
        raise ValueError("copied ingestion inventory mismatch")
    remote_inventory(code_root, expected)

if not (work / PIPELINE).is_file():
    raise SystemExit(
        f"{PIPELINE} is not in {code_root}; the PIPELINE parameter must name a committed "
        "ingestion/<name>_pipeline.py. Do not embed the pipeline code in the notebook."
    )

# The committed config decides the platform. Its ids are what the pipeline was reviewed
# with (retargeted in the uploaded copy by upload-to-onelake.py), so nothing here
# overrides them.
config_path = work / ".dlt" / "config.toml"
destination = tomllib.loads(config_path.read_text()).get("destination", {}) if config_path.is_file() else {}
if "fabric" in destination:
    platform = "fabric_warehouse"
elif "filesystem" in destination:
    platform = "fabric_lakehouse"
else:
    raise SystemExit(
        f"{config_path} names no destination: expected [destination.filesystem] (Lakehouse) or "
        "[destination.fabric] (Warehouse). The runner takes the platform from the committed "
        "config and nothing else."
    )

# Only the Domain contract. The destination is NOT set here: the runtime unit reads the
# committed destination table natively. Overriding its ids would silently retarget the
# load away from the config the pipeline was reviewed with.
os.environ["VD_DOMAIN_SECRET_STORE_KIND"] = "azure_key_vault"
os.environ["VD_DOMAIN_SECRET_STORE_LOCATION"] = SECRET_STORE_LOCATION
os.environ["VD_DOMAIN_DATA_PLATFORM"] = platform
os.environ["VD_DOMAIN_SLUG"] = DOMAIN_SLUG

# Validate every byte before either installing packages or executing user code.
subprocess.run([sys.executable, "-m", "pip", "install", "-q",
                "vibedata-dlt-fabric-notebook>=0.8.0", "dlt>=1.29"], check=True)

# The project's own requirements, plus each vendored connector's (`sources/<connector>/
# requirements.txt`, the dlt verified-source convention — e.g. the Salesforce connector's
# simple-salesforce). The agent image carries these; a Fabric kernel does not, and a
# pipeline whose connector cannot import fails before its first request.
for requirements in sorted([work / "requirements.txt", *work.glob("sources/*/requirements.txt")]):
    if requirements.is_file():
        print(f"vd-dlt-runner: installing {requirements.relative_to(work)}")
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", "-r", str(requirements)], check=True)

os.chdir(work)
sys.path.insert(0, str(work))
print(f"vd-dlt-runner: running {PIPELINE} from {code_root} ({platform})")

if platform == "fabric_lakehouse":
    # In-process, not a subprocess: the Lakehouse notebook unit imports `notebookutils`,
    # which exists only inside the kernel session.
    runpy.run_path(str(work / PIPELINE), run_name="__main__")
    print("vd-dlt-runner: completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# ---- Warehouse: the Studio Warehouse unit, driven by notebook tokens -------------------
#
# No `-notebook` distribution of the Warehouse unit exists; the Studio unit already has the
# seams a notebook needs (a per-connection token provider, `[destination.fabric]` read
# natively), so this cell supplies what the credential broker would: tokens minted by
# `notebookutils.credentials.getToken` for the SQL, OneLake, Fabric REST and Key Vault
# audiences, handed to a child interpreter through its environment only.
#
# A child interpreter, not in-process, because the kernel imports adlfs 2023.x for
# `notebookutils` before any cell runs and a `%pip install` does not replace what is
# already imported; that adlfs derives `account_name` from the OneLake URL and collides
# with dlt's own kwargs. Only the Lakehouse unit needs the
# kernel's `notebookutils`; the Warehouse load needs nothing from it once the tokens exist.

WAREHOUSE_BOOTSTRAP = r'''
"""Child-side bootstrap: tokens → the Warehouse unit's seams, then the committed pipeline."""
import json
import logging
import os
import runpy
import sys
import time
import urllib.error
import urllib.request

from azure.core.credentials import AccessToken
from azure.core.exceptions import ClientAuthenticationError

logger = logging.getLogger("vd-dlt-runner")

SQL_SCOPE = "https://database.windows.net/.default"
STORAGE_SCOPE = "https://storage.azure.com/.default"
FABRIC_API_SCOPE = "https://api.fabric.microsoft.com/.default"
# Popped, not read: the pipeline code and its connectors never see a token in the environment.
TOKENS = {
    SQL_SCOPE: os.environ.pop("VD_RUNNER_SQL_TOKEN"),
    STORAGE_SCOPE: os.environ.pop("VD_RUNNER_STORAGE_TOKEN"),
    FABRIC_API_SCOPE: os.environ.pop("VD_RUNNER_FABRIC_API_TOKEN"),
}
KEY_VAULT_TOKEN = os.environ.pop("VD_RUNNER_KEY_VAULT_TOKEN")
VAULT_URL = os.environ["VD_DOMAIN_SECRET_STORE_LOCATION"].rstrip("/")


class StorageTokenCredential:
    """azure-core TokenCredential over the pre-minted OneLake token, for adlfs."""

    def get_token(self, *scopes, **kwargs):
        return AccessToken(TOKENS[STORAGE_SCOPE], int(time.time()) + 3000)


def install_token_provider():
    from vibedata.dlt.fabric_warehouse import set_token_provider

    set_token_provider(lambda scope: TOKENS.get(scope) or TOKENS[SQL_SCOPE])


def install_staging_credential():
    """`onelake_staging()` builds a broker credential unconditionally; swap in the token one.

    Both import paths are patched because the pipeline imports the name from the package
    (`from vibedata.dlt.fabric_warehouse import onelake_staging`).
    """
    import vibedata.dlt.fabric_warehouse as unit
    from vibedata.dlt.fabric_warehouse import staging
    from dlt.common.configuration.specs import AzureCredentials
    from dlt.destinations import filesystem

    def onelake_staging(**kwargs):
        credentials = AzureCredentials.from_credential(StorageTokenCredential())
        credentials.azure_storage_account_name = staging.ONELAKE_ACCOUNT
        credentials.azure_account_host = staging.ONELAKE_BLOB_HOST
        return filesystem(bucket_url=staging.staging_bucket_url(), credentials=credentials, **kwargs)

    staging.onelake_staging = onelake_staging
    unit.onelake_staging = onelake_staging


def guard_adlfs_directory_probes():
    """OneLake answers a blob HEAD on a trailing-slash path with 403 AuthenticationFailed
    when the bearer is a notebook token (the same token gets 200 on every other request,
    and an `az` user token gets 200 on that one). dlt's staging client asks adlfs
    `isdir(<dataset>/)` before truncating, adlfs HEADs that path verbatim, and the 403
    would end the load. Treat that one answer as "not a blob" and let adlfs fall through
    to its listing-based directory check."""
    from adlfs import AzureBlobFileSystem

    original_exists, original_isfile = AzureBlobFileSystem._exists, AzureBlobFileSystem._isfile

    async def _exists(self, path, **kwargs):
        try:
            return await original_exists(self, path, **kwargs)
        except ClientAuthenticationError:
            if not str(path).endswith("/"):
                raise
            container, blob_path, _ = self.split_path(path)
            return await self._dir_exists(container, blob_path)

    async def _isfile(self, path, **kwargs):
        try:
            return await original_isfile(self, path, **kwargs)
        except ClientAuthenticationError:
            if not str(path).endswith("/"):
                raise
            return False

    AzureBlobFileSystem._exists, AzureBlobFileSystem._isfile = _exists, _isfile


def register_key_vault_provider():
    """The Domain's Key Vault as dlt's secret provider, read with the notebook's KV token.

    The Warehouse Studio unit registers no secret provider of its own (the broker path
    reads secrets on the agent side), so the notebook run supplies the same provider the
    Lakehouse notebook unit has, over REST instead of `notebookutils.credentials.getSecret`
    (which exists only in the kernel).
    """
    import dlt
    from vibedata.dlt.fabric_core.providers.akv_base import AzureKeyVaultProviderBase

    class NotebookTokenKeyVaultProvider(AzureKeyVaultProviderBase):
        def _get_secret(self, name):
            request = urllib.request.Request(
                f"{self._vault_url}/secrets/{name}?api-version=7.4",
                headers={"Authorization": f"Bearer {KEY_VAULT_TOKEN}"},
            )
            try:
                with urllib.request.urlopen(request, timeout=30) as response:
                    return json.loads(response.read().decode()).get("value")
            except urllib.error.HTTPError as exc:
                if exc.code == 404:
                    return None
                logger.warning("Key Vault read of %s failed: HTTP %s", name, exc.code)
                return None

    dlt.config.register_provider(NotebookTokenKeyVaultProvider(vault_url=VAULT_URL))


def main(pipeline_file):
    install_token_provider()
    install_staging_credential()
    guard_adlfs_directory_probes()
    register_key_vault_provider()
    sys.path.insert(0, os.getcwd())
    runpy.run_path(pipeline_file, run_name="__main__")


if __name__ == "__main__":
    main(sys.argv[1])
'''

if platform == "fabric_warehouse":
    subprocess.run(
        [sys.executable, "-m", "pip", "install", "-q",
         "vibedata-dlt-fabric-warehouse-studio>=0.1.1", "dlt[fabric,filesystem]>=1.30",
         "adlfs>=2026.8.0", "fsspec>=2026.7.0"],
        check=True,
    )

    fabric = tomllib.loads(config_path.read_text())["destination"]["fabric"]
    if str(fabric.get("workspace_id", "")).lower() != str(workspace_id).lower():
        raise SystemExit(
            f"{config_path} names workspace {fabric.get('workspace_id')} but this runner runs in "
            f"{workspace_id}: the uploaded copy was not retargeted. Re-upload ingestion/ with "
            "upload-to-onelake.py --workspace-id <this workspace> --warehouse-id <its Warehouse> "
            "--lakehouse-id <its bridge Lakehouse>; never edit the repo config."
        )
    warehouse_id = str(fabric.get("warehouse_id", ""))

    # Runtime coordinates the committed config does not carry (Studio supplies the same
    # three through the environment on the agent side): the Warehouse's TDS endpoint and
    # name, resolved from the committed id, and the bridge Lakehouse — which is the
    # Lakehouse the code came from, so no second name or id is needed.
    request = urllib.request.Request(
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/warehouses/{warehouse_id}",
        headers={"Authorization": f"Bearer {notebookutils.credentials.getToken('pbi')}"},
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            warehouse = json.loads(response.read().decode())
    except Exception as exc:  # noqa: BLE001 — the message is the diagnosis
        raise SystemExit(
            f"Warehouse {warehouse_id} could not be read in workspace {workspace_id} ({exc}). The "
            "committed [destination.fabric] warehouse_id must name a Warehouse in this workspace."
        ) from exc
    sql_endpoint = (warehouse.get("properties") or {}).get("connectionString")
    if not sql_endpoint:
        raise SystemExit(f"Warehouse {warehouse_id} reports no SQL endpoint yet; retry once it is provisioned.")

    child_env = {
        **os.environ,
        "DESTINATION__FABRIC__CREDENTIALS__HOST": sql_endpoint,
        "DESTINATION__FABRIC__CREDENTIALS__DATABASE": warehouse["displayName"],
        "DESTINATION__FABRIC__BRIDGE_LAKEHOUSE_ID": lakehouse_id,
        "VD_RUNNER_SQL_TOKEN": notebookutils.credentials.getToken("https://database.windows.net/"),
        "VD_RUNNER_STORAGE_TOKEN": notebookutils.credentials.getToken("storage"),
        "VD_RUNNER_FABRIC_API_TOKEN": notebookutils.credentials.getToken("pbi"),
        "VD_RUNNER_KEY_VAULT_TOKEN": notebookutils.credentials.getToken("keyvault"),
    }
    bootstrap = work / "_vd_dlt_runner_bootstrap.py"
    bootstrap.write_text(WAREHOUSE_BOOTSTRAP)
    try:
        # Streams the pipeline's own output into the notebook log; a non-zero exit fails the
        # activity with the child's traceback already printed above it.
        subprocess.run([sys.executable, str(bootstrap), PIPELINE], cwd=work, env=child_env, check=True)
    finally:
        bootstrap.unlink(missing_ok=True)
    print("vd-dlt-runner: completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
