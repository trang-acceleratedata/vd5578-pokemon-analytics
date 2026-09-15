---
status: decided
date: 2026-09-15
---

# The Pokemon Analytics orchestration target is Apache Airflow, not the Fabric Data Pipeline

The Pokemon Analytics domain commits its native orchestration items as Fabric **Apache Airflow Jobs** (`orchestration/<Name>.ApacheAirflowJob/`) rather than the plugin's default **Data Pipeline** target (`orchestration/<Name>.DataPipeline/`). The choice is hard to reverse — the orchestration playbook holds "a domain has exactly one target" and forbids one intent from committing both, so a later swap costs a full re-author plus orphaning the deployed items — and it changes what every future orchestration intent in this domain looks like, so a new reader arriving at `orchestration/` needs to know why the non-default target was picked. A real trade-off exists: Data Pipeline is the playbook's recommendation, has faster verification loops (minutes rather than the tens of minutes an Airflow environment rebuild costs), and needs no separate secret-store precondition — those advantages were considered and traded for Airflow-standard ownership.

## Considered Options

- **Fabric Apache Airflow Job (chosen).** The user named Airflow as the domain's operations standard ("We use Airflow"). The precondition — an `azure_key_vault`-backed Domain — is already satisfied for Pokemon Analytics. The plugin ships the target end-to-end (materializer, DAG template, validator, sandbox runner) and the run model is verified live (VD-5578, this domain).
- **Fabric Data Pipeline (rejected).** The plugin's default and general recommendation. Not chosen because it would put the domain's orchestration on a platform its owner does not standardise on, which would push every future maintainer through a native item type the rest of their environment does not use. The playbook and the platform contract at `plugins/vibedata-data-engineering/_shared/references/dialects/fabric/airflow-job.md` explicitly frame Airflow as opt-in for exactly this case.
- **A customer-owned framework outside `orchestration/` (rejected).** The playbook allows omitting orchestration, but the user asked for it inside the repository. Rejected without further consideration.

## Consequences

- Every later orchestration intent in this domain commits `.ApacheAirflowJob` items (plus one shared `.DataBuildToolJob` and any dlt runner `.Notebook` items the DAG triggers). A `.DataPipeline` next to them is a defect; the plugin's validator rejects a repo carrying both.
- The domain's Secret Store must remain `azure_key_vault` for the dlt runner's credential resolution to work. A migration to another secret backend is a domain-wide change, not an intent-level one, and is out of scope for any orchestration intent.
- Verification loops in this domain pay the Airflow environment cost: a first apply, and any later `airflowRequirements` change, needs a full environment stop/start rebuild (measured 2+ minutes bare, 20+ minutes with a heavy dependency tree). Iteration on DAG or `plugins/orchestration_support.py` reaches workers in about two minutes without a restart. Intent plans budget accordingly.
- Cadence lives at deployment. The committed DAG always carries `schedule=None` (validator `committed-active-schedule` rule); the customer's CI/CD activates the production cron. The desired cadence is recorded in each pipeline's design record so there is one authoritative source.
- The Airflow connection GUID and the target workspace GUID are never committed. The GUID reaches the DAG through Airflow's `fabric_default` connection at apply time; the workspace GUID reaches the DAG through the Airflow Variable `vd_fabric_workspace_id` at apply time. This is enforced by the validator's `committed-guid` rule.
- Anyone proposing to switch to Data Pipeline in a later intent needs a new ADR superseding this one, plus the intent-level ceremony to remove the existing items — this is not a decision an implementer changes in passing.
