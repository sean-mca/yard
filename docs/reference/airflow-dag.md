<!-- generated-by: gsd-doc-writer -->
# Airflow integration

yard generates Airflow DAGs from the same hierarchical YAML tree it uses for
job codegen. A directory containing a `dag.yaml` marker becomes one Airflow
DAG; the jobs under that directory become its tasks; the
`yard.yaml`/`account.yaml`/`region.yaml`/`dag.yaml` inheritance chain supplies
DAG-level config; and `yard plan` / `yard apply` / `yard show` render the DAG
to a Python file and (optionally) upload it to an MWAA-compatible S3 bucket.

This document is the authoritative reference for that integration. For the
top-level `airflow:` block field list that also appears in
[configuration](configuration.md), this doc gives the complete semantics,
operator mapping, and end-to-end examples.

- [Discovery and grouping](#discovery-and-grouping)
- [Config inheritance](#config-inheritance)
- [AirflowSection reference](#airflowsection-reference)
- [AirflowJobBlock reference](#airflowjobblock-reference)
- [Operator mapping](#operator-mapping)
- [Task and DAG identifiers](#task-and-dag-identifiers)
- [Cross-account connections](#cross-account-connections)
- [Airflow Datasets](#airflow-datasets)
- [Generation, deployment, destroy](#generation-deployment-destroy)
- [End-to-end example](#end-to-end-example)
- [Validation errors](#validation-errors)
- [Limitations and planned work](#limitations-and-planned-work)

---

## Discovery and grouping

### The `dag.yaml` marker

`dag.yaml` is a marker file — its **presence** in a directory declares "every
job at or below this directory is part of a single Airflow DAG." Discovery is
implemented in `yard-core/src/airflow_dag/collection.rs`:

1. `collect_dags` walks the project root with `walkdir` and collects every
   directory whose immediate contents include a file named `dag.yaml`.
2. Each discovered job (from `ProjectManifest.jobs`) is assigned to its
   **nearest ancestor** DAG directory. Jobs with no ancestor `dag.yaml` are
   ignored by Airflow codegen (but `yard apply` will still error if such a
   job has an `airflow:` block — see
   [Validation errors](#validation-errors)).
3. Nested `dag.yaml` files are a hard error — you cannot have a DAG dir
   inside another DAG dir.
4. An empty DAG directory (marker with no task files anywhere beneath it)
   is also a hard error.

DAG directories are processed in sorted path order so generation is
deterministic across runs.

### How jobs are grouped

```
aws/dev/us-east-2/
  dag.yaml            # <-- marks this directory as a DAG
  orders.yaml         # task in the DAG
  shipments.yaml      # task in the DAG
  notify.yaml         # task in the DAG
```

All three jobs become tasks of one DAG. Reorganising them into a subdirectory
(without adding a nested `dag.yaml`) does not change this — the DAG is defined
by the nearest ancestor marker, not by sibling-ness.

### `depends_on` semantics

Task dependencies are declared per-job under `airflow.depends_on`. Two name
forms are accepted, resolved in `yard-core/src/airflow_dag/resolve.rs`:

- **Full name** — the fully-qualified job key as it appears in
  `ProjectManifest.jobs` (e.g. `sales-orders`).
- **Short name** — the filename-derived `base_name` (e.g. `orders` for
  `orders.yaml`).

Resolution rules:

| Situation | Result |
|-----------|--------|
| `dep` exactly matches a task in this DAG | Resolves to that task. |
| `dep` matches the short name of exactly one task in this DAG | Resolves to that task. |
| `dep` matches multiple short names in this DAG | Error: `ambiguous — matches: ...` |
| `dep` matches a job in another DAG | Error: `cross-DAG depends_on ... — cross-DAG dependencies are not supported` |
| `dep` matches no job at all | Error: `depends_on '<dep>', which is not a task in this DAG` |
| `dep` equals the declaring task's own name (either form) | Error: `depends on itself` |

Cross-DAG dependencies are explicitly unsupported — if two DAGs need to be
chained, use [Airflow Datasets](#airflow-datasets) instead.

### Topological order

`collect_dags` runs Kahn's algorithm (in `airflow_dag/resolve.rs::topo_sort`)
with alphabetical tie-breaking on the ready set, producing:

- A deterministic `tasks: Vec<String>` ordering.
- A `depends_on: BTreeMap<String, Vec<String>>` where each task is a key
  (even if it has no upstreams) and upstream lists are sorted + deduplicated.

Cycles are detected and surface as `cycle detected in DAG dependencies
involving: a, b, c`.

---

## Config inheritance

Every layer that can contribute Airflow config uses the same
[`AirflowSection`](#airflowsection-reference) shape. Later layers shallow-
override earlier layers via `merge_airflow_sections`
(`yard-core/src/parsing.rs`), which means any `Some` field in the overlay
replaces the base; unset (`None`) overlay fields leave the base unchanged.
Arrays follow the same rule — a non-empty `publishes` replaces the
inherited one wholesale.

The chain applied to each DAG (in `airflow_dag/collection.rs::resolve_dag_airflow_config`):

```
yard.yaml  providers.airflow
   │
   ▼
account.yaml  airflow:
   │
   ▼
region.yaml  airflow:
   │
   ▼
dag.yaml  (entire file body IS the airflow section)
   │
   ▼
per-job airflow.overrides  (at most one task per DAG — see below)
```

| Layer | Source | File |
|-------|--------|------|
| Project | `providers.airflow` in `yard.yaml` | Root manifest |
| Account | `airflow:` key in `account.yaml` | Any ancestor |
| Region | `airflow:` key in `region.yaml` | Any ancestor |
| DAG | entire top-level body of `dag.yaml` | DAG marker file |
| Task override | `airflow.<field>` on a task YAML (fields inherited from `AirflowSection`) | Per-job |

**Important:** `dag.yaml` is parsed as an `AirflowSection` directly — there is
no `airflow:` wrapper key inside it. Its body IS the airflow section.

### Single DAG-level override rule

A task's `airflow:` block is primarily for per-task metadata (`depends_on`,
`publishes`). It can also carry DAG-level field overrides (`schedule`,
`retries`, `owner`, `dags_bucket`, `dags_prefix`) via the flattened
`AirflowSection` fields — but **at most one task per DAG may declare any of
those DAG-level fields**. Declaring DAG-level overrides on two tasks in the
same DAG is an error:

```
DAG at '<path>' has DAG-level overrides declared on multiple tasks:
  'a' and 'b' — at most one task may declare
  schedule/retries/owner/dags_bucket/dags_prefix
```

Enforced in `airflow_dag/collection.rs::enforce_single_dag_level_override`.
Put DAG-level config in `dag.yaml` unless you have a specific reason not to.

---

## AirflowSection reference

Defined in `yard-structs/src/config.rs::AirflowSection`. Every layer uses this
shape.

| Field | Type | Where it can appear | Emitted as | Notes |
|-------|------|---------------------|-----------|-------|
| `schedule` | string | any layer | `schedule="<cron>"` on `DAG(...)` | Standard Airflow cron or preset (`@daily`, `@hourly`). Mutually exclusive with `trigger:` block — declaring both is rejected at validation. |
| `owner` | string | any layer | `default_args["owner"]` | Free-form string. |
| `retries` | int | any layer | `default_args["retries"]` | Passed through as an integer. |
| `dags_bucket` | string | any layer | — (deployment) | S3 bucket the generated `.py` is uploaded to during `yard apply`. Typically the MWAA DAGs bucket. |
| `dags_prefix` | string | any layer | — (deployment) | S3 key prefix under `dags_bucket`. Defaults to `dags/` when unset. |
| `trigger` | object (typed) | DAG layer | per-source schedule (Dataset list, sensor task chain, or `schedule=None` for API) | Optional event-driven trigger block. See [Airflow Datasets](#airflow-datasets) and [configuration](configuration.md#dagyaml-trigger-block). |
| `publishes` | array of strings | any layer | `_yard_publish` synthetic terminal task with `outlets=[Dataset("uri"), ...]` | DAG-level Dataset URIs published when every user task succeeds. Per-task `outlets=` is configured via per-job `airflow.publishes`. |
| `max_active_runs` | int (>=1) | DAG layer | `max_active_runs=N` on `DAG(...)` | Optional concurrency limit. Defaults to `1` for event-driven DAGs (CONC-01); Airflow's default of 16 for schedule-only DAGs. |
| `aws` | object | any layer | — (deployment) | Optional credential override for DAG upload/destroy. When set, this `aws:` block OVERRIDES the root+account.yaml cascade. Same shape as root `aws:` (`assume_role`, `session_name`, `external_id`). See [DAG bucket credentials](#dag-bucket-credentials). |

Unknown keys in an `airflow:` body are ignored — forward compatibility.

---

## AirflowJobBlock reference

Defined in `yard-structs/src/config.rs::AirflowJobBlock`. This is the shape of
the per-job `airflow:` block on a task YAML. Fields of `AirflowSection` are
flattened into it as `overrides`.

| Field | Type | Emitted as | Notes |
|-------|------|-----------|-------|
| `depends_on` | array of strings | `t_up >> t_down` edges at module level | See [`depends_on` semantics](#depends_on-semantics). |
| `publishes` | array of strings | `outlets=[Dataset("uri"), ...]` on the operator | Dataset URIs this task publishes. Completion of the task marks every listed Dataset. |
| `schedule`, `owner`, `retries`, `dags_bucket`, `dags_prefix`, `trigger`, `publishes`, `max_active_runs` | (inherited from `AirflowSection`) | DAG-level | See [Single DAG-level override rule](#single-dag-level-override-rule). |

Example task YAML:

```yaml
type: glue
role: arn:aws:iam::123456789012:role/GlueJob
airflow:
  depends_on:
    - orders
  publishes:
    - s3://example-bucket/sales/shipments
  # DAG-level fields are allowed but only on one task per DAG
  # schedule: "@hourly"
```

---

## Operator mapping

Implemented in `yard-core/src/airflow_dag/generation.rs::render_task`. One
operator class per `job_type`; imports are emitted lazily (only the operators
actually used are imported).

| `job_type` | Airflow operator | Import |
|------------|------------------|--------|
| `glue` | `GlueJobOperator` | `from airflow.providers.amazon.aws.operators.glue import GlueJobOperator` |
| `bash` | `BashOperator` | `from airflow.operators.bash import BashOperator` |
| anything else | (error) | — |

Any job type that is not `bash` or `glue` causes `generate_dag` to error with
`job type '<type>' is not supported in Airflow codegen yet`. This is a hard
stop at codegen time — no silent fallback.

Per-operator field mapping:

**`bash`**

```python
t_<task_id> = BashOperator(
    task_id="<task_id>",
    bash_command="<from config.command>",
    outlets=[Dataset(...), ...],   # only if publishes is non-empty
)
```

Missing `command` on a bash task surfaces as `bash task '<id>' is missing
'command'`.

**`glue`**

```python
t_<task_id> = GlueJobOperator(
    task_id="<task_id>",
    job_name="<task_id>",
    aws_conn_id="<derived>",       # see Cross-account connections
    outlets=[Dataset(...), ...],   # only if publishes is non-empty
)
```

`job_name` is the task id — yard names the deployed Glue job after the task,
so the operator can look it up by that name.

### Template

DAGs are rendered through `yard-core/src/templates/airflow_dag.py.tera`
(`AIRFLOW_DAG_TEMPLATE` in `airflow_dag/mod.rs`). The template's skeleton:

```python
# Generated by YARD for DAG: {{ dag_name }}
{{ required_connections_block }}
from datetime import datetime

from airflow import DAG
{{ imports_block }}

default_args = {{ default_args }}

with DAG(
    dag_id="{{ dag_name }}",
    default_args=default_args,
    schedule={{ schedule }},
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
{{ tasks_block }}

{{ deps_block }}
```

`start_date` is hardcoded to `datetime(2024, 1, 1)` and `catchup=False` is
unconditional. There is currently no knob to change either.
<!-- VERIFY: Airflow version compatibility — codegen emits `airflow.datasets.Dataset`, which is an Airflow 2.4+ feature, and the `schedule=` keyword is Airflow 2.4+ (older versions use `schedule_interval=`). MWAA must be on an Airflow 2.4+ environment image. -->

---

## Task and DAG identifiers

### DAG id

The DAG id (`dag_id=...` in the rendered Python) is:

```
<sanitized_project_name>_<sanitized_dag_dir_name>
```

For `project: my-proj` with a DAG directory `aws/dev/us-east-2/orders-pipeline`,
the id is `my_proj_orders_pipeline`.

### Task id and Python variable names

- `task_id=...` is the full job key from `ProjectManifest.jobs` (not the
  short `base_name`).
- The Python variable used for `>>` edges is `t_<sanitized_task_id>`
  (`python_var_name` in `airflow_dag/helpers.rs`).

### Sanitization rules

`sanitize_identifier` (`airflow_dag/helpers.rs`):

- Keeps `[A-Za-z0-9_]`.
- Replaces every other character with `_`.
- Prepends `_` if the first character is a digit.
- Maps the empty string to `_`.

So `9am-etl` → `_9am_etl`, `order.flow` → `order_flow`, `pipe-line` → `pipe_line`.

---

## Cross-account connections

When a Glue task runs against a different AWS account than yard's own root
role, the emitted `GlueJobOperator` needs its own `aws_conn_id` pointing at
the target account's role. This logic lives in
`yard-core/src/airflow_dag/connections.rs`.

### Conn id derivation

Per task, `resolve_task_aws_conn_id`:

1. If the task has no `_aws.assume_role` → `aws_conn_id="aws_default"`.
2. If the task's `_aws.assume_role` equals the project root `aws.assume_role`
   (same-account case) → `aws_conn_id="aws_default"`.
3. Otherwise, derive from the ARN:
   ```
   arn:aws:iam::222222222222:role/path/to/MyRole
      →  yard_222222222222_path_to_MyRole
   ```

Malformed role ARNs (wrong prefix, non-12-digit account, missing `role/`,
empty role name) surface as `malformed role ARN '<arn>': ...` at codegen
time, via `derive_aws_conn_id`. Non-role ARNs (e.g. `arn:aws:iam::X:user/...`)
are rejected.

### `# Required Airflow connections` block

When any Glue task in a DAG needs a non-default conn id, the rendered DAG file
is prefixed with a docstring-style block listing them:

```python
# Required Airflow connections (create in MWAA before running):
#   - yard_222222222222_GlueInvoker  ->  arn:aws:iam::222222222222:role/GlueInvoker
#   - yard_333333333333_GlueInvoker  ->  arn:aws:iam::333333333333:role/GlueInvoker
```

yard does **not** create these connections in MWAA for you — they must exist
before the DAG runs. `yard apply` also prints the same list after a
plan/apply that creates or modifies DAGs:

```
Required Airflow connections (create in MWAA before the DAG runs):
  - yard_222222222222_GlueInvoker  ->  arn:aws:iam::222222222222:role/GlueInvoker
```

`required_connections_for_dag` deduplicates across tasks and returns them in
deterministic (BTreeMap) order. Bash tasks are ignored by this path.
<!-- VERIFY: exact MWAA connection-management procedure (UI, API, Secrets Manager backend) — yard does not interact with MWAA beyond uploading the DAG file to S3. -->

---

## Airflow Datasets

yard supports [Airflow
Datasets](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/datasets.html)
on both sides of a dependency:

### Producing datasets (`publishes` on a task)

```yaml
# shipments.yaml
type: glue
role: arn:aws:iam::111111111111:role/GlueJob
airflow:
  publishes:
    - s3://example-bucket/sales/shipments
```

Emits `outlets=[Dataset("s3://example-bucket/sales/shipments")]` on the
operator. Multiple URIs are allowed and produce a list of `Dataset(...)`
entries in declaration order.

DAG-level `publishes:` (declared at the top of `dag.yaml`) instead emits a
synthetic terminal `_yard_publish = EmptyOperator(...)` task with the
`Dataset(...)` outlets, wired downstream of every leaf user task. See
[configuration](configuration.md#dagyaml-publishes) for the
`publishes:` reference.

### Consuming datasets (`trigger.dataset:` on a DAG)

```yaml
# dag.yaml
trigger:
  all:
    - dataset: { uri: s3://example-bucket/sales/orders }
    - dataset: { uri: s3://example-bucket/sales/shipments }
```

Emits (homogeneous-Datasets composite under `all:` uses Airflow 2.9 native
`&` operator):

```python
from airflow.datasets import Dataset
...
schedule=(Dataset("s3://example-bucket/sales/orders") & Dataset("s3://example-bucket/sales/shipments"))
```

A single-source variant (`trigger: { dataset: { uri: ... } }`) emits
`schedule=[Dataset(uri)]`. See [configuration](configuration.md#dagyaml-trigger-block)
for the full `trigger:` block reference.

### Precedence

The typed `trigger:` block is mutually exclusive with `schedule:`. Declaring
both at any layer is rejected at validation. See the
[decision matrix](configuration.md#dagyaml-decision-matrix--schedule-vs-trigger)
in `configuration.md` for the four-cell truth table.

The `from airflow.datasets import Dataset` import is emitted only when the
DAG uses datasets on either side.

### Backfill semantics per trigger source

| Source | Backfill works? | Notes |
|--------|-----------------|-------|
| `schedule:` cron | Yes | Standard `airflow dags backfill <dag_id>` replays missed cron runs. |
| `trigger.dataset:` | NO | Datasets have no `logical_date` — historical re-runs do NOT replay missed Dataset events. Use API-trigger replay (below) to backfill against synthetic `dag_run.conf` payloads. |
| `trigger.s3:` | Broken | The deferrable `S3KeySensor` re-pokes against current S3 state — the original landed object is not replayable from event history. If the object still exists at backfill time, the sensor fires; otherwise it times out. |
| `trigger.sqs:` | Broken AND DESTRUCTIVE | `SqsSensor` drains the real queue. Backfilling against a live queue consumes pending messages. Do not run. |
| `trigger.api:` | Yes | Pass `--conf` payload via Airflow CLI/REST: `airflow dags trigger <dag_id> --conf '{"s3_key": "...", "sqs_body": "..."}'`. This is the recommended escape hatch for Dataset / S3 / SQS replays. |

**Recommended replay pattern**: declare a sibling `trigger: { api: { ... } }` DAG that takes the same `op_kwargs` shape and re-runs the user task. yard's `op_kwargs` threading (Phase 31) already wires the canonical fields (`s3_key`, `s3_bucket`, `sqs_body`, etc.) regardless of trigger source — the same task code accepts both event-driven payloads and synthetic API payloads.

---

## DAG bucket credentials

When your Airflow DAG bucket lives in a different AWS account than your
deployment targets (common for MWAA in a shared-services account), add
an optional `aws:` sub-block to the airflow provider config — or to any
`dag.yaml` / `account.yaml` in the cascade. Example:

```yaml
providers:
  airflow:
    region: us-east-1
    dags_bucket: my-mwaa-dags
    dags_prefix: dags/
    aws:
      assume_role: arn:aws:iam::333333333333:role/MwaaDagUploader
      session_name: yard-dag-upload
```

Resolution order for DAG-upload credentials (highest precedence first):

1. The `aws:` sub-block on `providers.airflow` or on a DAG's resolved
   `AirflowSection` (after the airflow-section cascade: yard.yaml →
   account.yaml → region.yaml → dag.yaml → per-job overrides).
2. The root `aws:` block, shallow-merged with the nearest ancestor
   `account.yaml`'s `aws:` block (today's cascade; preserved when the
   new `aws:` sub-block is unset).

**`providers.airflow.aws` OVERRIDES the cascade when set — it does NOT
merge with `account.yaml`.** Operators who want hierarchical cascade
behavior should leave the airflow `aws:` unset and rely on root `aws:`
+ `account.yaml`.

**Per-job `_aws` is IGNORED for DAG upload.** This is a locked
invariant (test `dag_upload_credentials_ignore_job_aws`): a per-job
`assume_role` affects the job's Glue/EMR provider run, not the DAG
file upload target. If job and DAG buckets live in the same account,
set the same `aws:` at both layers; if they diverge, the DAG bucket
wins for upload/destroy.

**Destroy uses persisted state.** At apply time, yard writes the
effective DAG `aws:` into `DagState.aws` on the state file
(`_dag_<name>.json`). At destroy time, `destroy_dag` and
`destroy_all_dags` read that field and re-authenticate to the same
account — the DAG's source directory does NOT need to be present.

**Migration note for pre-Phase-9 state.** DAG state files written by
a pre-Phase-9 yard have no `aws` field; destroy falls back to the
caller-supplied `aws:` parameter (today sourced from the project
root). To populate `DagState.aws`, run `yard apply` against the DAG
once after upgrading to Phase 9.

Implementation:
`yard-core/src/dag_lifecycle.rs::upload_dag_to_s3`,
`yard-core/src/dag_lifecycle.rs::resolve_effective_dag_aws`, and
`yard-core/src/dag_lifecycle.rs::resolve_destroy_dag_aws`.

---

## Worked example: three-account deployment

A realistic configuration where state, deployment targets, and the
DAG bucket each live in different AWS accounts:

- **Account A (`111111111111`)** — holds the S3 state bucket.
- **Account B (`222222222222`)** — holds the Glue jobs and EMR
  clusters (deployment targets).
- **Account C (`333333333333`)** — holds the MWAA DAG bucket.

Yard is invoked from a CI role in a fourth "runner" account (or any
account whose IAM allows AssumeRole into roles in A, B, and C).

```yaml
# yard.yaml
project: trifecta

state:
  type: s3
  bucket: account-a-yard-state
  region: us-east-1
  key: trifecta/state/
  aws:
    assume_role: arn:aws:iam::111111111111:role/YardStateAccess

# Root aws: targets Account B (deployment targets). Jobs inherit this
# unless overridden in an account.yaml / region.yaml / job.yaml.
aws:
  assume_role: arn:aws:iam::222222222222:role/YardDeploy

providers:
  glue:
    region: us-east-1
    # no aws: here — inherits root (Account B)
  emr:
    region: us-east-1
    # no aws: here — inherits root (Account B)
  airflow:
    region: us-east-1
    dags_bucket: account-c-mwaa-dags
    dags_prefix: dags/
    aws:
      assume_role: arn:aws:iam::333333333333:role/MwaaDagUploader
```

With this config:

- `yard plan` / `apply` reads and writes state using the
  `arn:aws:iam::111111111111:role/YardStateAccess` role.
- Job deploys (Glue/EMR) use
  `arn:aws:iam::222222222222:role/YardDeploy`.
- DAG file uploads (and destroys) use
  `arn:aws:iam::333333333333:role/MwaaDagUploader`.

Each role's IAM trust policy must permit the yard caller to assume
it. For CI, you can additionally set any of these via env vars to
override yaml:

```bash
# In CI, for ephemeral credentials:
export YARD_STATE_AWS_ASSUME_ROLE=arn:aws:iam::111111111111:role/YardStateAccessCI
export YARD_STATE_AWS_EXTERNAL_ID=xid-ci-rotate-daily
export YARD_AWS_ASSUME_ROLE=arn:aws:iam::222222222222:role/YardDeployCI
# (No dedicated YARD_DAG_AWS_* vars today — the airflow `aws:` sub-block
# is yaml-only in Phase 9; DAG env overrides are a deferred follow-up.)
```

**Gotcha: external IDs in yaml vs env.** If your external id is a
rotating secret, prefer the env vars — yaml is typically git-tracked
and external IDs should not appear in commits.
`YARD_STATE_AWS_EXTERNAL_ID` overrides yaml at state-load time.

**Strictly-additive guarantee.** A `yard.yaml` without any of the
`aws:` sub-blocks above, and without any `YARD_*_AWS_*` envs set,
resolves credentials exactly as before Phase 9 — the default chain
for state, the existing root+account.yaml cascade for providers and
DAG uploads.

---

## Generation, deployment, destroy

### CLI commands

All Airflow codegen is triggered from the same CLI that handles jobs — there
is no dedicated `yard airflow` subcommand. The wiring lives in
`yard-cli/src/commands/`:

| Command | Wiring | What it does for DAGs |
|---------|--------|-----------------------|
| `yard plan` | `plan.rs` → `airflow_dag::collect_dags` + `calculate_dag_diffs` | Shows `+ Create DAG`, `~ Modify DAG`, `- Delete DAG` diffs alongside job diffs. No side effects. |
| `yard apply` | `apply.rs` → `yard_core::apply` → `dag_lifecycle::apply_dags` | Regenerates DAG Python, writes to `.yard/generated/dags/<dag>.py`, uploads to S3 if `dags_bucket` is set, persists DAG state. Prints required cross-account connections. |
| `yard show <dag_name>` | `show.rs` → `show_dag` | Prints the generated DAG Python to stdout without writing or uploading. Falls back from job lookup. |
| `yard destroy <dag_name>` | `destroy.rs` → `destroy_dag` | Deletes the DAG `.py` from S3 (if deployed), deletes DAG state, removes `.yard/generated/dags/<dag>.py`. |
| `yard destroy` (no target) | `destroy.rs` → `destroy_all` | Destroys every tracked DAG in addition to every tracked job. |
| `yard validate` | `validate.rs` | Does **not** run DAG validation currently — only per-job schema + Python syntax. Orphan `airflow:` blocks are caught at `yard apply`, not `yard validate`. |

### Local output

For every DAG in the diff, `apply_dags` (`yard-core/src/dag_lifecycle.rs`)
writes the rendered Python to:

```
<project_root>/.yard/generated/dags/<dag_name>.py
```

The directory is created if missing. The file stays on disk even after a
dry-run apply.

### S3 upload (MWAA)

When `dags_bucket` resolves to a non-empty string (from anywhere in the
inheritance chain), `apply_dags` uploads the generated file to S3:

```
s3://<dags_bucket>/<dags_prefix><dag_name>.py
```

where `dags_prefix` defaults to `dags/` when unset. The resulting `s3_uri` is
recorded in DAG state with `status: "deployed"`. When `dags_bucket` is unset,
the file is only written locally and `status` becomes `"generated"`.

Region for the S3 client is resolved in this order
(`dag_lifecycle.rs::extract_airflow_region`):

1. `providers.airflow.region` in `yard.yaml`.
2. The state backend's region if `state.type: s3`.
3. Error: `Cannot determine AWS region for DAG S3 upload. Set 'region' in
   providers.airflow or use an S3 state backend.`

For cross-account uploads, the `account.yaml` `aws:` block at the DAG's
directory is shallow-merged with the root `aws:` block
(`dag_lifecycle.rs::resolve_aws_for_dir`), so per-account AssumeRole overrides
apply.

Example project-level config:

```yaml
# yard.yaml
providers:
  airflow:
    region: us-east-1
    dags_bucket: your-mwaa-dags-bucket
    dags_prefix: dags/
```

### Destroy

`yard destroy <dag_name>` deletes the S3 object (if `s3_uri` was recorded),
deletes the DAG state row, and removes the local `.py` file. Destroy uses
`aws_config` from the root manifest (without the DAG directory's
`account.yaml` overrides, since destroy runs purely off state and may not
have filesystem context).

### State

DAG deployment state is persisted via the same storage backend as jobs
(`yard-core/src/storage.rs`). The per-DAG record (`DagState`) holds:

- `content_hash` — blake3 hash of the rendered Python.
- `config` — serialized `AirflowSection` at apply time.
- `tasks` — ordered task list.
- `status` — `"deployed"` or `"generated"`.
- `applied_at` — RFC 3339 timestamp.
- `s3_uri` — the upload URI, or `None` for local-only DAGs.

Diffs (`calculate_dag_diffs` in `dag_lifecycle.rs`) compare the new
`content_hash` against the stored one and surface field-level changes when
they differ.

---

## End-to-end example

### Directory layout

```
my-project/
  yard.yaml
  aws/
    dev/
      account.yaml
      us-east-2/
        region.yaml
        orders-pipeline/
          dag.yaml              # DAG marker
          orders.yaml           # task
          notify.yaml           # task (depends on orders)
```

### Files

**`yard.yaml`**

```yaml
project: my-proj
state:
  type: local
  path: .yard/state
providers:
  airflow:
    region: us-east-2
    dags_bucket: your-mwaa-dags-bucket
    dags_prefix: dags/
    owner: data-team
    retries: 1
  glue:
    script_bucket: your-glue-scripts-bucket
    region: us-east-2
```

**`aws/dev/us-east-2/orders-pipeline/dag.yaml`**

```yaml
schedule: "@hourly"
```

**`aws/dev/us-east-2/orders-pipeline/orders.yaml`**

```yaml
type: glue
role: arn:aws:iam::111111111111:role/GlueJob
airflow:
  publishes:
    - s3://example-bucket/sales/orders
# transforms/sources elided — see CONFIGURATION.md
```

**`aws/dev/us-east-2/orders-pipeline/notify.yaml`**

```yaml
type: bash
command: "echo 'orders pipeline done'"
airflow:
  depends_on:
    - orders
```

### Resulting DAG Python

Rendered by `generate_dag` for the resolved DAG (illustrative; actual
whitespace follows the template in
`yard-core/src/templates/airflow_dag.py.tera`):

```python
# Generated by YARD for DAG: my_proj_orders_pipeline

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.datasets import Dataset

default_args = {
    "owner": "data-team",
    "retries": 1,
}

with DAG(
    dag_id="my_proj_orders_pipeline",
    default_args=default_args,
    schedule="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    t_orders = GlueJobOperator(
        task_id="orders",
        job_name="orders",
        aws_conn_id="aws_default",
        outlets=[Dataset("s3://example-bucket/sales/orders")],
    )
    t_notify = BashOperator(
        task_id="notify",
        bash_command="echo 'orders pipeline done'",
    )

t_orders >> t_notify
```

### What `yard apply` does

1. Walks to find `orders-pipeline/dag.yaml`. DAG id = `my_proj_orders_pipeline`.
2. Resolves schedule: project `airflow` has none; `dag.yaml` contributes
   `@hourly`.
3. Resolves `owner` + `retries` from the project-level `providers.airflow`.
4. Topologically sorts `[orders, notify]` with `notify` depending on `orders`.
5. Renders the Python above and writes it to
   `.yard/generated/dags/my_proj_orders_pipeline.py`.
6. Uploads it to
   `s3://your-mwaa-dags-bucket/dags/my_proj_orders_pipeline.py`.
7. Persists DAG state with `status: "deployed"`.

Because no task declares a cross-account `_aws.assume_role`, the
`aws_conn_id` is `aws_default` and no `# Required Airflow connections` header
is emitted.

---

## Validation errors

Errors raised during DAG collection, resolution, and generation. Sources
noted in parentheses.

| Error | When | Source |
|-------|------|--------|
| `nested dag.yaml: '<inner>' is inside another DAG at '<outer>'` | Two DAG markers in an ancestor/descendant relationship. | `collection.rs` |
| `dag.yaml at '<path>' has no task files` | DAG directory has a marker but no jobs discovered under it. | `collection.rs` |
| `DAG at '<path>' has DAG-level overrides declared on multiple tasks: '<a>' and '<b>' — at most one task may declare schedule/retries/owner/dags_bucket/dags_prefix` | Two+ tasks in the same DAG carry DAG-level fields in their `airflow:` block. | `collection.rs::enforce_single_dag_level_override` |
| `task '<a>' in DAG at '<path>' depends on itself` | Self-dependency via full or short name. | `resolve.rs::resolve_dep` |
| `task '<a>' in DAG at '<path>' depends_on '<dep>', which is not a task in this DAG` | Unknown task name. | `resolve.rs::resolve_dep` |
| `task '<a>' in DAG at '<path>' depends_on '<dep>' which is ambiguous — matches: <list>. Use the full name to disambiguate.` | Short name resolves to multiple tasks. | `resolve.rs::resolve_dep` |
| `task '<a>' in DAG at '<path>' has cross-DAG depends_on '<dep>' — cross-DAG dependencies are not supported` | `dep` exists as a job but lives in a different DAG. | `resolve.rs::resolve_dep` |
| `cycle detected in DAG dependencies involving: <list>` | `depends_on` forms a cycle. | `resolve.rs::topo_sort` |
| `Job "<name>" has an airflow: block but is not inside a DAG directory (no ancestor dag.yaml found)...` | Orphan `airflow:` block. Surfaced by `yard apply` (not `yard validate`). | `helpers.rs::validate_orphan_airflow_blocks` |
| `DAG '<dag>' task '<id>': job type '<type>' is not supported in Airflow codegen yet` | Task has a job type other than `bash`/`glue`. | `generation.rs` |
| `bash task '<id>' is missing 'command'` | `command` field absent on a bash task. | `generation.rs::render_task` |
| `malformed role ARN '<arn>': ...` | A Glue task's `_aws.assume_role` is not a valid IAM role ARN. | `connections.rs::derive_aws_conn_id` |
| `Cannot determine AWS region for DAG S3 upload. Set 'region' in providers.airflow or use an S3 state backend.` | `dags_bucket` is set but no region can be resolved. | `dag_lifecycle.rs::extract_airflow_region` |

Every rendered DAG is additionally passed through
`validation::validate_python_syntax` in the module's own test suite, ensuring
the template never emits syntactically invalid Python for supported inputs.
Note this syntax check runs in tests only; at runtime, generated DAGs are not
re-parsed before upload.

---

## Airflow version matrix

yard's emitted DAGs target Airflow 2.9+ (the first version with native `&` / `|` Dataset operators and stable deferrable sensors).

| Track | Airflow | apache-airflow-providers-amazon | aiobotocore |
|-------|---------|----------------------------------|-------------|
| Modern | >= 2.11 | >= 9.x | >= 2.5.x |
| Conservative | >= 2.9 | 8.13.x — 8.x | >= 2.1.1 |

The `apache-airflow-providers-amazon` floor matters for the deferrable sensor implementations. The conservative track pins `apache-airflow-providers-amazon` at the 8.13.x line (last 8.x with stable Triggerer-side `S3KeySensor`); the modern track tracks 9.x for current `SqsSensor` payload-shape parity.

**Triggerer process required.** Deferrable sensors (`S3KeySensor(deferrable=True)`, `SqsSensor(deferrable=True)`) only fire when the Triggerer is running. MWAA enables this by default; self-hosted deployments may need to start it explicitly (`airflow triggerer`).

Every emitted event-driven DAG carries this version contract as a comment header, alongside per-source backfill caveats. Schedule-only DAGs render WITHOUT this banner — they have no version-floor requirement beyond Airflow 2.0.

---

## Limitations and planned work

Derived from code comments and the supported-types list in
`yard-core/src/airflow_dag/` and `yard-core/src/config_merge.rs`:

- **Supported job types:** only `bash` and `glue`. EMR (and any future
  provider that is not `bash`-like) errors out at codegen time. EMR Airflow
  support is not implemented.
- **Cross-DAG dependencies:** unsupported — use Datasets (`publishes:` +
  `trigger.dataset:`) for cross-DAG chaining. v1.6 emits a non-fatal
  `WARN: dag '<dag_id>': trigger.dataset "<uri>" has no publisher in this
  project (broken link, non-fatal)` when a `trigger.dataset:` URI has no
  matching `publishes:` entry anywhere in the project.
- **Hardcoded DAG settings:** `start_date=datetime(2024, 1, 1)` and
  `catchup=False` are fixed in the template.
- **Dependency wiring shape:** one `t_up >> t_down` edge per line, no
  grouping/chaining shorthand. Comment in `generation.rs` notes richer
  grouping is deferred.
- **MWAA connection management:** yard lists required cross-account
  connections but does not create them in MWAA.
- **`yard validate` does not validate DAG structure.** Orphan blocks,
  cycles, and unknown `depends_on` targets only surface at `yard plan` /
  `yard apply`.
- **No `airflow` subcommand** — there is no way to regenerate DAG Python
  without going through `yard apply` or `yard show`.
