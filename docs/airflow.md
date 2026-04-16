# Airflow DAG generation

YARD generates Airflow DAG Python files from YAML definitions and uploads them to an S3 bucket (typically your MWAA DAGs folder). Each DAG is a directory with a `dag.yaml` marker file, and all job files in that directory become tasks in the DAG.

## Setup

Add an `airflow` provider to your `yard.yaml`:

```yaml
providers:
  airflow:
    region: us-east-1
    dags_bucket: my-mwaa-bucket
    dags_prefix: dags/
    schedule: "@daily"
    owner: data-team
    retries: 1
```

## Directory layout

Any directory depth works — group DAGs into sub-folders to keep the repo tidy.

```
my-project/
  yard.yaml
  pipelines/
    sales/
      dag.yaml                 # DAG marker
      orders.yaml              # Task: Glue job
      shipments.yaml           # Task: Glue job
      notify.yaml              # Task: bash command
    aggregates/
      dag.yaml                 # Another DAG
      daily-summary.yaml       # Task: Glue job
```

DAG names are prefixed with the project name and the directory name: `{project}_{dir}`. For example, `datalake_sales`. Same-named directories in different groups don't collide because the full relative path is incorporated.

The `dag.yaml` file marks a directory as a DAG. It can be empty or contain DAG-level overrides:

```yaml
# dag.yaml
schedule: "0 6 * * *"
owner: pipeline-team
```

## Task types

Jobs in a DAG directory become Airflow tasks. The operator is chosen based on job type:

| Job type | Operator | Import |
|----------|----------|--------|
| `bash` | `BashOperator` | `from airflow.operators.bash import BashOperator` |
| `glue` | `GlueJobOperator` | `from airflow.providers.amazon.aws.operators.glue import GlueJobOperator` |

### Glue task

```yaml
# orders.yaml
type: glue
role: arn:aws:iam::123456789:role/GlueJobExecutionRole

source:
  type: s3
  format: parquet
  path: s3://data-lake/raw/orders/

sink:
  type: iceberg
  database: sales
  table: orders
  path: s3://warehouse/sales/orders/
  mode: append

airflow:
  depends_on: []
```

### Bash task

```yaml
# notify.yaml
type: bash
command: "echo 'Pipeline complete' | aws sns publish --topic-arn arn:aws:sns:us-east-1:123456789:alerts --message file:///dev/stdin"

airflow:
  depends_on: [orders, shipments]
```

## Dependencies

Use `depends_on` in the per-job `airflow:` block to declare task ordering.

```yaml
airflow:
  depends_on:
    - orders
    - shipments
```

### Short-name resolution

You can reference tasks by their **base filename** (e.g. `orders`) or their **full prefixed name** (e.g. `sales-orders`). YARD resolves short names automatically within the same DAG.

If a short name is ambiguous (two tasks in the same DAG have the same base filename), YARD errors with a message listing the matches and asking you to use the full name to disambiguate.

YARD performs cycle detection and validates that all referenced tasks exist. Within-DAG dependencies use `depends_on`; for cross-DAG orchestration, see [Datasets](#cross-dag-dependencies-datasets) below.

## Cross-account DAG deployments

A common pattern: MWAA runs in account A, but Glue jobs are deployed to account B. YARD handles this natively through the config cascade and per-job `aws:` overrides.

### Setup

```yaml
# yard.yaml — project root (account A is MWAA home)
project: datalake

aws:
  assume_role: arn:aws:iam::AAAA:role/YardOperator

providers:
  airflow:
    region: us-east-1
    dags_bucket: mwaa-dags-account-a       # lives in A
    dags_prefix: dags/
    schedule: "@daily"
    owner: data-team
  glue:
    region: us-east-1
    script_bucket: glue-scripts-account-b  # lives in B
    script_prefix: yard-scripts/
    warehouse: s3://warehouse-account-b/iceberg/
    worker_type: G.1X
    number_of_workers: 2
    glue_version: "4.0"
```

```yaml
# pipelines/sales/orders.yaml — Glue job in account B
type: glue
role: arn:aws:iam::BBBB:role/OrdersGlueExecution
aws:
  assume_role: arn:aws:iam::BBBB:role/YardGlueDeploy

source:
  type: s3
  format: parquet
  path: s3://landing-account-b/orders/

sink:
  type: iceberg
  database: sales
  table: orders
  path: s3://warehouse-account-b/iceberg/sales/orders/
  mode: append

airflow:
  depends_on: []
```

### What happens on `yard apply`

| Artifact | Destination | Credentials |
|----------|-------------|-------------|
| DAG `.py` file | `mwaa-dags-account-a/dags/` | Root `aws.assume_role` (A:YardOperator) |
| PySpark script | `glue-scripts-account-b/yard-scripts/` | Job's `aws.assume_role` (B:YardGlueDeploy) |
| Glue job resource | Account B | Job's `aws.assume_role` (B:YardGlueDeploy) |

DAG uploads always use the root (or account.yaml) `aws.assume_role`, never the per-job role. This ensures the DAG lands in the MWAA account regardless of which account individual jobs target.

### Cross-account connection wiring

When a Glue task's `aws.assume_role` differs from the project root, YARD:

1. Emits `aws_conn_id="yard_<account>_<role_name>"` on the `GlueJobOperator` instead of `"aws_default"`.
2. Adds a docstring header to the generated DAG listing the required Airflow connections.
3. Prints the required connections in the CLI output after `yard apply`.

**Example generated task:**

```python
t_sales_orders = GlueJobOperator(
    task_id="sales-orders",
    job_name="sales-orders",
    aws_conn_id="yard_222222222222_YardGlueDeploy",
)
```

**Required setup in MWAA:** create an Airflow connection named `yard_222222222222_YardGlueDeploy` with:
- Connection type: `Amazon Web Services`
- Extra: `{"role_arn": "arn:aws:iam::222222222222:role/YardGlueDeploy"}`

Jobs that share the same `aws.assume_role` as the project root use `aws_conn_id="aws_default"` — no extra connection needed.

### IAM requirements

YARD does not manage IAM. You (or your Terraform/CDK layer) must set up:

1. **Operator role** (account A) — needs `sts:AssumeRole` on cross-account deploy roles, `s3:PutObject` on the DAGs bucket, and state backend access.
2. **Cross-account deploy role** (account B) — trust policy allows the operator role to assume it. Permissions: `glue:CreateJob`, `UpdateJob`, `DeleteJob`, `GetJob`, `iam:PassRole`, `s3:PutObject` on the scripts bucket.
3. **Glue execution role** (account B) — the `role:` field on the job. Trust policy: `glue.amazonaws.com`. Permissions: read/write data sources and sinks.
4. **Glue invoker role** (account B, optional) — for MWAA runtime. Only needed if the Airflow connection should use a narrower role than the deploy role.

## Cross-DAG dependencies (Datasets)

Airflow Datasets (2.4+) let you trigger one DAG when another DAG's task completes, without polling or sensors.

### Producer: `produces`

Add `produces` to a task's `airflow:` block to declare what data it writes. The URI is a logical identifier — Airflow doesn't access it; it's just the key that links producers to consumers.

```yaml
# pipelines/sales/orders.yaml
type: glue
role: arn:aws:iam::222222222222:role/OrdersGlueExecution

source:
  type: s3
  path: s3://landing/orders/

sink:
  type: iceberg
  database: sales
  table: orders
  path: s3://warehouse/sales/orders/

airflow:
  depends_on: []
  produces:
    - s3://warehouse/sales/orders/
```

This emits `outlets=[Dataset("s3://warehouse/sales/orders/")]` on the Airflow operator. When the task completes, Airflow marks this dataset as "updated."

A task can produce multiple datasets:

```yaml
airflow:
  produces:
    - s3://warehouse/sales/orders/
    - s3://warehouse/sales/order_items/
```

### Consumer: `triggered_by`

Set `triggered_by` in `dag.yaml` to make the entire DAG trigger on dataset updates instead of a cron schedule:

```yaml
# pipelines/aggregates/dag.yaml
triggered_by:
  - s3://warehouse/sales/orders/
owner: data-team
```

This generates `schedule=[Dataset("s3://warehouse/sales/orders/")]` on the DAG. Airflow fires the DAG when all listed datasets have been updated.

When `triggered_by` is set, it takes precedence over any inherited `schedule` from the project or account level. You don't need to explicitly unset the schedule.

A consumer can wait on multiple datasets — Airflow triggers the DAG when **all** are updated:

```yaml
# dag.yaml
triggered_by:
  - s3://warehouse/sales/orders/
  - s3://warehouse/sales/shipments/
```

### Full example: producer + consumer DAGs

```
pipelines/
  sales/
    dag.yaml                    # schedule: "@daily"
    orders.yaml                 # produces: [s3://warehouse/sales/orders/]
    shipments.yaml
    notify.yaml
  aggregates/
    dag.yaml                    # triggered_by: [s3://warehouse/sales/orders/]
    daily-summary.yaml
```

**Producer DAG output** (`yard show datalake_sales`):

```python
# Generated by YARD for DAG: datalake_sales

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.datasets import Dataset

default_args = {
    "owner": "data-team",
}

with DAG(
    dag_id="datalake_sales",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    t_sales_orders = GlueJobOperator(
        task_id="sales-orders",
        job_name="sales-orders",
        aws_conn_id="aws_default",
        outlets=[Dataset("s3://warehouse/sales/orders/")],
    )
    t_sales_shipments = GlueJobOperator(
        task_id="sales-shipments",
        job_name="sales-shipments",
        aws_conn_id="aws_default",
    )
    t_sales_notify = BashOperator(
        task_id="sales-notify",
        bash_command="echo done",
    )

t_sales_orders >> t_sales_shipments
t_sales_orders >> t_sales_notify
t_sales_shipments >> t_sales_notify
```

**Consumer DAG output** (`yard show datalake_aggregates`):

```python
# Generated by YARD for DAG: datalake_aggregates

from datetime import datetime

from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.datasets import Dataset

default_args = {
    "owner": "data-team",
}

with DAG(
    dag_id="datalake_aggregates",
    default_args=default_args,
    schedule=[Dataset("s3://warehouse/sales/orders/")],
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    t_aggregates_daily_summary = GlueJobOperator(
        task_id="aggregates-daily-summary",
        job_name="aggregates-daily-summary",
        aws_conn_id="aws_default",
    )

# No task dependencies
```

The consumer DAG has no cron schedule — it runs automatically when the `orders` task in `datalake_sales` completes successfully.

### Notes

- Datasets require **Airflow 2.4+** (MWAA 2.5+ supports them natively).
- The dataset URI is a logical identifier, not a physical locator. Airflow does not access or validate it. Use a URI that's meaningful and unique (the sink path is a natural choice).
- The Datasets tab in the Airflow UI shows which DAGs produce and consume each dataset, and when each was last updated.
- `produces` goes on individual tasks (job-level `airflow:` block). `triggered_by` goes on the DAG (`dag.yaml`).

## Config inheritance

Airflow configuration cascades through the directory hierarchy with deep merge at each level:

```
yard.yaml providers.airflow     # Project defaults
  → account.yaml airflow:      # Account overrides
    → region.yaml airflow:     # Region overrides
      → dag.yaml               # DAG overrides
        → job airflow:         # Per-task overrides (DAG-level fields only)
```

Later values win. At most one job per DAG may declare DAG-level fields (schedule, owner, retries, dags_bucket, dags_prefix). If none do, the DAG inherits from the nearest ancestor.

Provider configuration follows the same cascade:

```
yard.yaml providers.glue        # Project defaults
  → account.yaml glue:         # Account overrides
    → region.yaml glue:        # Region overrides
      → job glue:              # Per-job overrides
```

See [Config cascade](config.md#config-cascade-deep-merge) for details.

## Generated output

YARD generates a Python file per DAG and uploads it to `s3://{dags_bucket}/{dags_prefix}{dag_name}.py`. DAG name = `{project}_{sanitized_dir_name}`.

Use `yard show <dag_name>` to preview the generated DAG without deploying:

```bash
yard show datalake_sales /path/to/project
```

## Using YARD with MWAA

- Point `providers.airflow.dags_bucket` at your MWAA S3 bucket.
- `dags_prefix` should match MWAA's DAG folder (default `dags/`).
- MWAA polls S3 roughly every 5 minutes -- apply succeeds on upload, DAG activation is async.
- MWAA's execution role needs `glue:StartJobRun` + `glue:GetJobRun` for GlueJobOperator tasks.
- For cross-account Glue tasks, create the Airflow connection printed by `yard apply` in the MWAA UI under Admin > Connections.
- BashOperator runs in MWAA's worker environment -- binaries must be available there (manage via requirements.txt or startup scripts).
- For Dataset-triggered DAGs, ensure your MWAA environment is version 2.5 or later.
