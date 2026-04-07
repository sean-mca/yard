# YARD

**YAML Architecture for Rapid Development**

YARD is a declarative infrastructure tool for data engineering. You define ETL jobs in YAML, and YARD generates the underlying scripts, manages state, and deploys to cloud services like AWS Glue.

If you've used Terragrunt to manage Terraform across multiple environments, YARD applies the same ideas to data pipelines: hierarchical configuration, variable inheritance, per-job state files, and job-level locking.

## Why YARD?

Writing Glue jobs by hand means copy-pasting boilerplate, managing script uploads, and keeping track of what's deployed where. YARD lets you describe what your job does and handles the rest.

- Define sources, transforms, and sinks in YAML
- YARD generates the PySpark script and Glue boilerplate
- State is tracked per-job (locally or in S3), so changes are diffed before applying
- Job-level locking prevents concurrent modifications
- Provider-based architecture supports Glue today, with EMR, Databricks, and others planned

## Project structure

A typical YARD project looks like this:

```
my-project/
  yard.yaml                      # Root config: project name, state backend, provider settings
  aws/
    dev/
      account.yaml               # Account-level context (inherited by jobs below)
      us-east-2/
        region.yaml              # Region-level context
        orders.yaml              # Job definition
        customers.yaml           # Job definition
    prod/
      account.yaml
      us-east-1/
        region.yaml
        orders.yaml
```

The directory hierarchy mirrors your cloud topology: cloud provider, account, region. Context files (`account.yaml`, `region.yaml`) at each level are inherited by all job files below them, so the same job definition can be deployed across environments by placing it under different account/region directories.

### Root config (`yard.yaml`)

```yaml
project: my-project

state:
  type: local
  path: .yard/state/

providers:
  glue:
    region: us-east-1
    script_bucket: my-company-glue-scripts
    script_prefix: yard-scripts/
    role: arn:aws:iam::123456789:role/YardDeployRole
    # Runtime defaults for all Glue jobs
    worker_type: G.1X
    number_of_workers: 2
    glue_version: "4.0"
    timeout: 60
    bookmark: enabled
```

The `state` block controls where per-job state files are stored. For teams, use S3:

```yaml
state:
  type: s3
  bucket: my-company-yard-state
  region: us-east-1
  key: my-project/state/
```

The `providers` block configures deployment credentials and settings. The `role` here is the IAM role YARD uses to deploy -- it is separate from the execution role each job runs as.

Runtime settings like `worker_type`, `number_of_workers`, and `timeout` defined here serve as defaults for all jobs of that type. Individual jobs can override them.

### Context files

YARD uses hierarchical context files to inject variables into job definitions. These are resolved by walking up the directory tree from each job file, similar to how Terragrunt finds configuration in parent directories.

`account.yaml`:
```yaml
id: "123456789"
name: production
```

`region.yaml`:
```yaml
id: us-east-1
```

Variables are referenced in job YAML with `${account.id}`, `${region.id}`, etc. They are made available automatically and do not need to be referenced / called in a locals block.

### Job definitions

A job YAML file describes what the ETL job does: where it reads from, what transformations to apply, and where to write. Jobs inherit runtime settings from the `providers` block in `yard.yaml`, but can override any of them with a provider-specific block at the job level.

```yaml
type: glue
role: arn:aws:iam::123456789:role/GlueJobExecutionRole

source:
  type: s3
  format: parquet
  path: s3://data-lake/raw/orders/

transforms:
  - type: filter
    condition: "col('status') != 'cancelled'"
  - type: add_column
    name: processed_at
    expression: "current_timestamp()"

sink:
  type: s3
  format: parquet
  path: s3://data-lake/curated/orders/
  mode: overwrite
  partition_by:
    - year
    - month
```

The `role` on the job is the Glue execution role -- the IAM role the job runs as when processing data. This is distinct from the provider deploy role in `yard.yaml`.

#### Overriding provider defaults

If a specific job needs different runtime settings from the defaults in `yard.yaml`, add a provider-specific block to the job file. Job-level values take precedence over provider defaults:

```yaml
type: glue
role: arn:aws:iam::123456789:role/GlueJobExecutionRole

# Override defaults from yard.yaml for this job only
glue:
  worker_type: G.2X
  number_of_workers: 10
  timeout: 180
  bookmark: disabled

source:
  type: s3
  format: parquet
  path: s3://data-lake/raw/big-dataset/

sink:
  type: s3
  format: parquet
  path: s3://data-lake/curated/big-dataset/
  mode: overwrite
```

In this example, `worker_type`, `number_of_workers`, `timeout`, and `bookmark` are overridden for this job, while `glue_version`, `script_bucket`, and other settings are still inherited from the provider defaults.

#### Multiple sources and joins

Jobs can read from multiple sources and join them:

```yaml
type: glue
role: arn:aws:iam::123456789:role/GlueJobExecutionRole

sources:
  - name: orders
    type: s3
    format: parquet
    path: s3://data-lake/raw/orders/
  - name: customers
    type: s3
    format: parquet
    path: s3://data-lake/raw/customers/

transforms:
  - type: filter
    source: orders
    condition: "col('status') != 'cancelled'"
  - type: join
    left: orders
    right: customers
    on: customer_id
    how: left
    output: enriched
  - type: drop_columns
    source: enriched
    columns:
      - internal_id
      - debug_flag

sink:
  source: enriched
  type: s3
  format: parquet
  path: s3://data-lake/curated/enriched_orders/
  mode: overwrite
```

#### JDBC and Catalog sources

YARD supports reading from and writing to JDBC databases and the Glue Data Catalog:

```yaml
source:
  type: jdbc
  connection_url: jdbc:postgresql://host:5432/mydb
  table: public.users
  secret_id: my-rds-secret

sink:
  type: catalog
  database: curated
  table: users_clean
```

When a `secret_id` is specified, YARD generates code to fetch credentials from AWS Secrets Manager automatically.

#### SQL transforms

For complex logic, you can use SQL directly. All sources are registered as temp views:

```yaml
sources:
  - name: orders
    type: s3
    format: parquet
    path: s3://data-lake/raw/orders/
  - name: customers
    type: s3
    format: parquet
    path: s3://data-lake/raw/customers/

transforms:
  - type: sql
    output: enriched
    query: >
      SELECT o.*, c.name, c.segment
      FROM orders o
      JOIN customers c ON o.customer_id = c.id
      WHERE o.total > 0
```

#### External script file

For jobs with complex logic, you can point to an external Python file instead of using YARD's codegen. The file replaces YARD's generated script entirely — no Glue boilerplate is added, no sources/transforms/sinks are processed.

```yaml
type: glue
role: arn:aws:iam::123456789:role/GlueJobExecutionRole

job_file: ./my_custom_job.py
```

The path is relative to the job YAML file. This is useful when a job is complex enough that writing it in Python directly is more practical than expressing it in YAML. YARD still handles deployment, state tracking, and locking — it just uses your script as-is.

You can also use `body:` for short inline overrides that still get wrapped in the Glue template:

```yaml
type: glue
role: arn:aws:iam::123456789:role/GlueJobExecutionRole

body: |
  df = spark.read.format("parquet").load("s3://bucket/input/")
  df.write.format("parquet").save("s3://bucket/output/")
```

`body` and `job_file` cannot both be specified on the same job.

### Generated output

Given the first job example above, YARD generates a complete Glue script:

```python
# Generated by YARD for job: orders

import sys
import logging
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

logger = logging.getLogger("yard")
logger.setLevel(logging.INFO)

# --- Glue Setup ---
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


def run():
    # --- Sources ---
    df_source = spark.read.format("parquet").load("s3://data-lake/raw/orders/")

    # --- Transforms ---
    df_source = df_source.filter(col('status') != 'cancelled')
    df_source = df_source.withColumn("processed_at", current_timestamp())

    # --- Sink ---
    df_source.write.format("parquet").mode("overwrite").partitionBy("year", "month").save("s3://data-lake/curated/orders/")


if __name__ == "__main__":
    try:
        run()
        job.commit()
    except Exception as e:
        logger.error(f"Job failed: {str(e)}")
        raise
```

## CLI commands

All commands support `--no-color` to disable colored output, and `--colorblind` to use a palette distinguishable for red/green color vision deficiency (cyan/blue/magenta instead of green/yellow/red). The `NO_COLOR` environment variable is also respected.

### `yard init [directory]`

Initialize per-job state files for all jobs in the project. Skips jobs that already have state.

```bash
$ yard init
Initialized state for job "orders".
Initialized state for job "customers".
Initialized state for job "enriched_orders".
```

### `yard plan [directory] [--target <job_name>]`

Show what would change without modifying anything. Compares the current YAML definitions against stored state. Use `--target` to scope to a single job.

```
$ yard plan
--- Plan for my-project ---

  + Create job [orders]
  ~ Modify job [customers]
      script_name : "v1" -> "v2"
  - Delete job [old_job]
```

```
$ yard plan --target orders
--- Plan for my-project ---
(targeting: orders)

  + Create job [orders]
```

### `yard show <job_name> [directory]`

Display the generated script for a job without deploying or modifying state. Useful for reviewing what YARD will produce before running apply.

```
$ yard show orders
# Generated by YARD for job: orders

import sys
import logging
from awsglue.utils import getResolvedOptions
...
```

The output goes to stdout, so you can pipe it to a file or a pager: `yard show orders | less`.

### `yard validate [directory]`

Validate all job definitions. Checks the schema (source types, required fields, transform references, sink configuration, provider-specific config) and then generates each script and validates it is syntactically valid Python. Requires `python3` on the path.

```bash
$ yard validate
Validating job "orders"... OK
Validating job "customers"... OK
```

### `yard apply [directory] [--dry-run] [--auto-approve] [--target <job_name>]`

Apply changes: generate scripts, deploy to providers, and update state. All jobs are validated before any changes are made -- if any job has an invalid configuration, the entire apply is aborted. Shows the plan and asks for confirmation before proceeding. Each job is locked during its apply to prevent concurrent modifications.

```bash
$ yard apply
--- Plan for my-project ---

  + Create job [orders]
  ~ Modify job [customers]
      script_name : "v1" -> "v2"

Do you want to apply these changes? (y/n) y

Applying...
  + Created: orders
  ~ Modified: customers

State updated successfully.
```

Use `--dry-run` to see the plan without applying anything:

```bash
$ yard apply --dry-run
--- Plan for my-project ---

  + Create job [orders]

Dry run -- no changes applied.
```

Use `--auto-approve` to skip the confirmation prompt (useful in CI):

```bash
$ yard apply --auto-approve
```

### `yard destroy [job_name] [directory] [--dry-run] [--auto-approve]`

Tear down deployed jobs and remove their state. Shows what will be destroyed and asks for confirmation. Without a job name, destroys all jobs in the project. For each job, YARD calls the provider's destroy method to remove cloud resources, deletes the state file, and removes the generated script.

```bash
$ yard destroy
--- Destroy plan for my-project ---

  - Destroy job [orders]
  - Destroy job [customers]
  - Destroy job [enriched_orders]

Do you want to destroy all jobs? (y/n) y

Destroying...
  - Destroyed: orders
  - Destroyed: customers
  - Destroyed: enriched_orders

All jobs destroyed.
```

To destroy a single job:

```bash
$ yard destroy orders
--- Destroy plan ---

  - Destroy job [orders]

Do you want to destroy this job? (y/n) y

Destroying...
  - Destroyed: orders
```

Use `--dry-run` to see what would be destroyed, or `--auto-approve` to skip the prompt.

### `yard force-unlock <job_name> [directory]`

Remove a stale lock on a job. This is an escape hatch for when a process dies mid-apply and leaves a lock behind.

```bash
$ yard force-unlock orders
Removing lock on job "orders" (held by sean since 2026-04-05T14:30:00Z)
Lock removed.
```

## State management

YARD tracks state per-job, not as a single project blob. Each job gets its own state file (`<job_name>.json`) and its own lock file (`<job_name>.json.lock`).

For local state, these live in the `.yard/state/` directory. For S3, they live under the configured key prefix.

This means two people can apply changes to different jobs concurrently without blocking each other -- the same model Terragrunt uses for independent modules.

## Supported transforms

| Type | Description | Required fields |
|------|-------------|-----------------|
| `filter` | Filter rows by condition | `condition` |
| `sql` | Run a SQL query (sources registered as temp views) | `query` |
| `join` | Join two dataframes | `left`, `right`, `on` |
| `select` | Select specific columns | `columns` |
| `drop_columns` | Drop specific columns | `columns` |
| `rename` | Rename columns | `mapping` |
| `add_column` | Add a computed column | `name`, `expression` |

All transforms support optional `source` (which dataframe to operate on) and `output` (name for the result). If omitted, they default to the first source.

## yard-server

YARD includes a web server (`yard-server`) that provides a dashboard, GitHub webhook integration, and drift detection. It runs a Dioxus fullstack app with an axum API backend.

### Environment variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `YARD_GITHUB_TOKEN` | Yes | — | GitHub personal access token for API access |
| `YARD_WEBHOOK_SECRET` | Yes | — | Secret for verifying GitHub webhook signatures |
| `YARD_REPO_OWNER` | Yes | — | GitHub repository owner |
| `YARD_REPO_NAME` | Yes | — | GitHub repository name |
| `YARD_DB_TABLE_PREFIX` | No | `yard` | DynamoDB table name prefix (table: `{prefix}_yard`) |
| `YARD_DB_REGION` | No | `us-east-1` | AWS region for DynamoDB (falls back to `AWS_REGION`) |

Standard AWS credentials (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, or an IAM role/profile) must be configured for DynamoDB access. The server creates the table and indexes automatically on first startup.

### DynamoDB permissions

The IAM identity running yard-server needs the following DynamoDB permissions on the `{prefix}_yard` table:

- `dynamodb:CreateTable`
- `dynamodb:DescribeTable`
- `dynamodb:PutItem`
- `dynamodb:GetItem`
- `dynamodb:Query`

## Architecture

YARD is a Rust workspace with four crates:

- **yard-cli** -- Thin CLI wrapper. Parses arguments, calls into core, formats output.
- **yard-core** -- All business logic. Codegen, state management, storage, validation, provider deployment.
- **yard-structs** -- Shared data types. Job definitions, state structs, config types.
- **yard-server** -- Web dashboard and GitHub webhook handler. Dioxus fullstack app with DynamoDB persistence.

The provider system uses a trait-based architecture, so adding support for new services (EMR, Databricks, etc.) means implementing the `Provider` trait without touching existing code.
