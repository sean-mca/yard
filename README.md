
# YARD

**YAML Architecture for Rapid Development**

[![CI](https://github.com/sean-mca/yard/actions/workflows/ci.yml/badge.svg)](https://github.com/sean-mca/yard/actions/workflows/ci.yml)
[![License: BSL 1.1](https://img.shields.io/badge/License-BSL_1.1-blue.svg)](LICENSE)

Declarative infrastructure for data pipelines. Define ETL jobs in YAML, and YARD generates the PySpark scripts, manages state, and deploys to AWS. Think Terragrunt, but for data engineering.

## Demo

```yaml
# orders.yaml
type: glue
role: arn:aws:iam::123456789:role/GlueJobExecutionRole

source:
  type: s3
  format: parquet
  path: s3://data-lake/raw/orders/

transforms:
  - type: filter
    condition: "col('status') != 'cancelled'"

sink:
  type: s3
  format: parquet
  path: s3://data-lake/curated/orders/
  mode: overwrite
```

```
$ yard plan
--- Plan for my-project ---

  + Create job [orders]

$ yard apply --auto-approve
Applying...
  + Created: orders

State updated successfully.
```

That's it. YARD generated the PySpark script, uploaded it to S3, and created the Glue job. All you need to do is make your orchestrator aware of these new jobs, and schedule them.

## Providers

| Provider | Status | What it does |
|----------|--------|--------------|
| AWS Glue | Stable | Generates PySpark scripts, uploads to S3, creates/updates Glue jobs |
| AWS EMR (classic) | Stable | Generates PySpark scripts, uploads to S3, submits steps to existing clusters |
| AWS EMR Serverless | Planned | Submit job runs to serverless Spark applications |
| Airflow DAGs | Planned | Generates Airflow DAG Python files from YAML, uploads to a DAGs bucket |

## Project structure

```
my-project/
  yard.yaml                      # Root config: project name, state backend, providers
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

Directory hierarchy mirrors your cloud topology. Context files (`account.yaml`, `region.yaml`) at each level are inherited by all job files below them. Variables are referenced with `${account.id}`, `${region.id}`, etc.

### Root config (`yard.yaml`)

```yaml
project: my-project

state:
  type: local            # or s3
  path: .yard/state/

providers:
  glue:
    region: us-east-1
    script_bucket: my-company-glue-scripts
    script_prefix: yard-scripts/
    role: arn:aws:iam::123456789:role/YardDeployRole
    worker_type: G.1X
    number_of_workers: 2
    glue_version: "4.0"
    timeout: 60
    bookmark: enabled

  emr:
    region: us-east-1
    script_bucket: my-company-spark-scripts
    script_prefix: yard-scripts/
    cluster_id: j-ABC123DEF456
```

For teams, use S3 state:

```yaml
state:
  type: s3
  bucket: my-company-yard-state
  region: us-east-1
  key: my-project/state/
```

### Job definitions

Jobs describe what the ETL does: sources, transforms, and sinks. Runtime settings inherit from providers in `yard.yaml` but can be overridden per-job.

**Glue job:**
```yaml
type: glue
role: arn:aws:iam::123456789:role/GlueJobExecutionRole

source:
  type: s3
  format: parquet
  path: s3://data-lake/raw/orders/

sink:
  type: s3
  format: parquet
  path: s3://data-lake/curated/orders/
  mode: overwrite
```

**EMR job:**
```yaml
type: emr

source:
  type: s3
  format: parquet
  path: s3://data-lake/raw/orders/

sink:
  type: s3
  format: parquet
  path: s3://data-lake/curated/orders/
  mode: overwrite
```

Same YAML structure, different `type`. YARD generates the right template -- Glue gets `GlueContext`/`Job` boilerplate, EMR gets a plain `SparkSession`.

#### Overriding provider defaults

```yaml
type: glue
role: arn:aws:iam::123456789:role/GlueJobExecutionRole

glue:
  worker_type: G.2X
  number_of_workers: 10
  timeout: 180

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

#### Multiple sources and joins

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

sink:
  source: enriched
  type: s3
  format: parquet
  path: s3://data-lake/curated/enriched_orders/
  mode: overwrite
```

#### SQL transforms

All sources are registered as temp views:

```yaml
transforms:
  - type: sql
    output: enriched
    query: >
      SELECT o.*, c.name, c.segment
      FROM orders o
      JOIN customers c ON o.customer_id = c.id
      WHERE o.total > 0
```

#### Source types

| Type | Required fields | Description |
|------|-----------------|-------------|
| `s3` | `path` | Read from S3 (parquet, csv, json) |
| `jdbc` | `connection_url`, `table` | Read from a database via JDBC |
| `catalog` | `database`, `table` | Read from the Glue Data Catalog |

JDBC sources support `secret_id` for automatic Secrets Manager credential fetching.

#### Transforms

| Type | Required fields | Description |
|------|-----------------|-------------|
| `filter` | `condition` | Filter rows |
| `sql` | `query` | Run a SQL query (sources as temp views) |
| `join` | `left`, `right`, `on` | Join two dataframes |
| `select` | `columns` | Select specific columns |
| `drop_columns` | `columns` | Drop columns |
| `rename` | `mapping` | Rename columns |
| `add_column` | `name`, `expression` | Add a computed column |
| `aggregate` | `group_by`, `aggs` | Group and aggregate (sum, count, avg, etc.) |
| `window` | `name`, `expression`, `partition_by` and/or `order_by` | Add a column computed with a window function |

All transforms support optional `source` and `output` fields. Default to first source if omitted.

#### Aggregate

```yaml
- type: aggregate
  group_by: [region, day]
  aggs:
    total: "sum(amount)"
    order_count: "count(*)"
```

`aggs` is a map of `alias -> expression`. Expressions use Spark SQL syntax and are wrapped in `F.expr(...)`, so any aggregate function available to Spark SQL works (`sum`, `avg`, `count`, `count(distinct ...)`, `percentile_approx`, etc.).

#### Window functions

```yaml
- type: window
  name: row_num
  expression: "row_number()"
  partition_by: [customer_id]
  order_by:
    - column: created_at
      desc: true
    - column: id
```

`order_by` entries are `{column, desc}` — `desc` defaults to `false`. At least one of `partition_by` or `order_by` is required. Each `window` transform adds a single column; chain multiple transforms for more.

#### External scripts

For complex jobs, point to your own Python file. YARD handles deployment and state, you write the script:

```yaml
type: glue
role: arn:aws:iam::123456789:role/GlueJobExecutionRole
job_file: ./my_custom_job.py
```

Or inline with `body:` (gets wrapped in the provider template):

```yaml
body: |
  df = spark.read.format("parquet").load("s3://bucket/input/")
  df.write.format("parquet").save("s3://bucket/output/")
```

## CLI

```
yard init              Initialize state for all jobs
yard plan              Show what would change
yard apply             Deploy changes (with confirmation)
yard show <job>        Display the generated script
yard validate          Check all job definitions
yard destroy [job]     Tear down deployed jobs
yard force-unlock <job>  Remove a stale lock
```

All commands support `--no-color` and `--colorblind` (cyan/blue/magenta palette). `--target <job>` scopes plan/apply to a single job. `--auto-approve` and `--dry-run` work on apply and destroy.

## State management

State is tracked per-job, not as a single blob. Each job gets its own state file and lock file. Two people can apply changes to different jobs concurrently -- same model as Terragrunt with independent modules.

State backends: local filesystem (`.yard/state/`) or S3.

## yard-server

Web dashboard with GitHub webhook integration and drift detection. Dioxus fullstack app with axum API backend and DynamoDB persistence.

- PR-driven workflow (Atlantis-style): plan runs automatically on PR open, apply triggered by commenting `yard apply` on the PR
- Live drift detection -- compares repo config against deployed state on a configurable interval
- Dashboard with PR status, plan results, job counts
- Settings persistence (theme, drift interval, Slack webhook)

### GitHub webhook setup

Configure your repo's webhook to send `pull_request` and `issue_comment` events to `https://your-server/api/webhook/github`. Set the secret to match `YARD_WEBHOOK_SECRET`.

**Flow:**
1. Open a PR -- yard-server auto-runs `yard plan` and posts the result as a comment
2. Review the plan output in the PR
3. Comment `yard apply` -- yard-server runs `yard apply` and posts the result
4. Merge the PR

### Environment variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `YARD_GITHUB_TOKEN` | Yes | -- | GitHub personal access token |
| `YARD_WEBHOOK_SECRET` | Yes | -- | Webhook HMAC secret |
| `YARD_REPO_OWNER` | Yes | -- | GitHub repo owner |
| `YARD_REPO_NAME` | Yes | -- | GitHub repo name |
| `YARD_DB_TABLE_PREFIX` | No | `yard` | DynamoDB table prefix |
| `YARD_DB_REGION` | No | `us-east-1` | AWS region for DynamoDB |
| `YARD_DB_ENDPOINT_URL` | No | -- | Custom endpoint (for local dev) |
| `YARD_API_BASE` | No | `http://127.0.0.1:3001` | API base URL (compile-time, set to `""` for production) |

AWS credentials are required for DynamoDB. The server creates the table and indexes on first startup.

### Local development

```bash
docker compose up -d                              # ministack: S3 + DynamoDB on localhost:4566
cp env.local.example .env.local                    # fill in GitHub token
set -a && source .env.local && set +a && cd yard-server && dx serve  # start the server
```

### DynamoDB permissions

`dynamodb:CreateTable`, `dynamodb:DescribeTable`, `dynamodb:PutItem`, `dynamodb:GetItem`, `dynamodb:Query`

## Architecture

Rust workspace with four crates:

| Crate | Purpose |
|-------|---------|
| `yard-cli` | Thin CLI wrapper -- parses args, calls core, formats output |
| `yard-core` | Business logic -- codegen, state, storage, validation, providers |
| `yard-structs` | Shared types -- job definitions, state, config |
| `yard-server` | Web dashboard -- Dioxus fullstack, axum API, DynamoDB |

Provider system is trait-based. Adding a new provider means implementing the `Provider` trait -- no changes to existing code.
