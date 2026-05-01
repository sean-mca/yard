<!-- generated-by: gsd-doc-writer -->
# Configuration

yard is configured through a hierarchy of YAML files that mirror your cloud
topology, a small set of environment variables consumed by the CLI and
providers, and — for `yard-server` — a separate set of env vars plus a
Settings page persisted in DynamoDB.

This document enumerates every discoverable configuration surface:

- [yard CLI YAML files](#yard-cli-yaml-files) — `yard.yaml`, `account.yaml`,
  `region.yaml`, per-job `<job>.yaml`, and `dag.yaml`
- [yard CLI environment variables](#yard-cli-environment-variables) — AWS
  credentials, AssumeRole overrides, color settings
- [yard-server environment variables](#yard-server-environment-variables) —
  GitHub, DynamoDB, listen port
- [yard-server Settings page](#yard-server-settings-page) — runtime settings
  persisted to DynamoDB
- [Required vs optional settings](#required-vs-optional-settings)
- [Per-environment overrides](#per-environment-overrides)

---

## yard CLI YAML files

yard uses a hierarchical config model. Files higher in the directory tree
provide defaults that are shallow-merged into descendant files. A typical
layout:

```
my-project/
  yard.yaml                      # root project manifest
  aws/
    dev/
      account.yaml               # account-level context
      us-east-2/
        region.yaml              # region-level context
        orders.yaml              # job definition
        dag.yaml                 # (optional) DAG marker
```

### `yard.yaml` (root project manifest)

Defined by `ProjectManifest` in `yard-structs/src/config.rs`.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `project` | string | Yes | Project name — used in state keys and plan output. |
| `state` | object | Yes | State backend config (see below). |
| `providers` | map | No | Per-provider default config, keyed by job type (e.g. `glue`, `emr`). Values are passed as-is to the provider. |
| `jobs` | map | No | Job definitions, normally populated by discovering `<job>.yaml` files rather than authored inline. |
| `aws` | object | No | Root-level AWS credential config (AssumeRole target, session name, external id, region). Per-job and per-DAG `account.yaml` `aws:` blocks shallow-override this. When absent, providers fall back to the default AWS credential provider chain. |

#### `state` — state backend

Two variants are defined by the `StateBackend` enum, discriminated by `type:`:

**Local backend:**

```yaml
state:
  type: local
  path: .yard/state
```

**S3 backend:**

```yaml
state:
  type: s3
  bucket: my-yard-state
  region: us-east-1
  key: projects/my-project/
```

All three S3 fields (`bucket`, `region`, `key`) are required when `type: s3`.

##### Cross-account state backend credentials

When your S3 state bucket lives in a different AWS account than the
identity running `yard`, add an optional `aws:` sub-block to the
`state:` config (Phase 9 addition):

```yaml
state:
  type: s3
  bucket: my-org-yard-state
  region: us-east-1
  key: my-project/state/
  aws:
    assume_role: arn:aws:iam::111111111111:role/YardStateAccess
    session_name: yard-ci        # optional; default "yard"
    external_id: xid-abc-123     # optional
```

Resolution order for state credentials (highest precedence first):

1. `YARD_STATE_AWS_ASSUME_ROLE` / `YARD_STATE_AWS_SESSION_NAME` /
   `YARD_STATE_AWS_EXTERNAL_ID` environment variables.
2. The `state.aws:` sub-block above.
3. The default AWS credential provider chain (env vars, shared config,
   IMDS / ECS task role, SSO).

**Strictly-additive guarantee.** A `yard.yaml` with NO `state.aws:`
block and NO `YARD_STATE_AWS_*` envs set resolves state credentials
exactly as before Phase 9 — the default chain. Existing configs
continue to work unchanged.

**State creds are orthogonal to provider creds.** The provider
`YARD_AWS_*` environment variables (`YARD_AWS_ASSUME_ROLE`, etc.) do
NOT affect state backend credentials. This lets CI scope state and
provider credentials independently: you can set
`YARD_STATE_AWS_ASSUME_ROLE` for the state bucket without changing
provider cred resolution.

**Local state backend has no creds.** The `aws:` sub-block only
applies to `type: s3`. A `type: local` state backend has no
credential concept.

Implementation: `yard-core/src/storage.rs::get_storage` and
`yard-core/src/storage.rs::merge_state_aws_with_env`.

#### `providers.glue` — AWS Glue provider defaults

Consumed by `GlueProvider::new` in `yard-core/src/providers/glue.rs`.

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `script_bucket` | Yes | — | S3 bucket where generated PySpark scripts are uploaded. |
| `script_prefix` | No | `yard-scripts/` | Key prefix under `script_bucket`. |
| `region` | No | `us-east-1` | AWS region for the Glue client. |
| `glue_version` | No | `4.0` | Glue runtime version. |
| `worker_type` | No | `G.1X` | Glue worker instance type. |
| `number_of_workers` | No | `2` | Number of Glue workers. |
| `timeout` | No | (unset) | Job timeout in minutes. |
| `max_retries` | No | (unset) | Maximum automatic retries. |
| `max_concurrent_runs` | No | (unset) | Max concurrent executions. |
| `bookmark` | No | (unset) | `enabled`/`true` sets `--job-bookmark-enable`; anything else sets `--job-bookmark-disable`. |
| `connections` | No | `[]` | Array of Glue connection names to attach. |
| `default_arguments` | No | `{}` | Extra `--key: value` arguments. `--datalake-formats: iceberg` is injected automatically unless overridden. |

#### `providers.emr` — AWS EMR (classic) provider defaults

Consumed by `EmrProvider::new` in `yard-core/src/providers/emr.rs`.

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `script_bucket` | Yes | — | S3 bucket for uploaded scripts. |
| `cluster_id` | Yes | — | ID of an existing EMR cluster (`j-XXXXXXXX`). |
| `script_prefix` | No | `yard-scripts/` | Key prefix under `script_bucket`. |
| `region` | No | `us-east-1` | AWS region. |
| `deploy_mode` | No | `cluster` | Passed to `spark-submit --deploy-mode`. |
| `action_on_failure` | No | `CONTINUE` | EMR step failure action (`CONTINUE`, `CANCEL_AND_WAIT`, `TERMINATE_CLUSTER`). |

#### `aws` (root-level)

Controls how yard itself obtains AWS credentials. Shape is free-form JSON
passed through `aws_config()` in `yard-core/src/providers/mod.rs`:

```yaml
aws:
  assume_role: arn:aws:iam::123456789012:role/YardDeployer
  session_name: yard          # optional, default "yard"
  external_id: my-ext-id      # optional
```

Environment variables (`YARD_AWS_ASSUME_ROLE`, `YARD_AWS_SESSION_NAME`,
`YARD_AWS_EXTERNAL_ID`) override the YAML values when set.

### `account.yaml` / `region.yaml` (hierarchical context)

Defined by `YARDContext` in `yard-structs/src/config.rs`. These are opaque
YAML blobs merged into every descendant job file. The top-level keys
recognized are:

| Key | Purpose |
|-----|---------|
| `account` | Account-level variables (e.g. `${account.id}`). |
| `region` | Region-level variables (e.g. `${region.id}`). |
| `transforms` | Shared transform snippets usable by jobs. |
| `dag` | DAG-level config lifted from a nearby `dag.yaml` marker. |

An `aws:` block may also appear at the `account.yaml` / `region.yaml` layer
and shallow-overrides the root `yard.yaml` `aws:` block per-job.

### `<job>.yaml` (individual job definitions)

Defined by `JobDefinition` in `yard-structs/src/config.rs`.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `type` (→ `job_type`) | string | Yes | Provider type: `glue` or `emr`. |
| `sources` | array | Usually | Input datasets — see source fields below. |
| `sink` | object | Usually | Output dataset — see sink fields below. |
| `transforms` | array | No | Ordered transform steps. |
| `imports` | array | No | Extra Python imports injected into the generated script. |
| `body` | string | No | Inline Python body appended to the generated script. |
| `job_file` | string | No | Path to an external Python file that replaces codegen entirely. |
| `airflow` | object | No | Per-job Airflow metadata (`depends_on`, `publishes`, plus overrides for `schedule`/`owner`/`retries`/etc.). |
| `partition_by` | array | No | Iceberg partition columns. Only `year`, `month`, `day` are supported. |
| `partition_timestamp_column` | string | No | Existing timestamp column to derive year/month/day from. Mutually exclusive with `create_timestamp`. |
| `create_timestamp` | bool | No | If true, adds `ingestion_timestamp = current_timestamp()` and derives partitions. Mutually exclusive with `partition_timestamp_column`. |
| `config` | object | No | Free-form provider-specific config merged with `providers.<type>` from `yard.yaml`. |

#### `sources[]` fields (`Source` struct)

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | Variable name — generates a `df_<name>` DataFrame. |
| `source_type` (often `type:`) | Yes | One of `s3`, `jdbc`, `catalog`, `kafka`, `api`. |
| `format` | Context-dependent | `parquet`, `csv`, `json`, `orc`. |
| `path` | s3 | S3 URI. |
| `connection_url` | jdbc/kafka | JDBC URL or Kafka bootstrap servers. |
| `table` / `database` | jdbc/catalog | Table and database names. |
| `secret_id` | No | AWS Secrets Manager secret for credentials. |
| `engine` | No | `spark` (SparkSession.read) or `glue` (DynamicFrame). Defaults to the provider's `default_engine`; falls back to `spark`. |
| `connection_type` | jdbc+glue | Glue connector name (`mysql`, `postgresql`, etc.). |
| `topic` | kafka | Kafka topic. |
| `url` / `headers` | api | HTTP GET URL and headers. |
| `options` | No | Opaque passthrough to `.option()` (spark) or `connection_options` (glue). |

#### `sink` fields (`Sink` struct)

| Field | Required | Description |
|-------|----------|-------------|
| `source` | No | Which DataFrame to write (defaults to first/only source). |
| `sink_type` (often `type:`) | Yes | `s3`, `jdbc`, or `catalog`. |
| `format` | Context-dependent | `parquet`, `csv`, `json`, `orc`. |
| `path` / `connection_url` / `table` / `database` / `secret_id` | Varies | Location + credentials. |
| `mode` | No | `overwrite`, `append`, or `error`. |
| `partition_by` | No | Partition columns. |
| `fill_nulls` | No | Iceberg-only. Defaults to true; set `false` to opt out of null/void coercion. |

#### `transforms[]` fields (`Transform` struct)

Transforms run in the order declared. Each entry's `transform_type`
selects one of nine operations, and every transform may set `source`
(the input DataFrame) and `output` (the result DataFrame name).

**Common fields (all transform types):**

| Field | Applies to | Description |
|-------|------------|-------------|
| `source` | all | Name of the DataFrame to operate on. Defaults to the first/only source, or the previous transform's output. |
| `output` | all | Name for the result DataFrame. Defaults to the same value as `source` (overwrites it in place). |

Per-type field reference follows. See `yard-core/src/codegen/transform.rs`
for the exact dispatch logic and `yard-structs/src/config.rs` (struct
`Transform`) for the full list of fields parsed from YAML.

##### `filter`

| Field | Required | Description |
|-------|----------|-------------|
| `condition` | Yes | PySpark Column expression string (inlined into `.filter(...)`). Defaults to `True` if omitted. |

```yaml
transforms:
  - transform_type: filter
    source: orders
    output: big_orders
    condition: F.col("amount") > 100
```

##### `sql`

| Field | Required | Description |
|-------|----------|-------------|
| `query` | Yes | Full SQL `SELECT` against registered temp views (all named sources are registered as views with their `name`). Defaults to `SELECT * FROM source` if omitted. |

```yaml
transforms:
  - transform_type: sql
    output: joined
    query: SELECT o.*, c.name FROM orders o JOIN customers c ON o.customer_id = c.id
```

##### `drop_columns`

| Field | Required | Description |
|-------|----------|-------------|
| `columns` | Yes | Array of column names to drop. |

```yaml
transforms:
  - transform_type: drop_columns
    source: orders
    columns: [internal_id, debug_flag]
```

##### `select`

| Field | Required | Description |
|-------|----------|-------------|
| `columns` | Yes | Array of columns to keep (dropped columns are everything else). |

```yaml
transforms:
  - transform_type: select
    source: orders
    columns: [order_id, customer_id, amount]
```

##### `rename`

| Field | Required | Description |
|-------|----------|-------------|
| `mapping` | Yes | `HashMap<String, String>` of old → new column names. Applied as successive `withColumnRenamed` calls. |

```yaml
transforms:
  - transform_type: rename
    source: orders
    mapping:
      cust_id: customer_id
      amt: amount
```

##### `add_column`

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | Name of the new column. |
| `expression` | No | PySpark expression to compute the column. Defaults to `lit(None)` if omitted. |

```yaml
transforms:
  - transform_type: add_column
    source: orders
    name: total_with_tax
    expression: F.col("amount") * 1.08
```

##### `join`

| Field | Required | Description |
|-------|----------|-------------|
| `left` | No | Left-side DataFrame name. Defaults to the first/only source. |
| `right` | Yes | Right-side DataFrame name. |
| `on` | Yes | Column name to join on. |
| `how` | No | Join type: `inner`, `left`, `right`, `outer`. Defaults to `inner`. |

```yaml
transforms:
  - transform_type: join
    left: orders
    right: customers
    on: customer_id
    how: inner
    output: orders_enriched
```

##### `aggregate`

| Field | Required | Description |
|-------|----------|-------------|
| `group_by` | Yes | Array of grouping column names. |
| `aggs` | Yes | `HashMap<alias, expression>`, e.g. `total: sum(amount)`. Each entry becomes `F.expr("<expression>").alias("<alias>")`. |

```yaml
transforms:
  - transform_type: aggregate
    source: orders
    output: totals_by_customer
    group_by: [customer_id]
    aggs:
      total: sum(amount)
      order_count: count(*)
```

##### `window`

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | Name of the new column populated by the window expression. |
| `expression` | Yes | Window function call (e.g. `row_number()`, `rank()`, `lag(amount, 1)`) wrapped by `F.expr(...)` and applied `.over(window_spec)`. |
| `partition_by` | No | Array of columns passed to `Window.partitionBy(...)`. Omit for an unpartitioned window. |
| `order_by` | No | Array of `{column, desc}` records passed to `Window.orderBy(...)`. `desc: true` emits `F.col("col").desc()`; otherwise `.asc()`. |

```yaml
transforms:
  - transform_type: window
    source: orders
    name: order_rank
    expression: row_number()
    partition_by: [customer_id]
    order_by:
      - column: created_at
        desc: true
```

### `dag.yaml` (DAG marker)

Presence of a `dag.yaml` file in a directory marks it as a DAG grouping.
All jobs under that directory become tasks in the generated Airflow DAG.
The file contents are parsed as an `AirflowSection`:

| Field | Type | Description |
|-------|------|-------------|
| `schedule` | string | Cron string. Mutually exclusive with `trigger:` block. |
| `owner` | string | DAG `owner` default arg. |
| `retries` | int | DAG `retries` default arg. |
| `dags_bucket` | string | S3 bucket where the generated DAG `.py` is uploaded (typically the MWAA DAGs bucket). |
| `dags_prefix` | string | Key prefix under `dags_bucket`. |
| `trigger` | object (typed) | Optional event-driven trigger block. See [trigger:](#dagyaml-trigger-block). Mutually exclusive with `schedule`. |
| `publishes` | array of strings | Dataset URIs published when the DAG completes. See [publishes:](#dagyaml-publishes). |
| `max_active_runs` | int (>=1) | Optional concurrency limit. Default `1` for event-driven DAGs (CONC-01); Airflow default (16) for schedule-only DAGs. |

The same `AirflowSection` shape may also appear under an `airflow:` block
in `yard.yaml`, `account.yaml`, `region.yaml`, and per-job files. Later
layers shallow-override earlier layers.

For the full Airflow reference — how DAGs are discovered and generated,
the operator mapping, dataset-based triggering, and per-job Airflow
metadata — see [airflow DAG reference](airflow-dag.md).

### dag.yaml: `trigger:` block

Single-source map. Exactly one of these five keys at the top level of `trigger:`:

- `schedule:` — cron string or preset (`"@daily"`, `"@hourly"`, etc.). Equivalent to top-level `schedule:`; the typed form is `trigger: { schedule: "@daily" }`.
- `dataset:` — Airflow Dataset URI consumer. Renders as `schedule=[Dataset(uri)]`.
  ```yaml
  trigger:
    dataset:
      uri: s3://example-bucket/raw/orders/
  ```
- `s3:` — S3 file-drop trigger via `S3KeySensor(deferrable=True)`. Required: `bucket` plus one of `key` (exact) or `prefix` (glob).
  ```yaml
  trigger:
    s3:
      bucket: example-landing-bucket
      prefix: incoming/orders/
      poke_interval: 60         # seconds, default 60, must be >= 10
      timeout: 86400            # seconds, default 86400
      deferrable: true          # default true; false emits legacy non-deferrable form
      aws_conn_id: yard_222222222222_GlueInvoker  # see "aws_conn_id resolution" below
  ```
- `sqs:` — SQS queue trigger via `SqsSensor(deferrable=True)`. Required: `queue_url`.
  ```yaml
  trigger:
    sqs:
      queue_url: https://sqs.us-east-1.amazonaws.com/123456789012/orders-events
      wait_time_seconds: 20     # default 20 (long polling)
      max_messages: 5           # default 5
      delete_message_on_reception: true  # default true
  ```
- `api:` — manual / external trigger. Renders `schedule=None`. Optional `description` and `payload_schema`.
  ```yaml
  trigger:
    api:
      description: "Triggered by upstream replay job"
      payload_schema:
        customer_id: string
        replay_window_start: string
  ```

### dag.yaml: `trigger.any:` / `trigger.all:` composites

Flat one-level lists of single-source variants:

```yaml
trigger:
  all:
    - dataset: { uri: s3://example-bucket/raw/orders/ }
    - dataset: { uri: s3://example-bucket/raw/customers/ }
```

- **Homogeneous Datasets (`any:` or `all:`)** — render via Airflow 2.9 native operators: `schedule=(Dataset(a) & Dataset(b))` for `all:`, `schedule=(Dataset(a) | Dataset(b))` for `any:`.
- **Heterogeneous `all:`** (mixing `dataset:` with `s3:` / `sqs:` / `api:`) — renders as a sensor chain plus `_yard_join` `EmptyOperator(trigger_rule="all_success")` synthesis. Sensor task IDs are deterministic: `_yard_wait_s3`, `_yard_wait_sqs`, `_yard_wait_dataset`, `_yard_wait_api`, `_yard_join`.
- **Heterogeneous `any:`** is REJECTED at validation — no clean Airflow primitive in v1.6. Split into multiple DAGs.
- Empty composites (`any: []`, `all: []`) and nested composites (`any: [{ all: [...] }]`) are REJECTED at validation.

### dag.yaml: `publishes:`

Top-level DAG-level Dataset producers. Each URI emits `Dataset(uri)` in a synthetic terminal task `_yard_publish` wired downstream of every leaf user task.

```yaml
publishes:
  - s3://example-bucket/processed/orders/
  - s3://example-bucket/processed/shipments/
```

Runtime semantics: the synthetic `_yard_publish = EmptyOperator(task_id="_yard_publish", outlets=[Dataset(...), ...])` runs after every user task succeeds (default `trigger_rule="all_success"`). Outlets fire on success. Per-task `outlets=` is still available via `airflow.publishes` on each `<job>.yaml` (renamed from `produces:`).

See [airflow DAG reference](airflow-dag.md#airflow-datasets) for runtime semantics and backfill caveats.

### dag.yaml: `max_active_runs:`

Optional concurrency limit. Must be `>= 1`.

- **Event-driven DAGs** (any DAG with a `trigger:` block) default to `max_active_runs=1`. Override by setting `max_active_runs: <N>` explicitly.
- **Schedule-only DAGs** preserve Airflow's default of `16` unless overridden.

### dag.yaml: per-source knobs

| Source | Knob | Default | Notes |
|--------|------|---------|-------|
| `s3` | `poke_interval` | 60 (seconds) | Must be `>= 10` (rejected at parse otherwise) |
| `s3` | `timeout` | 86400 (seconds) | |
| `s3` | `deferrable` | `true` | Set `false` for `apache-airflow-providers-amazon < 8.0.0` deployments |
| `s3` | `aws_conn_id` | resolved per cascade ladder below | Per-trigger override beats cascade. See "aws_conn_id resolution" below. |
| `sqs` | `wait_time_seconds` | 20 | Long polling — saves SQS API costs |
| `sqs` | `max_messages` | 5 | |
| `sqs` | `delete_message_on_reception` | `true` | |
| `api` | `description` | (none) | Free-form prose injected into DAG header |
| `api` | `payload_schema` | (none) | `field: type` map; doc-only — no runtime enforcement in Airflow 2.9 |
| `dataset` | `uri` | (required) | |

### dag.yaml: `aws_conn_id` resolution

yard derives the AWS connection ID for emitted sensors and the per-DAG `default_aws_conn_id` via this precedence ladder (highest first):

1. **Per-trigger explicit override** — e.g. `trigger.s3.aws_conn_id` set on a single trigger source. Wins for that one sensor.
2. **DAG-level cascaded `airflow.aws.aws_conn_id`** — set on `dag.yaml` `airflow.aws:` (or inherited via the cascade chain `yard.yaml → account → region → dag`). Becomes the DAG's `default_aws_conn_id`.
3. **Project-root `aws.aws_conn_id`** — set on the top-level `aws:` block in `yard.yaml`. Inherited via `cascade_provider_defaults` for jobs that don't override.
4. **`derive_aws_conn_id(assume_role)`** — synthesized from `aws.assume_role` ARN when set (e.g. `assume_role: arn:aws:iam::222222222222:role/GlueInvoker` yields `yard_222222222222_GlueInvoker`).
5. **Airflow's `aws_default`** — runtime fallback when none of the above resolve. yard emits no `aws_conn_id` kwarg in this case; the sensor uses the Airflow worker's default chain.

Empty strings are treated as unset at every layer (the typed-config helper filters them out), so setting `aws_conn_id: ""` at a more-specific layer falls through to the next tier — useful for intentional-strip overlays.

### dag.yaml: decision matrix — `schedule:` vs `trigger:`

| | `trigger:` declared | `trigger:` absent |
|---|---|---|
| **`schedule:` declared** | REJECTED at validation. Pick one — use `trigger: { schedule: "<cron>" }` if you need both forms in one DAG. | Schedule-only DAG. Renders `schedule="<cron>"`. PRES-02 byte-identical to pre-v1.6. |
| **`schedule:` absent** | Event-driven DAG. Renders per-source schedule (Dataset list, sensor task, or `schedule=None` for API). `max_active_runs=1` default applies. | DAG with no scheduling — Airflow defaults to manual trigger. Same as pre-v1.6 behavior. |

Migration from `triggered_by:` and `produces:` is documented in [v1.6 migration](migrations/v1.6.md).

---

## yard CLI environment variables

Discovered by greping `std::env::var` across `yard-cli/src/` and
`yard-core/src/`.

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `AWS_ACCESS_KEY_ID` | Conditional | — | Used by the AWS SDK default credential chain. Required unless another mechanism (AssumeRole, IMDS, SSO, `~/.aws/credentials`) provides credentials. <!-- VERIFY: exact IAM permissions required for Glue + EMR + S3 operations --> |
| `AWS_SECRET_ACCESS_KEY` | Conditional | — | Paired with `AWS_ACCESS_KEY_ID`. |
| `AWS_SESSION_TOKEN` | Conditional | — | For temporary credentials. |
| `AWS_REGION` | No | `us-east-1` (provider fallback) | Consumed by the AWS SDK. Provider `region` config overrides it. |
| `AWS_PROFILE` | No | — | Picks a named profile from `~/.aws/credentials`. |
| `YARD_AWS_ASSUME_ROLE` | No | — | Overrides the `aws.assume_role` field in YAML. When set, STS AssumeRole wraps the default credential chain. |
| `YARD_AWS_SESSION_NAME` | No | `yard` | STS session name. |
| `YARD_AWS_EXTERNAL_ID` | No | — | STS external id for cross-account roles. |
| `YARD_STATE_AWS_ASSUME_ROLE` | No | — | Overrides `state.aws.assume_role`. Applies ONLY to the S3 state backend — provider credentials are controlled by `YARD_AWS_ASSUME_ROLE` (separate scope). |
| `YARD_STATE_AWS_SESSION_NAME` | No | `yard` | STS session name for the state backend AssumeRole. |
| `YARD_STATE_AWS_EXTERNAL_ID` | No | — | STS external id for cross-account state access. |
| `NO_COLOR` | No | — | Disables ANSI colors in CLI output (https://no-color.org). The `--no-color` CLI flag has the same effect. |
| `USER` / `USERNAME` | No | `unknown` | Used as the lock owner in state lock files. |

---

## yard-server environment variables

The server crate has two environment-variable surfaces: runtime variables
read at server start (`yard-server/src/main.rs` and `yard-server/src/db/mod.rs`)
and one compile-time variable consumed by the Dioxus UI
(`yard-server/src/ui/mod.rs`).

See `env.local.example` at the repo root for a working local-dev template.

### Runtime (required)

| Variable | Required | Description |
|----------|----------|-------------|
| `YARD_GITHUB_TOKEN` | Yes | GitHub personal access token or app token. The server exits with `"YARD_GITHUB_TOKEN must be set"` if missing or empty. <!-- VERIFY: exact GitHub token scopes required --> |
| `YARD_WEBHOOK_SECRET` | Yes | Shared secret used to validate `X-Hub-Signature-256` on incoming GitHub webhooks. |
| `YARD_REPO_OWNER` | Yes | GitHub organization or user that owns the watched repo. |
| `YARD_REPO_NAME` | Yes | Name of the watched repo (without owner prefix). |

### Runtime (optional)

| Variable | Default | Description |
|----------|---------|-------------|
| `YARD_PORT` | `3001` | TCP port the API+UI listen on. Server binds `0.0.0.0:<port>`. |
| `YARD_DB_TABLE_PREFIX` | `yard` | Prefix for the DynamoDB table name. Final table is `{prefix}_yard` (e.g. default yields `yard_yard`). |
| `YARD_DB_REGION` | Falls back to `AWS_REGION`, then `us-east-1` | Region for the DynamoDB client. |
| `YARD_DB_ENDPOINT_URL` | (unset) | When set, points the DynamoDB client at a custom endpoint. Used for local development against `ministack` (`http://localhost:4566`). Unset in production so the client hits the real AWS endpoint. |
| `RUST_LOG` | `info` | `tracing-subscriber` filter directive. Standard `env_logger`-style syntax (`debug`, `yard_server=debug`, etc.). |

### Compile-time

| Variable | Default | Description |
|----------|---------|-------------|
| `YARD_API_BASE` | `http://127.0.0.1:3001` | Compile-time base URL used by the Dioxus UI to reach the API. Set to an empty string in production so the UI derives the host from `window().location()`. Resolved via `option_env!`, so rebuild is required to change it. |

### AWS credentials for DynamoDB / S3

The server uses the standard AWS SDK credential chain for DynamoDB (and,
transitively, any S3 operations it performs). The same
`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN` /
`AWS_PROFILE` / IMDS / SSO resolution applies as for the CLI.
`env.local.example` uses `test` / `test` against `ministack`.

---

## yard-server Settings page

Runtime settings that can be changed without restarting the server are
persisted in DynamoDB and exposed via `/api/settings` (`GET`/`POST`). The
Settings page in the Dioxus UI reads and writes these keys.

Allowed keys and their validation rules are defined in
`yard-server/src/api/settings.rs` (`validate_setting`).

| Key | Type | Allowed values | Default (if unset) | Description |
|-----|------|----------------|--------------------|-------------|
| `theme` | string | `light`, `dark`, `system` | `light` | UI theme. |
| `drift_interval` | string (minutes) | `1`, `3`, `5`, `10` | `3` | Interval between drift-check runs. Applied by the `drift_poll_loop` background task each iteration. |
| `dashboard_interval` | string (minutes) | any positive integer | `5` | Interval between dashboard cache refreshes. |
| `slack_enabled` | string bool | `true`, `false` | `false` (alerts disabled) | Master switch for drift-threshold Slack alerts. |
| `slack_webhook_url` | string | any (lenient) | (empty) | Slack Incoming Webhook URL used for alerts. <!-- VERIFY: Slack workspace / webhook channel configuration is set up outside the repo --> |
| `alert_drift_threshold` | string (u32) | integer `>= 1` | (unset — alerts off) | Minimum number of drifted jobs that triggers a Slack alert. |
| `alert_cooldown_minutes` | string (u64) | integer `>= 1` | `10` | Minimum minutes between consecutive alerts. |
| `alert_last_sent_at` | string (RFC 3339) | any (server-written) | — | Timestamp of the last successful alert. Written by the alerting loop; not meant to be edited via the UI. |

Invalid values cause `POST /api/settings` to return `400 Bad Request` and
no keys are written (validation is all-or-nothing).

---

## Required vs optional settings

The server will fail to start if any of these are missing or empty — they
are validated up-front by `required_env()` in `yard-server/src/main.rs`:

- `YARD_GITHUB_TOKEN`
- `YARD_WEBHOOK_SECRET`
- `YARD_REPO_OWNER`
- `YARD_REPO_NAME`

Additionally, DynamoDB connectivity is required at startup: the server
calls `DynamoDatabase::connect(...).migrate()` during boot and exits if
either call fails. This means AWS credentials resolvable by the default
chain (or a reachable `YARD_DB_ENDPOINT_URL`) are effectively required.

The CLI has no hard-required environment variables — AWS credential
resolution falls through the default chain, and missing credentials
surface as errors only when a provider command actually calls AWS.

The following YAML fields are hard-required (validated in
`yard-core/src/providers/*.rs`):

- `yard.yaml`: `project`, `state`
- `providers.glue`: `script_bucket`
- `providers.emr`: `script_bucket`, `cluster_id`
- Per-job: `job_type` (commonly `type:` in YAML)

---

## Per-environment overrides

yard does not ship a built-in `NODE_ENV`-style environment selector. The
following per-environment patterns are discoverable from the repo:

- **Hierarchical YAML.** The standard pattern is to put
  `account.yaml` and `region.yaml` under `aws/dev/`, `aws/staging/`,
  `aws/prod/`, etc. Each descendant job inherits the appropriate context
  by directory path. This is the primary mechanism for per-env
  differences (state buckets, IAM roles, VPC settings, etc.).
- **Local dev vs production for the server.** `env.local.example` is the
  template for local development against `ministack` — it sets
  `YARD_DB_ENDPOINT_URL=http://localhost:4566` and `AWS_ACCESS_KEY_ID=test`.
  In production, `YARD_DB_ENDPOINT_URL` is unset and real AWS credentials
  are supplied via the default credential chain. No `.env.production` or
  `.env.staging` file exists in the repo. <!-- VERIFY: production deployment platform and how production env vars are injected (Docker, ECS task definition, Fargate, etc.) -->
- **CI / AssumeRole overrides.** The `YARD_AWS_ASSUME_ROLE`,
  `YARD_AWS_SESSION_NAME`, and `YARD_AWS_EXTERNAL_ID` env vars exist
  specifically so CI can override any YAML-declared `aws:` block without
  editing config. For state-bucket credential overrides scoped
  separately from provider creds (e.g. state in Account A, providers in
  Account B), use `YARD_STATE_AWS_ASSUME_ROLE`,
  `YARD_STATE_AWS_SESSION_NAME`, and `YARD_STATE_AWS_EXTERNAL_ID`. See
  the state backend section above for the full cascade.
- **UI API base URL.** `YARD_API_BASE` is resolved at compile time via
  `option_env!`, so a production build of the server is typically
  compiled with `YARD_API_BASE=""` (so the UI derives its host from
  `window().location()`), while local dev uses the default
  `http://127.0.0.1:3001`.
