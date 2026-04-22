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
| `airflow` | object | No | Per-job Airflow metadata (`depends_on`, `produces`, plus overrides for `schedule`/`owner`/`retries`/etc.). |
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

`transform_type` selects one of: `filter`, `sql`, `drop_columns`, `rename`,
`select`, `add_column`, `join`, `aggregate`, `window`. Each type uses a
subset of the available fields — see `yard-structs/src/config.rs` for the
full list.

### `dag.yaml` (DAG marker)

Presence of a `dag.yaml` file in a directory marks it as a DAG grouping.
All jobs under that directory become tasks in the generated Airflow DAG.
The file contents are parsed as an `AirflowSection`:

| Field | Type | Description |
|-------|------|-------------|
| `schedule` | string | Cron string. Mutually exclusive with `triggered_by`. |
| `owner` | string | DAG `owner` default arg. |
| `retries` | int | DAG `retries` default arg. |
| `dags_bucket` | string | S3 bucket where the generated DAG `.py` is uploaded (typically the MWAA DAGs bucket). |
| `dags_prefix` | string | Key prefix under `dags_bucket`. |
| `triggered_by` | array | Dataset URIs that trigger this DAG. When set, `schedule` becomes `[Dataset("uri"), ...]`. |

The same `AirflowSection` shape may also appear under an `airflow:` block
in `yard.yaml`, `account.yaml`, `region.yaml`, and per-job files. Later
layers shallow-override earlier layers.

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
  editing config.
- **UI API base URL.** `YARD_API_BASE` is resolved at compile time via
  `option_env!`, so a production build of the server is typically
  compiled with `YARD_API_BASE=""` (so the UI derives its host from
  `window().location()`), while local dev uses the default
  `http://127.0.0.1:3001`.
