<!-- generated-by: gsd-doc-writer -->
# Configuration

yard is configured through a hierarchy of YAML files that mirror your cloud
topology, a small set of environment variables consumed by the CLI and
providers.

This document enumerates every discoverable configuration surface:

- [yard CLI YAML files](#yard-cli-yaml-files) — `yard.yaml`, `account.yaml`,
  `region.yaml`, and per-job `<job>.yaml`
- [yard CLI environment variables](#yard-cli-environment-variables) — AWS
  credentials, AssumeRole overrides, color settings
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

#### `providers.<type>` — provider defaults

Each key under `providers:` names a plugin type and must match the `type:` field
of the jobs that use it (`providers.glue` for `type: glue`, and so on).

Since v2.0 the accepted fields are defined by the plugin, not by yard: core
passes the merged block through to the plugin and validates it against whatever
the plugin's `schema()` operation reports. For the field list, consult the
plugin's own documentation — e.g. the `yard-plugin-glue` and `yard-plugin-emr`
repositories (see [providers/glue.md](providers/glue.md) and
[providers/emr.md](providers/emr.md)).

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

An `aws:` block may also appear at the `account.yaml` / `region.yaml` layer
and shallow-overrides the root `yard.yaml` `aws:` block per-job.

### `<job>.yaml` (individual job definitions)

Defined by `JobDefinition` in `yard-structs/src/config.rs`.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `type` (→ `job_type`) | string | Yes | Plugin provider type, e.g. `glue`, `emr`. Resolves the plugin binary name as `yard-plugin-<type>`. |
| `plugin_version` | string | Yes | Version of the provider plugin to use, e.g. `"0.3.1"`. Must be set together with `plugin_source`. |
| `plugin_source` | string | Yes | URL template yard downloads the plugin binary from. Supports `${name}`, `${version}`, `${os}`, `${arch}`. Must be set together with `plugin_version`. |
| `sources` | array | Usually | Input datasets — see source fields below. |
| `sink` | object | Usually | Output dataset — see sink fields below. |
| `transforms` | array | No | Ordered transform steps. |
| `imports` | array | No | Extra Python imports injected into the generated script. |
| `body` | string | No | Inline Python body appended to the generated script. |
| `job_file` | string | No | Path to an external Python file that replaces codegen entirely. |
| `airflow` | object | No | *Vestigial in v2.0.* Core no longer generates Airflow DAGs, so this block produces no output. It is still parsed, and `airflow.trigger` is still mixed into the job's config hash — editing it will show up as a diff in `yard plan`. |
| `partition_by` | array | No | Iceberg partition columns. Only `year`, `month`, `day` are supported. |
| `mask_pii` | array | No | PII entity types to detect and redact. Interpreted by the provider plugin. See [`mask_pii`](#mask_pii) below. |
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

**`secret_id` JSON schema:**

When `source_type: jdbc`, the referenced AWS Secrets Manager secret's `SecretString` MUST be a JSON object with this shape:

```json
{
  "username": "<jdbc-user>",
  "password": "<jdbc-password>"
}
```

These keys are read literally by the emitted PySpark (`_secret["username"]` / `_secret["password"]`); other key names will cause the script to error at job execution time.

Consumed by `render_secret_fetch` in `yard-core/src/codegen/helpers.rs` (line 101); the jdbc consumer is at `yard-core/src/codegen/source.rs` (line 80).

**Gotcha (`secret_id` on non-jdbc):** If `secret_id` is set on a non-jdbc source (`s3`, `catalog`, `kafka`, `api`), `render_secret_fetch` still emits the boto3 SecretsManager fetch lines, but no codegen arm reads `_secret`. The fetch is silently unused — jobs run but waste a SecretsManager call. AWS Secrets Manager setup (creating the secret, granting `secretsmanager:GetSecretValue` to the job role) is out of scope for this page; see AWS docs.

#### `sink` fields (`Sink` struct)

| Field | Required | Description |
|-------|----------|-------------|
| `source` | No | Which DataFrame to write (defaults to first/only source). |
| `sink_type` (often `type:`) | Yes | `s3`, `jdbc`, or `catalog`. |
| `format` | Context-dependent | `parquet`, `csv`, `json`, `orc`. |
| `path` | s3 | S3 URI for the output. |
| `connection_url` | jdbc | JDBC URL. |
| `table` / `database` | jdbc / catalog / iceberg | Table and database names. |
| `secret_id` | No | AWS Secrets Manager secret for credentials. |
| `mode` | No | `overwrite`, `append`, or `error`. |
| `partition_by` | No | Partition columns. |
| `fill_nulls` | No | Iceberg-only. Defaults to true; set `false` to opt out of null/void coercion. |

**`secret_id` JSON schema:**

Same `{"username": "...", "password": "..."}` shape as for sources. Consumed when `sink_type: jdbc` — see `yard-core/src/codegen/sink.rs` (line 48). The non-jdbc dead-code gotcha applies the same way: setting `secret_id` on `s3` / `catalog` / `iceberg` sinks emits an unused SecretsManager fetch.

For the full schema and rationale, see the [Source `secret_id` schema](#sources-fields-source-struct) section above.

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

#### `mask_pii`

`mask_pii` declares which PII entity types to detect and redact in the
generated Glue script. Each entry names an AWS entity type; the codegen
emits a single `EntityDetector.detect()` call that handles all listed
types in one pass.

Defined by `JobDefinition` in `yard-structs/src/config.rs`.

**Worked example:**

```yaml
# orders.yaml
type: glue
sources:
  - name: events
    type: s3
    format: parquet
    path: s3://data-lake/raw/events/
mask_pii:
  - USA_SSN
  - CREDIT_CARD
  - EMAIL
sink:
  type: s3
  format: parquet
  path: s3://data-lake/clean/events/
  mode: overwrite
```

**What the generated code does:**

After all transforms run and before the sink write, the codegen inserts a
PII masking block that:

1. Converts the sink DataFrame to a `DynamicFrame` via
   `DynamicFrame.fromDF()`.
2. Calls `EntityDetector.detect()` with a fine-grained
   `detectionParameters` dict — one key per entity type, each configured
   with `REDACT` action and `"****"` mask text.
3. Converts the result back to a DataFrame via `.toDF()`.
4. Drops the `DetectedEntities` metadata column that `EntityDetector`
   appends.

All intermediate variables use the `_yard_pii_` prefix to avoid
collisions with user-defined names.

**Constraints:**

- **Glue 3.0+ required.** `EntityDetector` is part of the
  `awsglueml.transforms` module available in Glue 3.0 and later. Jobs
  targeting earlier Glue versions will fail at runtime.
- **`body` / `job_file` silently skips PII.** When either override is
  set, yard replaces codegen entirely — the `mask_pii` entries have no
  effect. No warning is emitted because the override is intentional.
- **EMR and Bash jobs are not supported.** Setting `mask_pii` on a
  non-Glue job type is rejected at validation.

**Entity type format:** Values must be `SCREAMING_SNAKE_CASE` (e.g.
`USA_SSN`, `CREDIT_CARD`, `EMAIL`). Duplicates are rejected at
validation. The full list of supported entity types is defined by AWS —
see the
[AWS Glue PII detection documentation](https://docs.aws.amazon.com/glue/latest/dg/detect-PII.html).

## yard CLI environment variables

Discovered by greping `std::env::var` across `yard-cli/src/` and
`yard-core/src/`.

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `AWS_ACCESS_KEY_ID` | Conditional | — | Used by the AWS SDK default credential chain. Required unless another mechanism (AssumeRole, IMDS, SSO, `~/.aws/credentials`) provides credentials. Cross-account guidance lives at [how-to/cross-account-deploy.md](../how-to/cross-account-deploy.md). |
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

## Required vs optional settings

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
- **CI / AssumeRole overrides.** The `YARD_AWS_ASSUME_ROLE`,
  `YARD_AWS_SESSION_NAME`, and `YARD_AWS_EXTERNAL_ID` env vars exist
  specifically so CI can override any YAML-declared `aws:` block without
  editing config. For state-bucket credential overrides scoped
  separately from provider creds (e.g. state in Account A, providers in
  Account B), use `YARD_STATE_AWS_ASSUME_ROLE`,
  `YARD_STATE_AWS_SESSION_NAME`, and `YARD_STATE_AWS_EXTERNAL_ID`. See
  the state backend section above for the full cascade.

---

## Directory scoping with --dir

The `--dir <path>` flag scopes `plan`, `apply`, `destroy`, and `validate`
to all jobs discovered under a directory subtree. This is the directory-level
equivalent of `--target` (which selects a single job by name).

### How it works

`--dir` takes a path relative to the project root (where `yard.yaml` lives)
or an absolute path. yard canonicalizes both the provided path and the
project root, then retains only jobs whose resolved directory starts with
the `--dir` path prefix.

```bash
# Plan only jobs under staging/us-east-1/
yard plan --dir staging/us-east-1

# Apply with dry-run, scoped to a region
yard apply --dir production/eu-west-1 --dry-run

# Destroy all jobs under a subtree
yard destroy --dir staging/us-east-1 --auto-approve

# Validate only jobs in a specific directory
yard validate --dir staging/
```

Output includes a `(scoped to: <path>/)` line so it is clear which subtree
is active:

```
--- Plan for my-project ---
(scoped to: staging/us-east-1/)

  + Create job [etl-orders]
  + Create job [etl-users]
```

### Mutual exclusivity

`--dir` and `--target` are mutually exclusive on `plan`, `apply`, and
`validate`. For `destroy`, `--dir` is mutually exclusive with the
positional `JOB_NAME` argument. Passing both produces a clap conflict
error before any code runs.

### Error behavior

| Condition | Error message |
|-----------|---------------|
| Path does not exist | `directory not found: <resolved path>` |
| Path is a file, not a directory | `expected a directory, got a file -- use --target for single jobs` |
| Path is outside the project root | `directory <path> is outside the project root <root>` |
| No jobs found under the path | `no jobs found under <path> -- the directory exists but contains no YAML job files` |
