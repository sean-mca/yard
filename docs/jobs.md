# Job definitions

Jobs describe what the ETL does: sources, transforms, and sinks. Runtime settings inherit from providers in `yard.yaml` but can be overridden per-job.

## Glue job

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

## EMR job

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

## Overriding provider defaults

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

## Multiple sources and joins

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

## Source types

| Type | Required fields | Description |
|------|-----------------|-------------|
| `s3` | `path` | Read from S3 (parquet, csv, json, orc) |
| `jdbc` | `connection_url`, `table` | Read from a database via JDBC |
| `catalog` | `database`, `table` | Read from the Glue Data Catalog |
| `kafka` | `connection_url` (bootstrap servers), `topic` | Batch read from a Kafka topic |
| `api` | `url` | HTTP GET → JSON → DataFrame (JSON root must be a list) |

JDBC sources support `secret_id` for automatic Secrets Manager credential fetching. API sources support `headers:` (string→string map).

All sources support an opaque `options:` map that passes through to Spark's `.option(k, v)` chain (or to Glue's `connection_options` when using the glue engine).

### Engine: Spark DataFrame vs Glue DynamicFrame

For `s3` and `jdbc` sources you can pick the read engine. Default is Spark; set `engine: glue` to use `glueContext.create_dynamic_frame.from_options(...).toDF()` instead. A project-level default lives under `providers.glue.default_engine`.

```yaml
# events_ingest.yaml
sources:
  - name: events
    type: s3
    engine: glue
    format: json
    path: s3://bucket/raw/events/
    options:
      recurse: true
      groupFiles: inPartition
      compressionType: gzip
```

For `jdbc + engine: glue`, also set `connection_type:` to pick the Glue connector (`mysql`, `postgresql`, `sqlserver`, `oracle`, `redshift`). `catalog` sources are always DynamicFrame-backed. `kafka` and `api` have a single renderer each.

## Sink types

| Type | Required fields | Description |
|------|-----------------|-------------|
| `s3` | `path` | Write parquet/csv/json/orc to S3 |
| `jdbc` | `connection_url`, `table` | Write via JDBC |
| `catalog` | `database`, `table` | Write through the Glue Data Catalog |
| `iceberg` | `database`, `table` | Write to an Iceberg table registered in the Glue Catalog |

### Iceberg sinks

```yaml
sink:
  type: iceberg
  database: analytics
  table: events
  path: s3://warehouse/analytics/events/   # optional: custom table location
  mode: append           # append | overwrite (dynamic partition overwrite)
  fill_nulls: true       # default; set false to skip null/void coercion
```

When `path` is set, it becomes the Iceberg table's physical storage location via `.tableProperty("location", "...")` at table creation time. If omitted, Iceberg uses the default warehouse path from `providers.glue.warehouse`. The `path` only applies on first create — existing tables are not relocated.

**What yard emits for you:**

- A SparkSession with `glue_catalog` pre-configured against `providers.glue.warehouse`.
- A boto3 check for the Glue database — creates it if missing.
- `CREATE TABLE IF NOT EXISTS glue_catalog.<db>.<table>` via `writeTo(...).using("iceberg")`, with partitioning (if set) and sensible table properties: `format-version=2`, `write.spark.accept-any-schema=true`, `write.target-file-size-bytes=512MB`, `write.parquet.compression-codec=zstd`, `write.distribution-mode=hash`.
- Subsequent writes use `option("mergeSchema", "true")` so new source columns append automatically.

**Null/void coercion (`fill_nulls`, default `true`):** JSON ingestion can produce `void`-typed columns and all-null nested structs that fail Iceberg writes. Yard inlines a `_yard_fill_nulls(df)` pass that coerces those into type-appropriate defaults (empty string, `0`, `false`, empty struct, empty array) before writing. Opt-out with `fill_nulls: false` when you've pre-cleaned the data.

### Job-level partitioning (Iceberg only)

Set partitions at the top of the job file; yard derives them from a timestamp column and passes them through to `writeTo(...).partitionedBy(...)` on first create.

```yaml
partition_by: [year, month, day]      # subset of {year, month, day}
create_timestamp: true                 # adds ingestion_timestamp column + derives from it
# or, to derive from an existing column:
# partition_timestamp_column: event_time
```

Exactly one of `create_timestamp: true` or `partition_timestamp_column:` must be set. The column derivations are idempotent — if `year` already exists on the DataFrame, yard leaves it alone.

## Transforms

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

### SQL transforms

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

### Aggregate

```yaml
- type: aggregate
  group_by: [region, day]
  aggs:
    total: "sum(amount)"
    order_count: "count(*)"
```

`aggs` is a map of `alias -> expression`. Expressions use Spark SQL syntax and are wrapped in `F.expr(...)`, so any aggregate function available to Spark SQL works (`sum`, `avg`, `count`, `count(distinct ...)`, `percentile_approx`, etc.).

### Window functions

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

`order_by` entries are `{column, desc}` -- `desc` defaults to `false`. At least one of `partition_by` or `order_by` is required. Each `window` transform adds a single column; chain multiple transforms for more.

## External scripts

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
