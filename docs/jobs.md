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
| `s3` | `path` | Read from S3 (parquet, csv, json) |
| `jdbc` | `connection_url`, `table` | Read from a database via JDBC |
| `catalog` | `database`, `table` | Read from the Glue Data Catalog |

JDBC sources support `secret_id` for automatic Secrets Manager credential fetching.

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
