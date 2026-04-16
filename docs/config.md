# Root config (yard.yaml)

The root config lives at the top of your project directory and defines the project name, state backend, yard's own AWS credentials, and provider defaults.

## Basic example

```yaml
project: my-project

state:
  type: local
  path: .yard/state/

# Optional: yard's own AWS creds (for deploying). Separate from the per-job
# execution role. When omitted, yard uses the default AWS provider chain.
aws:
  assume_role: arn:aws:iam::111122223333:role/YardDeployRole
  session_name: yard-apply       # optional, default "yard"
  external_id: acme-prod         # optional, only if target role requires it

providers:
  glue:
    region: us-east-1
    script_bucket: my-company-glue-scripts
    script_prefix: yard-scripts/
    warehouse: s3://my-company/lakehouse/   # required for iceberg sinks
    default_engine: spark                    # or "glue" for DynamicFrames
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

  airflow:
    region: us-east-1
    dags_bucket: my-mwaa-bucket
    dags_prefix: dags/
    schedule: "@daily"
    owner: data-team
    retries: 1
```

## AWS credentials for yard itself

Distinct from per-job execution roles (e.g. the Glue `role:` field). This block controls the credentials yard uses to talk to AWS APIs during `apply`/`destroy`.

**Resolution order:**
1. Env var `YARD_AWS_ASSUME_ROLE` wins over yaml `assume_role` (same for `YARD_AWS_SESSION_NAME`, `YARD_AWS_EXTERNAL_ID`).
2. If an `assume_role` is resolved, yard calls STS `AssumeRole` on top of the default AWS provider chain.
3. Otherwise, yard uses the default chain directly (env vars → profiles → IMDS/ECS task role → SSO).

## Config cascade (deep merge)

Every configuration layer is deep-merged using a four-layer precedence chain. Later layers win on conflict; unrelated sibling keys at every nesting depth are preserved.

```
yard.yaml  →  account.yaml  →  region.yaml  →  job.yaml
```

This applies to both `aws:` and provider blocks (e.g. `glue:`, `airflow:`).

### How deep merge works

Deep merge recurses into nested objects so you can override a single key of a nested map without wiping its siblings. Arrays and scalars are replaced wholesale.

**Example:** override one `default_arguments` key per job while keeping provider defaults:

```yaml
# yard.yaml
providers:
  glue:
    default_arguments:
      --enable-metrics: "true"
      --job-language: python
      --TempDir: s3://temp/
```

```yaml
# my-job.yaml
type: glue
glue:
  default_arguments:
    --job-language: scala    # only this key changes
```

**Result:** `{--enable-metrics: "true", --job-language: "scala", --TempDir: "s3://temp/"}`. The provider defaults for `--enable-metrics` and `--TempDir` are preserved.

### account.yaml and region.yaml overrides

Place an `account.yaml` or `region.yaml` file at any level in your directory tree. Jobs discover the nearest ancestor file and deep-merge its contents between the root and per-job layers.

**`account.yaml`** — override `aws:` and/or provider settings per account:

```yaml
# accounts/prod/account.yaml
account:
  id: "999988887777"

aws:
  assume_role: arn:aws:iam::999988887777:role/YardProdDeployRole

glue:
  script_bucket: prod-glue-scripts
  warehouse: s3://prod-warehouse/iceberg/
```

**`region.yaml`** — override settings per region:

```yaml
# accounts/prod/eu-west-1/region.yaml
region:
  id: eu-west-1

glue:
  region: eu-west-1
  warehouse: s3://eu-warehouse/iceberg/

airflow:
  dags_bucket: mwaa-eu-dags
```

All jobs under `accounts/prod/eu-west-1/` inherit these overrides. A job can still override further via its own `glue:` block.

### Inline AWS overrides on jobs

You can set `aws.assume_role` directly on a job file without requiring an `account.yaml` folder:

```yaml
# jobs/orders.yaml
type: glue
role: arn:aws:iam::222222222222:role/OrdersGlueExecution
aws:
  assume_role: arn:aws:iam::222222222222:role/YardGlueDeploy

source:
  type: s3
  path: s3://landing/orders/
sink:
  type: iceberg
  database: sales
  table: orders
```

The job's `aws:` block is deep-merged as the final layer: `yard.yaml → account.yaml → region.yaml → job-inline`. This is useful for cross-account deployments where a few jobs target a different AWS account than the project root.

## State backends

### Local

```yaml
state:
  type: local
  path: .yard/state/
```

State files are written to the local filesystem. Good for solo development.

### S3

```yaml
state:
  type: s3
  bucket: my-company-yard-state
  region: us-east-1
  key: my-project/state/
```

For teams. State is tracked per-job, not as a single blob. Each job gets its own state file and lock file. Two people can apply changes to different jobs concurrently -- same model as Terragrunt with independent modules.
