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

**Account-level overrides** in `account.yaml` shallow-merge over the root `aws:` block:

```yaml
# aws/prod/account.yaml
aws:
  assume_role: arn:aws:iam::999988887777:role/YardProdDeployRole
```

All jobs and DAGs under `aws/prod/` use the prod role; jobs elsewhere keep the root role. Env vars still win over both.

**`external_id`** is only required when the target role's trust policy has an `sts:ExternalId` condition (typical for third-party SaaS or strict cross-account setups). Leave it out otherwise.

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
