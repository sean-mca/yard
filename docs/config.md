# Root config (yard.yaml)

The root config lives at the top of your project directory and defines the project name, state backend, and provider defaults.

## Basic example

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
