# Deploy across AWS accounts

yard supports a three-way credential split — your state bucket can
live in account A, your Glue/EMR deploy targets in account B, and
your MWAA DAG bucket in account C, all driven from a single CI
runner. Each layer gets its own AssumeRole; yard merges them
per-field so a more-specific layer can override individual fields
without redeclaring the rest.

The per-field cascade behavior was established by commit
[`691a950`](https://github.com/sean-mca/yard/commit/691a950) — before
that, a more-specific `airflow.aws` block fully replaced rather than
merged. After `691a950`, fields cascade individually through
`yard.yaml` -> `account.yaml` -> `region.yaml` -> `dag.yaml` /
`<job>.yaml`. This page documents the post-`691a950` shape.

## The three-account pattern

| Account | Holds | Role |
|---------|-------|------|
| **A** (`111111111111`) | S3 state bucket | `arn:aws:iam::111111111111:role/YardStateAccess` |
| **B** (`222222222222`) | Glue jobs, EMR clusters (deploy targets) | `arn:aws:iam::222222222222:role/YardDeploy` |
| **C** (`333333333333`) | MWAA DAG bucket | `arn:aws:iam::333333333333:role/MwaaDagUploader` |

yard is invoked from a fourth "runner" identity (CI role, dev laptop,
etc.) whose IAM allows `sts:AssumeRole` into all three roles above.
Each role's trust policy must permit the runner.

## yard.yaml: per-field aws cascade

Wire the three roles in `yard.yaml`:

```yaml
project: trifecta

state:
  type: s3
  bucket: account-a-yard-state
  region: us-east-1
  key: trifecta/state/
  aws:
    assume_role: arn:aws:iam::111111111111:role/YardStateAccess

# Root aws: targets Account B (deployment targets). Jobs inherit this
# unless an account.yaml / region.yaml / job.yaml overrides specific
# fields.
aws:
  assume_role: arn:aws:iam::222222222222:role/YardDeploy

providers:
  glue:
    region: us-east-1
    # no aws: here — inherits root (Account B)
  emr:
    region: us-east-1
  airflow:
    region: us-east-1
    dags_bucket: account-c-mwaa-dags
    dags_prefix: dags/
    aws:
      assume_role: arn:aws:iam::333333333333:role/MwaaDagUploader
```

With this manifest:

- `yard plan` / `yard apply` reads and writes state via
  `arn:aws:iam::111111111111:role/YardStateAccess`.
- Glue and EMR deploys use
  `arn:aws:iam::222222222222:role/YardDeploy` (the root `aws:` block).
- DAG file uploads (and destroys) use
  `arn:aws:iam::333333333333:role/MwaaDagUploader` (only the
  `providers.airflow.aws` block declares it).

The per-field merge from `691a950` means an `account.yaml` can
override a single field — say, `session_name` — without redeclaring
`assume_role` and `external_id`. Empty strings (`assume_role: ""`)
fall through to the next less-specific layer at every tier, useful
for intentional-strip overlays in CI.

## CI-side env-var overrides

For ephemeral CI credentials, override the yaml without touching the
file. Two independent scopes:

```bash
# State backend creds — affect ONLY the S3 state bucket (Account A).
export YARD_STATE_AWS_ASSUME_ROLE=arn:aws:iam::111111111111:role/YardStateAccessCI
export YARD_STATE_AWS_SESSION_NAME=yard-ci
export YARD_STATE_AWS_EXTERNAL_ID=xid-ci-rotate-daily

# Provider creds — affect Glue/EMR deploy targets (Account B).
export YARD_AWS_ASSUME_ROLE=arn:aws:iam::222222222222:role/YardDeployCI
export YARD_AWS_SESSION_NAME=yard-ci
```

`YARD_STATE_AWS_*` and `YARD_AWS_*` are independent — setting one
does not affect the other. There are no dedicated `YARD_DAG_AWS_*`
env vars; the `providers.airflow.aws` block is yaml-only today.

Prefer env vars over yaml for `external_id` in particular — yaml is
typically git-tracked, and rotating external IDs should not appear in
commits.

## airflow.aws_conn_id resolution

For Airflow connections (used by emitted sensors and the per-DAG
`default_aws_conn_id`), yard derives the conn id via this precedence
ladder (highest first):

1. **Per-trigger explicit override** — `trigger.s3.aws_conn_id` set
   on a single trigger source. Wins for that one sensor only.
2. **DAG-level cascaded `airflow.aws.aws_conn_id`** — set on
   `dag.yaml` `airflow.aws:` (or inherited via the
   `yard.yaml -> account -> region -> dag` chain). Becomes the DAG's
   `default_aws_conn_id`. The `691a950` per-field merge applies
   here.
3. **Project-root `aws.aws_conn_id`** — set on the top-level `aws:`
   block in `yard.yaml`. Inherited via `cascade_provider_defaults`
   for jobs that don't override.
4. **`derive_aws_conn_id(assume_role)`** — synthesized from
   `aws.assume_role` ARN. For example
   `assume_role: arn:aws:iam::222222222222:role/GlueInvoker`
   yields conn id `yard_222222222222_GlueInvoker`.
5. **Airflow's `aws_default`** — runtime fallback when nothing
   above resolves. yard emits no `aws_conn_id` kwarg in this case.

Empty strings (`aws_conn_id: ""`) are treated as unset at every
layer, falling through to the next tier.

Generated DAGs that need cross-account connections include a
`# Required Airflow connections` comment header listing the connection
name -> role ARN pairs, which an operator must create in MWAA before
the DAG runs. yard does NOT create the MWAA connections itself. See
[airflow-dag.md "Cross-account connections"](../reference/airflow-dag.md#cross-account-connections).

## See also

- Commit [`691a950`](https://github.com/sean-mca/yard/commit/691a950) — the per-field-merge fix this page is built around.
- [configuration.md "Cross-account state backend credentials"](../reference/configuration.md#cross-account-state-backend-credentials) — `state.aws:` resolution chain.
- [configuration.md "yard CLI environment variables"](../reference/configuration.md#yard-cli-environment-variables) — full `YARD_*` env reference.
- [airflow-dag.md "Cross-account connections"](../reference/airflow-dag.md#cross-account-connections) — emitted `# Required Airflow connections` header and operator-side MWAA setup.
- [airflow-dag.md "aws_conn_id resolution"](../reference/airflow-dag.md#aws_conn_id-resolution) — full precedence ladder.
