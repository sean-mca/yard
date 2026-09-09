# Deploy across AWS accounts

yard supports a two-way credential split — your state bucket can live
in account A and your deploy targets in account B, all driven from a
single CI runner. Each layer gets its own AssumeRole; yard merges them
per-field so a more-specific layer can override individual fields
without redeclaring the rest.

Fields cascade individually through `yard.yaml` -> `account.yaml` ->
`region.yaml` -> `<job>.yaml`; a more-specific layer overrides single
fields rather than replacing the whole block.

> **v2.0 note.** Provider credentials are consumed by the provider
> *plugin*, not by yard core. The `aws:` cascade below still resolves in
> core and is handed to the plugin; how a given plugin uses it is
> documented by that plugin. Airflow/MWAA connection derivation was
> removed from core in v2.0 along with DAG generation.

## The three-account pattern

| Account | Holds | Role |
|---------|-------|------|
| **A** (`111111111111`) | S3 state bucket | `arn:aws:iam::111111111111:role/YardStateAccess` |
| **B** (`222222222222`) | Glue jobs, EMR clusters (deploy targets) | `arn:aws:iam::222222222222:role/YardDeploy` |

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
```

Each key under `providers:` is a plugin type -- it matches the `type:`
field of the jobs that use it.

With this manifest:

- `yard plan` / `yard apply` reads and writes state via
  `arn:aws:iam::111111111111:role/YardStateAccess`.
- Glue and EMR deploys use
  `arn:aws:iam::222222222222:role/YardDeploy` (the root `aws:` block),
  resolved in core and passed to the plugin.

The per-field merge means an `account.yaml` can
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
does not affect the other.

Prefer env vars over yaml for `external_id` in particular — yaml is
typically git-tracked, and rotating external IDs should not appear in
commits.

## See also

- Commit [`691a950`](https://github.com/sean-mca/yard/commit/691a950) — the per-field-merge fix this page is built around.
- [configuration.md "Cross-account state backend credentials"](../reference/configuration.md#cross-account-state-backend-credentials) — `state.aws:` resolution chain.
- [configuration.md "yard CLI environment variables"](../reference/configuration.md#yard-cli-environment-variables) — full `YARD_*` env reference.
