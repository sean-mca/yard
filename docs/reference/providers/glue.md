# Glue provider

This page documents every knob accepted under `providers.glue:` in `yard.yaml`, the AWS resources the Glue provider touches when `yard apply` runs, and provider-specific gotchas.

Consumed by `GlueProvider::new` in `yard-core/src/providers/glue.rs`.

- [Knob reference](#knob-reference)
- [AWS resources and IAM](#aws-resources-and-iam)
- [Minimal example](#minimal-example)
- [Limitations / Gotchas](#limitations--gotchas)

## Knob reference

Every field accepted under `providers.glue:` in `yard.yaml`. The 13 rows below cover all 12 fields of the `GlueRawConfig` struct plus the pre-extracted `script_bucket` field that is read directly from the raw YAML before deserialization.

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `script_bucket` | Yes | — | S3 bucket where generated PySpark scripts are uploaded. Read from raw YAML before serde deserialization; missing value produces the error `providers.glue.script_bucket is required`. |
| `region` | No | `us-east-1` | AWS region for the Glue and S3 clients. |
| `script_prefix` | No | `yard-scripts/` | Key prefix under `script_bucket` where the generated `<job>.py` file is uploaded. The trailing slash is significant — when the prefix ends with `/`, yard appends `<job>.py` directly; otherwise yard inserts a `/` before the job filename. |
| `glue_version` | No | `4.0` | Glue runtime version. Validation accepts `"3.0"`, `"4.0"`, or `"5.0"`. (The validation error message currently lists only `3.0, 4.0` — see Limitations.) |
| `worker_type` | No | `G.1X` | Glue worker instance type. Validation accepts `G.025X`, `G.1X`, `G.2X`, `G.4X`, `G.8X`, and `Z.2X`. |
| `number_of_workers` | No | `2` | Number of Glue workers. Must be `>= 1`. |
| `timeout` | No | (unset) | Job timeout in minutes. Must be `>= 1` if set. When unset, yard does not pass a timeout to AWS Glue. |
| `max_retries` | No | (unset) | Maximum automatic retries on job failure. Must be `>= 0` if set. |
| `max_concurrent_runs` | No | (unset) | Maximum concurrent executions of this job. When unset, yard does not pass `execution_property` to AWS Glue and Glue defaults to **1** concurrent run. |
| `bookmark` | No | (unset) | Controls Glue job bookmarks. Validation accepts only the literals `"enabled"` or `"disabled"`, but the runtime predicate is asymmetric: `"enabled"` or `"true"` enables bookmarks (`--job-bookmark-enable`); **anything else disables them** (`--job-bookmark-disable`). See Limitations. |
| `connections` | No | `[]` | Array of Glue connection names attached to the job via `ConnectionsList`. |
| `default_arguments` | No | `{}` | Map of extra Glue job arguments. Keys must be the Glue-required `--key` shape (e.g. `--enable-metrics`, `--TempDir`). yard auto-injects `--datalake-formats: iceberg` unless that key is already present. |
| `_aws` | No | (unset) | Per-provider AWS credential override (e.g. `assume_role`, `session_name`, `external_id`). The wire YAML key is the **literal underscore-prefixed `_aws:`**, not `aws:` — distinct from the manifest-level `aws:` key. Plumbed to `aws_config()` for cross-account `AssumeRole`. |

## AWS resources and IAM

When `yard apply` runs a Glue job, the provider issues the following AWS API calls. The IAM column lists the action the caller's identity (or assumed role, when `_aws.assume_role` is set) must be allowed.

| AWS API call | Operation | IAM action |
|--------------|-----------|------------|
| `S3Client::put_object` | Upload generated `.py` script to `s3://{script_bucket}/{script_prefix}{job_name}.py` (`Content-Type: text/x-python`) | `s3:PutObject` on `arn:aws:s3:::{script_bucket}/{script_prefix}*` |
| `GlueClient::get_job` | Existence check for the Glue job (used by `verify_resources`) | `glue:GetJob` |
| `GlueClient::update_job` | Update Glue job definition (tried first) | `glue:UpdateJob` |
| `GlueClient::create_job` | Create Glue job (fallback when update returns `EntityNotFoundException`) | `glue:CreateJob` |
| `GlueClient::delete_job` | Destroy path: remove the Glue job | `glue:DeleteJob` |
| `S3Client::head_object` | Verify the uploaded `.py` script still exists | `s3:GetObject` (or `s3:ListBucket`) on the script key |
| `S3Client::delete_object` | Destroy path: remove the uploaded `.py` script | `s3:DeleteObject` on the script key |
| (transitive — set on `create_job`/`update_job`) | Allow Glue to assume the per-job execution role declared in `<job>.yaml` | `iam:PassRole` on the per-job `role:` ARN |
| `aws_config::sts::AssumeRoleProvider` | Cross-account caller credentials (only when `_aws.assume_role` is set) | `sts:AssumeRole` on the configured role |

### Resources tracked in state

After `deploy()` succeeds the Glue provider records two `Resource` entries in yard state, one per managed object:

- `Resource { type: "s3_object", id: "s3://{bucket}/{key}", provider: "glue" }` — the uploaded PySpark script.
- `Resource { type: "glue_job", id: "{job_name}", provider: "glue" }` — the Glue Job created or updated.

## Minimal example

A complete Glue setup needs two YAML files: project-level provider defaults in `yard.yaml`, and a per-job definition under `<account>/<region>/<job>.yaml`.

**`yard.yaml` snippet:**

```yaml
project: my-project
state:
  type: local
  path: .yard/state
providers:
  glue:
    script_bucket: example-yard-scripts
    glue_version: "4.0"
    worker_type: G.1X
    number_of_workers: 2
```

**`<account>/<region>/<job>.yaml` snippet:**

```yaml
type: glue
role: arn:aws:iam::123456789012:role/example-glue-job-role
sources:
  - name: orders
    source_type: s3
    location: s3://example-input/orders/
    format: parquet
transforms:
  - filter: status = 'paid'
sink:
  sink_type: iceberg
  catalog: example_catalog
  table: orders_paid
  location: s3://example-output/orders_paid/
```

See [cli.md](../cli.md) for `yard apply` flags and command-line semantics.

## Limitations / Gotchas

- **Iceberg auto-injection.** yard unconditionally adds `--datalake-formats: iceberg` to `default_arguments` unless the user has explicitly set that key (`glue.rs:114-117`). This is a global side-effect on every Glue job yard generates.
- **`bookmark` truthiness is asymmetric.** Validation accepts only the literals `"enabled"` and `"disabled"` (`glue.rs:377`), but at runtime the predicate is `matches!(bookmark.as_str(), "enabled" | "true")` (`glue.rs:121`). So `"disabled"` and any other non-`"enabled"`/non-`"true"` value all map to `--job-bookmark-disable`. `"disabled"` is not a special value — it is just one of the many strings that disable bookmarks.
- **`max_concurrent_runs` unset → AWS default of 1.** When the field is omitted, yard does not pass `execution_property` to the Glue API call, so AWS Glue applies its own default of 1 concurrent run.
- **`default_arguments` keys must be `--key` shape.** Keys like `--enable-metrics` or `--TempDir` are required; yard does not validate or transform key names.
- **Job role is per-job, not per-provider.** The Glue execution role is read from the `<job>.yaml` `role:` field, NOT from `providers.glue` (`glue.rs:144-146`). Missing → `Job "<job_name>" requires a "role" (Glue execution role)`.
- **`_aws` vs manifest-level `aws`.** The per-provider AWS credential override key is the literal underscore-prefixed `_aws:` (`glue.rs:56-57`). This is distinct from the manifest-level `aws:` key (`yard-structs/src/config.rs:149-150`). Writing `aws:` under `providers.glue:` is silently ignored.
- **No `#[serde(deny_unknown_fields)]` on `GlueRawConfig`.** Typos like `glue_versoin` are silently accepted and produce no error (`glue.rs:524-546`). Verify field names match this page.
