# EMR provider

This page documents every knob accepted under `providers.emr:` in `yard.yaml`, the AWS resources the EMR provider touches when `yard apply` runs, and provider-specific gotchas. **yard's EMR provider targets classic EMR (the `aws_sdk_emr` crate) — NOT EMR Serverless. It submits Spark steps to an existing cluster you own; yard does not create or destroy EMR clusters.**

Consumed by `EmrProvider::new` in `yard-core/src/providers/emr.rs`.

- [Knob reference](#knob-reference)
- [AWS resources and IAM](#aws-resources-and-iam)
- [Minimal example](#minimal-example)
- [Limitations / Gotchas](#limitations--gotchas)

## Knob reference

Every field accepted under `providers.emr:` in `yard.yaml`. The 7 rows below cover all 5 fields of the `EmrRawConfig` struct (`emr.rs:27-39`) plus the 2 pre-extracted fields (`script_bucket` and `cluster_id`) that are read directly from the raw YAML before serde deserialization.

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `script_bucket` | Yes | — | S3 bucket where generated PySpark scripts are uploaded. Read from raw YAML before serde deserialization (`emr.rs:53`); missing value produces the error `providers.emr.script_bucket is required`. |
| `cluster_id` | Yes | — | ID of an existing EMR cluster (e.g. `j-XXXXXXXX`). yard does NOT create or terminate clusters; if missing, `yard apply` errors with `providers.emr.cluster_id is required` (`emr.rs:59`). |
| `region` | No | `us-east-1` | AWS region for the EMR and S3 clients. |
| `script_prefix` | No | `yard-scripts/` | Key prefix under `script_bucket` where the generated `<job>.py` file is uploaded. The trailing slash is significant — when the prefix ends with `/`, yard appends `<job>.py` directly; otherwise yard inserts a `/` before the job filename. |
| `deploy_mode` | No | `cluster` | Passed to `spark-submit --deploy-mode`. Validation accepts `cluster` or `client` only (`emr.rs:252-258`). |
| `action_on_failure` | No | `CONTINUE` | EMR step failure action. Validation accepts `CONTINUE`, `CANCEL_AND_WAIT`, or `TERMINATE_CLUSTER` (`emr.rs:235-236`). The runtime `parse::<ActionOnFailure>()` falls back to `Continue` on any parse failure (`emr.rs:93-95`), so a validation-bypassed value silently degrades to `CONTINUE`. See Limitations. |
| `_aws` | No | (unset) | Per-provider AWS credential override (e.g. `assume_role`, `session_name`, `external_id`). The wire YAML key is the **literal underscore-prefixed `_aws:`**, not `aws:` — distinct from the manifest-level `aws:` key (`emr.rs:37-38`). Plumbed to `aws_config()` for cross-account `AssumeRole`. |

## AWS resources and IAM

When `yard apply` runs an EMR job, the provider issues the following AWS API calls. The IAM column lists the action the caller's identity (or assumed role, when `_aws.assume_role` is set) must be allowed.

| AWS API call | Operation | IAM action |
|--------------|-----------|------------|
| `S3Client::put_object` | Upload generated `.py` script to `s3://{script_bucket}/{script_prefix}{job_name}.py` | `s3:PutObject` on `arn:aws:s3:::{script_bucket}/{script_prefix}*` |
| `EmrClient::add_job_flow_steps` | Submit a `spark-submit` step to the existing cluster (`emr.rs:108-114`) | `elasticmapreduce:AddJobFlowSteps` on the cluster ARN |
| `EmrClient::cancel_steps` | Best-effort step cancel during destroy (errors swallowed — `emr.rs:131-139`) | `elasticmapreduce:CancelSteps` on the cluster ARN |
| `S3Client::head_object` | Verify the uploaded `.py` script still exists | `s3:GetObject` (or `s3:ListBucket`) on the script key |
| `S3Client::delete_object` | Destroy path: remove the uploaded `.py` script | `s3:DeleteObject` on the script key |
| (transitive — submitted by EMR step) | The cluster's instance profile role must allow whatever the rendered PySpark does (S3 reads/writes, Glue catalog access for Iceberg, etc.) — yard does NOT manage this role | `iam:PassRole` (transitive on the cluster's instance profile / job-flow role) |
| `aws_config::sts::AssumeRoleProvider` | Cross-account caller credentials (only when `_aws.assume_role` is set) | `sts:AssumeRole` on the configured role |

### Resources tracked in state

After `deploy()` succeeds the EMR provider records two `Resource` entries in yard state, one per managed object (`emr.rs:160-171`):

- `Resource { type: "s3_object", id: "s3://{bucket}/{key}", provider: "emr" }` — the uploaded PySpark script.
- `Resource { type: "emr_step", id: "{step_id_from_AddJobFlowSteps}", provider: "emr" }` — the EMR step ID returned from the `AddJobFlowSteps` call.

## Minimal example

A complete EMR setup needs two YAML files: project-level provider defaults in `yard.yaml`, and a per-job definition under `<account>/<region>/<job>.yaml`.

**`yard.yaml` snippet:**

```yaml
project: my-project
state:
  type: local
  path: .yard/state
providers:
  emr:
    script_bucket: example-yard-scripts
    cluster_id: j-EXAMPLE12345
    deploy_mode: cluster
    action_on_failure: CANCEL_AND_WAIT
```

**`<account>/<region>/<job>.yaml` snippet:**

```yaml
type: emr
sources:
  - name: orders
    source_type: s3
    location: s3://example-input/orders/
    format: parquet
transforms:
  - filter: status = 'paid'
sink:
  sink_type: s3
  format: parquet
  path: s3://example-output/orders_paid/
  mode: overwrite
```

The EMR provider is for `yard apply` only — it submits steps to an existing cluster. **yard's Airflow DAG codegen does NOT support EMR jobs**: any DAG that contains an `emr` job errors out at codegen time (see [airflow-dag.md "Limitations and planned work"](../airflow-dag.md#limitations-and-planned-work)). Use the Glue provider for jobs that need to participate in a generated Airflow DAG.

See [cli.md](../cli.md) for `yard apply` flags and command-line semantics.

## Limitations / Gotchas

- **Classic EMR only — NOT EMR Serverless.** yard's EMR provider uses the `aws_sdk_emr` crate (Cargo.lock confirms `aws-sdk-emr`; zero `aws-sdk-emrserverless` references exist anywhere in the workspace). EMR Serverless is a different AWS service with a different SDK and is not supported.
- **Existing cluster required.** yard does NOT create or destroy EMR clusters. `CreateCluster` and `TerminateClusters` are out of scope — bring your own long-lived cluster and set its ID in `providers.emr.cluster_id`. `yard destroy` removes the uploaded script and best-effort cancels pending steps; it does not touch the cluster.
- **Step-based, not job-based.** Every `yard apply` call appends a NEW step to the cluster via `AddJobFlowSteps` (`emr.rs:108-114`). Steps are append-only on EMR — there is no equivalent of `update_job` for steps. `yard destroy` calls `cancel_steps` (best-effort, errors swallowed at `emr.rs:131-139`) but cannot remove a completed step from EMR's step history.
- **No EMR Airflow operator support.** yard's Airflow codegen explicitly only supports `bash` and `glue` job types — `airflow-dag.md` "Limitations and planned work" (line 859+) states that EMR (and any future provider) **errors out at codegen time** for DAG generation. The EMR provider is therefore a `yard apply` / `yard destroy` surface only; if your job needs to participate in a yard-generated DAG, use the Glue provider instead.
- **`action_on_failure` defaults to `CONTINUE`.** Step failures are silent by default — downstream EMR steps continue running even when an earlier yard-submitted step fails. Operators who want a failed step to halt the cluster's step queue must set `action_on_failure: CANCEL_AND_WAIT` or `TERMINATE_CLUSTER` explicitly. Additionally, the runtime `.parse::<ActionOnFailure>()` call falls back to `Continue` on any parse failure (`emr.rs:93-95`); a validation-bypassed value (e.g. injected by tooling that skips `yard validate`) silently degrades to `CONTINUE` rather than erroring.
- **`emr_step` resources are ephemeral in `verify_resources`.** The verify path returns `exists: true` unconditionally for `emr_step` resources (`emr.rs:218` — `// EMR steps are ephemeral — skip verification`). Drift detection on EMR is effectively scripted-only — the S3 script object IS verified via `head_object`, but the step itself is not checked against the EMR API.
- **`_aws` vs manifest-level `aws`.** The per-provider AWS credential override key is the literal underscore-prefixed `_aws:` (`emr.rs:37-38`). This is distinct from the manifest-level `aws:` key (`yard-structs/src/config.rs:149-150`). Writing `aws:` under `providers.emr:` is silently ignored — same quirk as the Glue provider.
- **No `#[serde(deny_unknown_fields)]` on `EmrRawConfig`.** Typos like `cluster_idd` or `deploy_modee` are silently accepted and produce no error (`emr.rs:307-324` test-locks this behavior). Verify field names match this page.
