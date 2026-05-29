# Debug codegen output

yard generates PySpark scripts and Airflow DAG `.py` files at apply
time, then deploys them. Most "the deploy didn't do what I expected"
issues come down to inspecting one of four artifacts: the emitted
PySpark, the generated DAG, the `yard plan` diff, or the state file.

All four are inspectable with the CLI alone — no AWS console
required. See
[docs/explanation/why-codegen.md](../explanation/why-codegen.md) for
why these artifacts exist and how the codegen split works.

## Inspect emitted PySpark

Print the generated PySpark for a single job to stdout, without
deploying anything:

```bash
yard show <job_name>
```

`<job_name>` is positional (the resolver-synthesized
`<env>-<folder>-<filename>` job name, NOT the bare yaml file
basename). For example, the `glue-spark-etl` example resolves
its single job to `orders-pipeline-raw_to_clean`:

```bash
yard show orders-pipeline-raw_to_clean
yard show orders-pipeline-raw_to_clean my-project/   # alternate project directory
```

Pipe it through `less` for paging or `wc -l` to spot truncated
output. The script yard prints is byte-identical to what
`yard apply` will upload.

After `yard apply` runs, the same script lives on S3 at:

```
s3://<providers.glue.script_bucket>/<providers.glue.script_prefix><job_name>.py
```

Default `script_prefix` is `yard-scripts/`. Resolve the literal path
by reading `yard.yaml`. Pull the deployed copy with the AWS CLI to
diff against `yard show`:

```bash
aws s3 cp s3://acme-analytics-prod-glue-scripts/yard-scripts/orders-pipeline-raw_to_clean.py /tmp/deployed.py
yard show orders-pipeline-raw_to_clean > /tmp/local.py
diff /tmp/local.py /tmp/deployed.py
```

Empty diff means `yard apply` would upload the same bytes — useful
to confirm a yaml change actually changed something.

See [docs/reference/codegen.md](../reference/codegen.md) for the
full emitted-script structure (helpers like `_yard_conform`,
source/sink/transform render order).

## Inspect a generated DAG .py file

DAGs render to a local file at apply time before being uploaded to
MWAA. After `yard apply` (or `yard apply --dry-run`, which still
writes the local file but skips the S3 upload), look in:

```bash
ls -la .yard/generated/dags/
cat .yard/generated/dags/<dag_id>.py
```

`<dag_id>` is `<project>_<dag_directory_name>` with hyphens
converted to underscores. For the `multi-job-dag` example,
`clean-pipeline/` becomes `acme_multi_job_clean_pipeline.py`.

To dry-run codegen against your current yaml without touching AWS:

```bash
yard apply --dry-run
ls -la .yard/generated/dags/
```

The deployed copy lives on S3 at:

```
s3://<providers.airflow.dags_bucket>/<providers.airflow.dags_prefix><dag_id>.py
```

For event-driven DAGs (any DAG with a `trigger:` block) the file
starts with a comment header listing the Airflow / providers-amazon
version floor. Schedule-only DAGs render WITHOUT this banner. See
[docs/reference/airflow-dag.md "Airflow version matrix"](../reference/airflow-dag.md#airflow-version-matrix).

Cross-account DAGs additionally emit a `# Required Airflow
connections` block listing connection-name -> role-ARN pairs an
operator must create in MWAA before the DAG runs. See
[docs/reference/airflow-dag.md "Cross-account connections"](../reference/airflow-dag.md#cross-account-connections).

## Read yard plan drift output

`yard plan` prints the diff between your yaml and the last applied
state, without making changes:

```bash
yard plan
```

Output is grouped per target. For each Glue or EMR job, plan emits
one of three states:

- `+ create` — yaml describes a job that has no state row yet. apply
  will run `CreateJob`.
- `~ update` — yaml differs from the persisted hash. apply will
  run `UpdateJob` (Glue) or rebuild EMR steps. The diff body lists
  the changed fields.
- `(no change)` — yaml matches state hash. apply is a no-op for
  this target.

Drift surfaces a fourth state: `* drift` — the deployed AWS
resource has diverged from the persisted state hash even though
the yaml hasn't changed. This typically means someone edited the
Glue job in the AWS console.

To plan a single target only:

```bash
yard plan --target orders-pipeline-raw_to_clean
```

See [docs/reference/cli.md "yard plan"](../reference/cli.md#yard-plan)
for every flag. The exit code is 0 even when there ARE changes —
use the diff body, not the exit code, to decide whether to apply.

## Read state hashes

yard persists per-target hashes (and per-DAG, per-resource hashes)
to the configured `state` backend.

For `state.type: local`:

```bash
ls .yard/state/
find .yard/state -type f -name '*.json' | head -20
cat .yard/state/<project>/jobs/<job_name>.json | jq .
```

For `state.type: s3`:

```bash
aws s3 ls s3://<state.bucket>/<state.key>
aws s3 cp s3://<state.bucket>/<state.key>jobs/<job_name>.json - | jq .
```

Each state row carries the canonical hash yard computed at the last
apply, plus the resources it managed (`s3_object` + `glue_job` for a
Glue target, `s3_object` + `dag_python` for an Airflow DAG).

To force a rebuild of a target whose hash you suspect is wrong,
delete its state row and re-apply:

```bash
yard destroy <job_name> --auto-approve   # removes state + AWS resources
yard apply --target <job_name>           # recreates from yaml
```

For DAGs, the same path works — `yard destroy <dag_id>` removes the
persisted DAG state row and the deployed `.py` from S3.

## See also

- [docs/explanation/why-codegen.md](../explanation/why-codegen.md) — why yard generates PySpark + DAG code at apply time instead of templating at runtime.
- [docs/reference/codegen.md](../reference/codegen.md) — full emitted-script structure and helper inventory.
- [docs/reference/airflow-dag.md](../reference/airflow-dag.md) — DAG render contract, local output path, S3 upload semantics.
- [docs/reference/cli.md](../reference/cli.md) — every `yard` subcommand and flag.
