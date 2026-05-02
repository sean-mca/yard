# glue-spark-etl example

A complete, validated yard project showing the source -> transform -> sink
pattern end-to-end on AWS Glue.

Copy it, edit a few placeholders, run `yard apply`.

## What this shows

- Project root manifest (`yard.yaml`) with `providers.glue` defaults and
  `providers.airflow` so the job can be wrapped in a DAG.
- One Glue job (`raw_to_clean`) reading parquet from S3, applying three
  transforms (`sql`, `filter`, `drop_columns`), and writing parquet back.
- One scheduled DAG (`@daily`) that wraps the job. Schedule-only — the
  [multi-job-dag](../multi-job-dag/) example covers `trigger:`.

See [docs/reference/providers/glue.md](../../reference/providers/glue.md)
for every Glue knob, and
[docs/reference/configuration.md](../../reference/configuration.md) for
the full source/transform/sink reference.

## How to run it

From the repository root, validate the example layout:

```bash
cargo run -p yard -- validate docs/examples/glue-spark-etl/
```

Expected output: `[PASS] orders-pipeline-raw_to_clean.yaml` and exit 0.
This is what the `validate-examples` CI workflow runs on every PR.

To preview a real apply without touching AWS, copy the directory out and
run `yard plan` from the copy:

```bash
cp -r docs/examples/glue-spark-etl/ my-project/
cd my-project/
yard plan
```

To actually deploy the job and the DAG (requires the AWS resources listed
in the next section), run:

```bash
yard apply
```

See [docs/reference/cli.md](../../reference/cli.md) for every flag
`validate`, `plan`, and `apply` accept.

## What to change for your project

Three placeholders in this example are fake; everything else is real
yard schema you can keep as-is.

1. **AWS account id `123456789012`** in `aws/dev/us-east-1/orders-pipeline/raw_to_clean.yaml`
   — replace with your account id (12 digits, no hyphens).
2. **Bucket prefix `acme-analytics-prod-`** in `yard.yaml` and
   `raw_to_clean.yaml` — four buckets are referenced:
   - `acme-analytics-prod-glue-scripts` (Glue uploads the generated `.py` here)
   - `acme-analytics-prod-mwaa-dags` (MWAA reads the DAG `.py` from here)
   - `acme-analytics-prod-raw` and `acme-analytics-prod-clean` (the job's
     input and output buckets)
3. **Role ARN `arn:aws:iam::123456789012:role/acme-yard-glue-job`** in
   `raw_to_clean.yaml` — replace with the IAM role Glue assumes when it
   runs the job. Required permissions are documented in
   [providers/glue.md](../../reference/providers/glue.md#aws-resources-and-iam).

Optional knobs to tune in `yard.yaml` -> `providers.glue`:

- `glue_version` — `"3.0"`, `"4.0"` (default), or `"5.0"`.
- `worker_type` — `G.025X`, `G.1X` (default), `G.2X`, `G.4X`, `G.8X`, `Z.2X`.
- `number_of_workers` — any integer `>= 1`.

See also: [docs/how-to/schedule-a-dag.md](../../how-to/schedule-a-dag.md)
for the `schedule:` vs `trigger:` decision, and
[docs/how-to/cross-account-deploy.md](../../how-to/cross-account-deploy.md)
if your state, deployment, and DAG buckets live in different AWS accounts.
