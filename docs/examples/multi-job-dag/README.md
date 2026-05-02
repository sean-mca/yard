# multi-job-dag example

A two-DAG yard project demonstrating Airflow Dataset chaining
— the v1.6 headline feature. The producer DAG fires a Dataset URI
when it succeeds; the consumer DAG triggers automatically on that
same URI.

Copy it, edit a few placeholders, run `yard apply`.

## What this shows

Two DAGs wired by an Airflow Dataset:

- **`raw-pipeline/`** — runs `@hourly`, reads
  `s3://acme-analytics-prod-raw/orders/`, filters to paid status,
  writes to `s3://acme-analytics-prod-clean/orders/`, and **publishes**
  that URI as a Dataset.
- **`clean-pipeline/`** — has no `schedule:`. Its `trigger.dataset.uri`
  matches the URI raw-pipeline publishes, so it triggers automatically
  when raw-pipeline succeeds. It aggregates per-customer order totals
  and writes to `s3://acme-analytics-prod-aggregated/orders_by_customer/`.

The wiring contract is the URI string. It MUST appear identically in
three places:

```yaml
# raw-pipeline/dag.yaml
publishes:
  - s3://acme-analytics-prod-clean/orders/
```

```yaml
# raw-pipeline/raw_to_clean.yaml
sink:
  path: s3://acme-analytics-prod-clean/orders/
```

```yaml
# clean-pipeline/dag.yaml
trigger:
  dataset:
    uri: s3://acme-analytics-prod-clean/orders/
```

If those three strings drift, yard emits a non-fatal warning at apply
time: `WARN: dag '<id>': trigger.dataset "<uri>" has no publisher in
this project (broken link, non-fatal)`. The DAG will still deploy, but
it will never fire.

See [docs/reference/airflow-dag.md](../../reference/airflow-dag.md#airflow-datasets)
for the full Dataset reference and
[docs/reference/configuration.md](../../reference/configuration.md#dagyaml-trigger-block)
for every `trigger:` source variant (`schedule`, `dataset`, `s3`, `sqs`, `api`).

## How to run it

From the repository root, validate the example layout:

```bash
cargo run -p yard -- validate docs/examples/multi-job-dag/
```

Expected output (alphabetically sorted by resolved job_name) and
exit 0:

```
[PASS] clean-pipeline-clean_to_aggregated.yaml
[PASS] raw-pipeline-raw_to_clean.yaml
```

This is what the `validate-examples` CI workflow runs on every PR.

To preview an apply without touching AWS, copy the directory out and
run `yard plan`:

```bash
cp -r docs/examples/multi-job-dag/ my-project/
cd my-project/
yard plan
```

To deploy both DAGs and their jobs:

```bash
yard apply
```

To deploy a single target only (e.g. just the consumer DAG):

```bash
yard apply --target clean_to_aggregated
```

See [docs/reference/cli.md](../../reference/cli.md) for every flag
`validate`, `plan`, and `apply` accept.

## What to change for your project

Three placeholders in this example are fake; everything else is real
yard schema you can keep as-is.

1. **AWS account id `123456789012`** in both
   `raw-pipeline/raw_to_clean.yaml` and
   `clean-pipeline/clean_to_aggregated.yaml` — replace with your account
   id (12 digits, no hyphens).
2. **Bucket prefix `acme-analytics-prod-`** in `yard.yaml` and the
   four bucket references in the two job yamls. Buckets in use:
   - `acme-analytics-prod-glue-scripts` (Glue uploads `.py` here)
   - `acme-analytics-prod-mwaa-dags` (MWAA reads DAG `.py` from here)
   - `acme-analytics-prod-raw` (producer input)
   - `acme-analytics-prod-clean` (producer output / consumer input — the Dataset URI)
   - `acme-analytics-prod-aggregated` (consumer output)
3. **Role ARN `arn:aws:iam::123456789012:role/acme-yard-glue-job`** in
   both job yamls — replace with your Glue execution role.

**If you change the Dataset URI**, change it in all three places
listed in "What this shows" or the wiring breaks silently (warning
only, no error).

See also: [docs/how-to/schedule-a-dag.md](../../how-to/schedule-a-dag.md)
for the `schedule:` vs `trigger:` decision and adjacent recipes, and
[docs/how-to/cross-account-deploy.md](../../how-to/cross-account-deploy.md)
if your producer and consumer DAGs live in different AWS accounts.
