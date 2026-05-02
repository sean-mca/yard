# Schedule a DAG

Every yard DAG decides when to run via either `schedule:` (cron) or
`trigger:` (event-driven). They are mutually exclusive — declaring
both is rejected at validation. This page walks through one worked
example for each path.

| Use this | When |
|---|---|
| `schedule:` | The DAG should run on a fixed cadence (`@hourly`, `@daily`, a cron string). |
| `trigger:` | The DAG should run when an upstream event happens — an Airflow Dataset fires, an S3 object lands, an SQS message arrives, or someone calls the Airflow API. |

Internally yard renders both into the same `schedule=` keyword on the
Airflow `DAG(...)` constructor — `"@daily"` for `schedule:`, a
`Dataset(...)` list / sensor chain for `trigger:`. See
[docs/reference/airflow-dag.md](../reference/airflow-dag.md) for the
full render contract.

## schedule:

A schedule-only DAG runs on a cron cadence. It has no `trigger:` block.

Create `dag.yaml` next to your job files:

```yaml
# aws/dev/us-east-1/orders-pipeline/dag.yaml
schedule: "@daily"
```

Validate the project:

```bash
yard validate
```

The DAG renders as `schedule="@daily"` and inherits Airflow's default
`max_active_runs=16`. Standard Airflow backfill works
(`airflow dags backfill <dag_id>` replays missed cron runs).

Cron strings (`"0 */4 * * *"`) and presets (`"@hourly"`, `"@daily"`,
`"@weekly"`) are both accepted — anything Airflow accepts as
`schedule=`.

The full schedule-only worked example (project layout, `yard.yaml`,
job yaml, resulting Python) lives in
[docs/examples/glue-spark-etl/](../examples/glue-spark-etl/).

## trigger:

A `trigger:` block makes the DAG event-driven. It is mutually exclusive
with `schedule:` — declaring both is rejected at validation.

The five `trigger:` source variants are documented in
[configuration.md `dag.yaml: trigger:`](../reference/configuration.md#dagyaml-trigger-block).
This recipe covers `trigger.dataset:` — chain one DAG off another via
an Airflow Dataset.

Set up two DAGs:

```yaml
# aws/dev/us-east-1/raw-pipeline/dag.yaml
schedule: "@hourly"

publishes:
  - s3://acme-analytics-prod-clean/orders/
```

```yaml
# aws/dev/us-east-1/clean-pipeline/dag.yaml
trigger:
  dataset:
    uri: s3://acme-analytics-prod-clean/orders/
```

The producer DAG (`raw-pipeline/`) runs hourly and lists the URI in
`publishes:`. yard emits a synthetic `_yard_publish` task that fires
the Dataset when every user task succeeds. The consumer DAG
(`clean-pipeline/`) has no `schedule:` — its `trigger.dataset.uri`
matches the producer's published URI, so Airflow fires it
automatically.

Validate:

```bash
yard validate
```

The wiring contract is the URI string. If the producer's `publishes:`
URI and the consumer's `trigger.dataset.uri` drift, yard emits a
non-fatal warning at apply time:

```
WARN: dag '<dag_id>': trigger.dataset "<uri>" has no publisher in this project (broken link, non-fatal)
```

The DAG still deploys but never fires — the consumer is waiting for a
Dataset that nothing publishes.

Event-driven DAGs default to `max_active_runs=1`. Override with
`max_active_runs: <N>` on `dag.yaml` if you need concurrent runs.
Backfill semantics differ per source — Datasets cannot be replayed
from event history; use a sibling `trigger.api:` DAG for replays. See
[airflow-dag.md "Backfill semantics per trigger source"](../reference/airflow-dag.md#backfill-semantics-per-trigger-source).

The full multi-DAG worked example (both projects, both jobs, README
explaining the wiring contract) lives in
[docs/examples/multi-job-dag/](../examples/multi-job-dag/).

## See also

- [configuration.md `dag.yaml: trigger:` block](../reference/configuration.md#dagyaml-trigger-block) — the five `trigger:` source variants (`schedule`, `dataset`, `s3`, `sqs`, `api`) and their per-source knobs.
- [airflow-dag.md "Airflow Datasets"](../reference/airflow-dag.md#airflow-datasets) — full Dataset reference: producing, consuming, composites (`any:`/`all:`), backfill caveats.
- [docs/examples/glue-spark-etl/](../examples/glue-spark-etl/) — schedule-only example.
- [docs/examples/multi-job-dag/](../examples/multi-job-dag/) — `trigger.dataset:` example.
- [docs/reference/cli.md](../reference/cli.md) — `yard validate`, `yard plan`, `yard apply` flag reference.
