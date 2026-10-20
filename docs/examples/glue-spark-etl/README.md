# glue-spark-etl example

A complete, validated yard project showing the source -> transform -> sink
pattern end-to-end on AWS Glue.

Copy it, edit a few placeholders, run `yard apply`.

## What this shows

- Project root manifest (`yard.yaml`) with `providers.glue` defaults and the
  `plugins.glue` pin.
- One Glue job, defined in `aws/dev/us-east-1/orders-pipeline/raw_to_clean.yaml`,
  reading parquet from S3, applying three transforms (`sql`, `filter`,
  `drop_columns`), and writing parquet back. yard's resolver synthesizes the
  job name as `<env>-<folder>-<filename>` (here, `orders-pipeline-raw_to_clean`),
  which is what `[PASS]` lines and `--target` flags reference.
- The `plugins.glue` block in `yard.yaml`, which tells yard which Glue plugin
  to run: the plugin's repository (`source`) and the release to fetch for the
  current platform (`version`). Every glue job inherits it, and a job can
  override it with its own `plugin_version` / `plugin_source`.

Glue-specific knobs are documented in the `yard-plugin-glue` repository. See
[docs/reference/configuration.md](../../reference/configuration.md) for
the full source/transform/sink reference, and
[docs/reference/migrations/v2.0.md](../../reference/migrations/v2.0.md) if you
are coming from v1.x.

## How to run it

With yard installed, validate the example layout from the repository root:

```bash
yard validate docs/examples/glue-spark-etl/
```

Expected output: `[PASS] orders-pipeline-raw_to_clean.yaml` and exit 0.
The `validate-examples` CI workflow runs the same check on every PR
(via `cargo run -p yard -- validate …` against the checked-out source).

To preview a real apply without touching AWS, copy the directory out and
run `yard plan` from the copy:

```bash
cp -r docs/examples/glue-spark-etl/ my-project/
cd my-project/
yard plan
```

To actually deploy the job (requires the AWS resources listed in the next
section, plus a reachable `yard-plugin-glue` release at `plugins.glue.source`), run:

```bash
yard apply
```

See [docs/reference/cli.md](../../reference/cli.md) for every flag
`validate`, `plan`, and `apply` accept.

## What to change for your project

The placeholders below are fake; everything else is real yard schema you can
keep as-is.

1. **AWS account id `123456789012`** in `aws/dev/us-east-1/orders-pipeline/raw_to_clean.yaml`
   — replace with your account id (12 digits, no hyphens).
2. **Bucket prefix `acme-analytics-prod-`** in `yard.yaml` and
   `raw_to_clean.yaml` — four buckets are referenced:
   - `acme-analytics-prod-glue-scripts` (Glue uploads the generated `.py` here)
   - `acme-analytics-prod-raw` and `acme-analytics-prod-clean` (the job's
     input and output buckets)
3. **Role ARN `arn:aws:iam::123456789012:role/acme-yard-glue-job`** in
   `raw_to_clean.yaml` — replace with the IAM role Glue assumes when it
   runs the job. Required permissions are documented in the
   `yard-plugin-glue` repository.
4. **Region `us-east-1`** appears in two places that must change
   together: `providers.glue.region` in `yard.yaml`, plus the
   `aws/dev/us-east-1/` directory name (the third path component is the
   region per yard's hierarchical context convention).
5. **`plugins.glue.source`** in `yard.yaml` — point it at your org's
   `yard-plugin-glue` repository (or a `file://` path to a local build).

Optional knobs to tune in `yard.yaml` -> `providers.glue`:

- `glue_version` — `"3.0"`, `"4.0"` (default), or `"5.0"`.
- `worker_type` — `G.025X`, `G.1X` (default), `G.2X`, `G.4X`, `G.8X`, `Z.2X`.
- `number_of_workers` — any integer `>= 1`.

See also: [docs/how-to/cross-account-deploy.md](../../how-to/cross-account-deploy.md)
if your state bucket and deployment targets live in different AWS accounts, and
[docs/how-to/build-a-plugin.md](../../how-to/build-a-plugin.md) if you want to
write your own provider.
