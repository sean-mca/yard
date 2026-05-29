# Why codegen?

This page captures the rationale behind yard's codegen pipeline — why
yard emits a script instead of running anything locally, where codegen
sits in the `plan` / `apply` lifecycle, and why scaffolding lives in
Tera templates while the dataframe body is built in Rust. For the
*rules* of how source / sink / transform code is rendered (templates,
helpers, Glue vs. EMR specifics, escape hatches), see
[reference/codegen.md](../reference/codegen.md).

## What codegen does

Codegen takes a parsed `JobDefinition` (from `yard-structs/src/config.rs`)
plus provider context (merged from `providers.<type>:` in `yard.yaml` and
the per-job `<job_type>:` block) and produces a single Python/PySpark
script as a `String`. The string is then either:

- written to stdout by `yard show <job>`; or
- uploaded to S3 by the provider's `deploy` method (as `text/x-python`)
  and pointed at by the Glue job definition or EMR step that yard
  creates/updates.

No code runs locally — yard only emits the script, uploads it, and wires
up the cloud resource that will execute it. This keeps the CLI itself
free of a Spark/PySpark runtime dependency, makes the same generated
script reproducible across local dev / CI / production, and means a
failed generation is a parse-time error rather than a partially-applied
deploy.

## Where it fits in plan/apply

From `yard-core/src/providers/glue.rs` and `yard-core/src/providers/emr.rs`:

1. `generate_python_script(job_name, job_def)` produces the script string.
2. `S3ScriptOps::upload_script(job_name, artifact)` uploads it to
   `s3://{script_bucket}/{script_prefix}{job_name}.py`.
3. The Glue provider calls `update_job` (falling back to `create_job` if
   the Glue job doesn't exist yet) with `script_location` set to that S3
   URI. The EMR provider submits a `spark-submit` step whose last arg is
   the S3 URI.
4. `deploy` returns a `Vec<Resource>` (one `s3_object`, one `glue_job` or
   `emr_step`) that `yard-core` records in state for drift detection.

The same `generate_python_script` call also runs during `plan`: the
generated script's content participates in the BLAKE3 hash that
`calculate_diff` compares against the previously-deployed
`config_hash`, so any change to the rendered output — whether from a
YAML edit, a yard upgrade that changes a template, or a new helper
inlined into `imports_block` — surfaces as a deterministic diff before
anything is uploaded.

## Scaffolding in Tera, body in Rust

yard uses the [Tera](https://keats.github.io/tera/) template engine, but
only for the outer scaffolding of each generated script — `getResolvedOptions`,
`SparkSession` / `GlueContext` setup, `Job.init` / `job.commit`, the
`finally: spark.stop()` in EMR, optional Iceberg catalog configs. The
templates are compiled into the binary at build time with
`include_str!("../templates/<name>")`, so there is nothing to deploy
alongside the CLI.

Both the `glue.py.tera` and `emr.py.tera` templates receive a **flat**
render context with pre-rendered string fragments (`imports_block`,
`body`, `iceberg_warehouse`, `job_name`, `job_type`) rather than nested
structures. The split — scaffolding in Tera, pipeline body generated in
Rust — keeps the Tera templates short and predictable, and lets the Rust
renderers emit arbitrary multiline strings without fighting Tera's
whitespace rules.

There are no per-source, per-sink, or per-transform loops in the templates
themselves: everything under `def run():` is built in Rust (in
`yard-core/src/codegen/source.rs`, `transform.rs`, `sink.rs`) and inlined
as a single string. This pushes all the dispatch logic — "is this an
`s3_csv` source or a `jdbc` source?", "does this transform produce a
temp view for downstream `sql`?", "does this Iceberg sink need
`_yard_conform`?" — into typed Rust code with `match` arms over the
`Source` / `Transform` / `Sink` enums in `yard-structs`, where the
compiler enforces that every variant is handled. The trade-off is that
adding a new source/sink/transform variant is a Rust edit, not a
template edit; in exchange, codegen failures are caught at `cargo
build` rather than at script-render time.

## See also

- [reference/codegen.md](../reference/codegen.md) — the rules: template
  files, render context, per-source/transform/sink dispatch, provider
  specifics, escape hatches, end-to-end example.
- [explanation/architecture.md](architecture.md) — where codegen sits
  in the wider yard / yard-core / yard-server architecture.
