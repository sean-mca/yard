# yard documentation

← [Back to repo README](../README.md)

15 docs across 7 categories. This index links to every page in `docs/`.

> Diátaxis-lite layout: tutorials (one quickstart) / how-to (task recipes) / reference (schema + API) / explanation (architecture + rationale) / examples (copy-paste projects) / server (yard-server) / contributing (developer setup).

## Quickstart

- [quickstart.md](quickstart.md) — Zero-to-deployed walkthrough: prerequisites, building yard from source, scaffolding a project, authoring one job, and running `yard plan` / `yard apply`.

## How-to

- [how-to/deploy.md](how-to/deploy.md) — Operator guide for deploying yard-server: topology, environment variables, DynamoDB table, GitHub webhook setup, drift-poll loop tuning.

*Additional how-to recipes (`schedule-a-dag`, `cross-account-deploy`, `debug-codegen-output`, `upgrade-yard`) coming in Phase 35.*

## Reference

- [reference/cli.md](reference/cli.md) — Every `yard` subcommand (`init`, `plan`, `apply`, `show`, `validate`, `destroy`, `force-unlock`, `list targets`) and every flag, with synopsis + flag table + 1-2 examples per command. CI-guarded by `.github/workflows/verify-cli-docs.yml` against drift.
- [reference/configuration.md](reference/configuration.md) — Full configuration surface: `yard.yaml` / `<account>.yaml` / `<region>.yaml` / `<job>.yaml` / `dag.yaml` field reference, CLI environment variables, yard-server env vars, and the runtime Settings page.
- [reference/codegen.md](reference/codegen.md) — Authoritative reference for the `<job>.yaml` → PySpark codegen pipeline: Tera templates, source/transform/sink renderers, escape hatches, Glue vs EMR differences, and a "how to add a new provider/source/transform/sink" guide.
- [reference/airflow-dag.md](reference/airflow-dag.md) — `dag.yaml` schema, AirflowSection / AirflowJobBlock field reference, operator mapping, cross-account connections, Airflow Datasets, version matrix, and per-source backfill semantics. *(Phase 35 will lift the worked-example trailer into how-to recipes.)*

### Providers

- [reference/providers/glue.md](reference/providers/glue.md) — AWS Glue provider config: every knob in `GlueRawConfig`, AWS resources created/updated, IAM action requirements, and limitations.
- [reference/providers/emr.md](reference/providers/emr.md) — AWS EMR (classic, NOT Serverless) provider config: every knob in `EmrRawConfig`, step-submission model, and the existing-cluster requirement.

### Migrations

- [reference/migrations/v1.6.md](reference/migrations/v1.6.md) — v1.6 migration guide: hard rename of `triggered_by:` → `trigger:` and `produces:` → `publishes:`, plus the per-field `airflow.aws:` cascade and one-time post-upgrade state-hash drift callout.
- [reference/migrations/v1.11.md](reference/migrations/v1.11.md) — v1.11 migration guide: version-aware codegen (`airflow.version: "3"` emits Asset/providers-standard instead of Dataset/legacy), `"asset"` trigger alias, and one-time state-hash drift.

## Explanation

- [explanation/architecture.md](explanation/architecture.md) — System overview, four-crate workspace layout (yard-cli / yard-core / yard-structs / yard-server), crate dependency graph, trait-based provider model, and the layering rules each crate enforces.
- [explanation/why-codegen.md](explanation/why-codegen.md) — Rationale for yard's codegen design: why Tera scaffolds, why dataframe bodies are baked in Rust at apply-time, and why the split. Pairs with [reference/codegen.md](reference/codegen.md) for the rules.

## Examples

*Coming in Phase 35: `examples/glue-spark-etl/` (source → transform → sink Glue Spark template) + `examples/multi-job-dag/` (multi-job orchestration with `dag.yaml`). CI will run `yard validate` against each example on every PR.*

## Server

- [server/overview.md](server/overview.md) — yard-server overview: the Atlantis-like GitHub-webhook-driven workflow, drift-detection daemon, Dioxus dashboard, and the v1.5 Phase 25 auth + Slack-secret-store posture.
- [server/api.md](server/api.md) — yard-server HTTP + WebSocket API reference: every route under `/api/*`, request/response shapes, the bearer-token + cookie-session auth model, and the GitHub webhook contract.

## Contributing

- [contributing/development.md](contributing/development.md) — Contributor setup: repo layout, local dev environment, build/run commands for the CLI and yard-server, lint/format workflow, coding rules, and the "adding a new provider / CLI command" recipes.
- [contributing/testing.md](contributing/testing.md) — Test taxonomy, how to invoke tests at workspace/crate/test scope, per-crate layout, the in-memory yard-server test harness, the `ministack`-backed integration suite, and how CI runs the full battery.

---

*Doc count rises further after Phase 35 fills `examples/` and adds the additional how-to recipes. Phase 36 closes the milestone with the root-`README.md` refresh and a final stale-content sweep.*
