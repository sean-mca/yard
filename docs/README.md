# yard documentation

← [Back to repo README](../README.md)

18 pages across 7 categories. This index links to every page in `docs/`.

> Diátaxis-lite layout: tutorial (one quickstart) / how-to (task recipes) / reference (schema + CLI + migrations) / explanation (architecture) / examples (copy-paste projects) / server (yard-server) / contributing (developer setup).

**New in v2.0:** providers are external plugin binaries, not compiled into yard.
If you are upgrading from v1.x, start with the
[v2.0 migration guide](reference/migrations/v2.0.md). If you are writing a
provider, start with [build a plugin](how-to/build-a-plugin.md).

## Quickstart

- [quickstart.md](quickstart.md) — Zero-to-deployed walkthrough: prerequisites, building yard from source, scaffolding a project, authoring one job, and running `yard plan` / `yard apply`.

## How-to

- [how-to/build-a-plugin.md](how-to/build-a-plugin.md) — Build a provider plugin: the Rust SDK tutorial (`yard-plugin-sdk`, the `PluginHandler` trait, local testing, release naming) plus the raw JSON-over-stdio protocol spec for plugins in other languages.
- [how-to/cross-account-deploy.md](how-to/cross-account-deploy.md) — Split state bucket and deploy targets across AWS accounts: the per-field `aws:` cascade and the `YARD_*` / `YARD_STATE_AWS_*` CI env-var overrides.
- [how-to/upgrade-yard.md](how-to/upgrade-yard.md) — Upgrade procedure per install method, post-upgrade drift check, and the index of per-version migration guides.
- [how-to/deploy.md](how-to/deploy.md) — Operator guide for deploying yard-server: topology, environment variables, DynamoDB table, GitHub webhook setup, drift-poll loop tuning.

## Reference

- [reference/cli.md](reference/cli.md) — Every `yard` subcommand (`init`, `plan`, `apply`, `show`, `validate`, `destroy`, `force-unlock`, `list targets`) and every flag, with synopsis + flag table + examples. CI-guarded by `.github/workflows/verify-cli-docs.yml` against drift.
- [reference/configuration.md](reference/configuration.md) — Full configuration surface: `yard.yaml` / `<account>.yaml` / `<region>.yaml` / `<job>.yaml` field reference (including the required `plugin_version` / `plugin_source`), CLI environment variables, yard-server env vars, and the runtime Settings page.

### Providers

Provider configuration is documented by each plugin's own repository. These
pages are forwarding stubs kept so v1.x links resolve.

- [reference/providers/glue.md](reference/providers/glue.md) — Moved to the `yard-plugin-glue` repo.
- [reference/providers/emr.md](reference/providers/emr.md) — Moved to the `yard-plugin-emr` repo.

### Migrations

- [reference/migrations/v2.0.md](reference/migrations/v2.0.md) — **Current.** Providers move to plugin binaries; `plugin_version` / `plugin_source` become required per job; Airflow DAG generation and `yard show dag` removed from core; plugin auto-download and the `yard.lock` TOFU checksum model.
- [reference/migrations/v1.11.md](reference/migrations/v1.11.md) — *Historical (v1.x).* Version-aware DAG codegen and the `"asset"` trigger alias.
- [reference/migrations/v1.6.md](reference/migrations/v1.6.md) — *Historical (v1.x).* Hard rename of `triggered_by:` → `trigger:` and `produces:` → `publishes:`.

## Explanation

- [explanation/architecture.md](explanation/architecture.md) — System overview, five-crate workspace layout, crate dependency graph, the plugin host and protocol boundary, and the layering rules each crate enforces.

## Examples

- [examples/glue-spark-etl/](examples/glue-spark-etl/README.md) — A single Glue Spark job: source → transform → sink, with the hierarchical `yard.yaml` / `account.yaml` / `region.yaml` context tree. CI runs `yard validate` against it on every PR.

## Server

- [server/overview.md](server/overview.md) — yard-server overview: the Atlantis-like GitHub-webhook-driven workflow, drift-detection daemon, Dioxus dashboard, and the auth + Slack-secret-store posture.
- [server/api.md](server/api.md) — yard-server HTTP + WebSocket API reference: every route under `/api/*`, request/response shapes, the bearer-token + cookie-session auth model, and the GitHub webhook contract.

## Contributing

- [contributing/development.md](contributing/development.md) — Contributor setup: repo layout, local dev environment, build/run commands for the CLI and yard-server, lint/format workflow, coding rules, and the "adding a new CLI command" recipe.
- [contributing/testing.md](contributing/testing.md) — Test taxonomy, how to invoke tests at workspace/crate/test scope, per-crate layout, the in-memory yard-server test harness, the `ministack`-backed integration suite, and how CI runs the full battery.
