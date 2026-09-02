<!-- generated-by: gsd-doc-writer -->
# YARD

**YAML Architecture for Rapid Development**

[![CI](https://github.com/sean-mca/yard/actions/workflows/ci.yml/badge.svg)](https://github.com/sean-mca/yard/actions/workflows/ci.yml)
[![License: BSL 1.1](https://img.shields.io/badge/License-BSL_1.1-blue.svg)](LICENSE)

Declarative infrastructure for data pipelines. Define ETL jobs in YAML, and YARD deploys to AWS via provider plugins. Think Terragrunt, but for data engineering.

## Status

Active development. Latest release: v2.0.0. Plugin-based provider architecture — providers (Glue, EMR) are external plugin binaries. Per-target deployment via `yard list targets`. No stability guarantees yet.

## Why YARD?

Most data teams manage Glue jobs and EMR steps through a mix of Terraform, custom scripts, and ClickOps. When someone leaves, the knowledge of how things are wired together leaves with them.

YARD replaces all of that with a single YAML-driven workflow:

- **One file per job.** No Terraform modules, no CloudFormation, no copy-pasted boilerplate.
- **Plugin providers.** Provider logic runs as external binaries over a JSON protocol. Install `yard-plugin-glue` or `yard-plugin-emr`, or build your own with `yard-plugin-sdk`.
- **Plan/apply lifecycle.** See what will change before it changes. State is per-job, so teams deploy concurrently without locks.
- **Auto-download.** Plugins are downloaded from GitHub Releases on first use with TOFU checksum verification (`yard.lock`).

## Installation

Install via Homebrew:

```bash
brew install sean-mca/yard/yard
```

Or download a binary from [GitHub Releases](https://github.com/sean-mca/yard/releases) (macOS ARM/Intel, Linux ARM/x86_64).

Or build from source:

```bash
git clone https://github.com/sean-mca/yard.git
cd yard
cargo build --release
# Binary lives at target/release/yard
```

See [docs/quickstart.md](docs/quickstart.md) for prerequisites and first-run setup.

## Demo

```yaml
# orders.yaml
type: glue
plugin_version: "0.1.0"
plugin_source: "https://github.com/your-org/yard-plugin-glue/releases/download/v${version}/yard-plugin-glue-${version}-${os}-${arch}"
role: arn:aws:iam::123456789:role/GlueJobExecutionRole

source:
  type: s3
  format: parquet
  path: s3://data-lake/raw/orders/

transforms:
  - type: filter
    condition: "col('status') != 'cancelled'"

sink:
  type: s3
  format: parquet
  path: s3://data-lake/curated/orders/
  mode: overwrite
```

```
$ yard plan
--- Plan for my-project ---

  + Create job [orders]

$ yard apply --auto-approve
Applying...
  + Created: orders

State updated successfully.
```

That's it. YARD downloaded the Glue plugin, generated the PySpark script, uploaded it to S3, and created the Glue job.

## Plugins

Providers are external plugin binaries that communicate with yard over a JSON-over-stdio protocol.

| Plugin | Status | What it does |
|--------|--------|--------------|
| [yard-plugin-glue](https://github.com/your-org/yard-plugin-glue) | In progress | PySpark codegen, S3 upload, Glue job create/update/destroy |
| [yard-plugin-emr](https://github.com/your-org/yard-plugin-emr) | In progress | PySpark codegen, S3 upload, EMR step submission |

See [docs/how-to/build-a-plugin.md](docs/how-to/build-a-plugin.md) to build your own provider plugin.

## Project structure

```
my-project/
  yard.yaml                      # Root config: project name, state backend, providers
  aws/
    dev/
      account.yaml               # Account-level context (inherited by jobs below)
      us-east-2/
        region.yaml              # Region-level context
        orders.yaml              # Job definition
        customers.yaml           # Job definition
    prod/
      account.yaml
      us-east-1/
        region.yaml
        orders.yaml
```

Directory hierarchy mirrors your cloud topology. Context files (`account.yaml`, `region.yaml`) at each level are inherited by all job files below them. Variables are referenced with `${account.id}`, `${region.id}`, etc.

## CLI

```
yard init              Initialize state for all jobs
yard plan              Show what would change
yard apply             Deploy changes (with confirmation)
yard show <job>        Display the generated script
yard validate          Check all job definitions
yard list targets      List deployable targets (JSON output)
yard destroy [job]     Tear down deployed jobs
yard force-unlock <job>  Remove a stale lock
```

All commands support `--no-color` and `--colorblind`. `--target <job>` scopes plan/apply to a single job. `--auto-approve` and `--dry-run` work on apply and destroy.

## yard-server

Web dashboard with GitHub webhook integration and drift detection. PR-driven workflow: plan runs automatically on PR open, apply triggered by commenting `yard apply`.

![Dashboard](docs/images/dashboard.png)

![Jobs](docs/images/job_with_content_sheet.png)

![Drift Detection](docs/images/diff_with_content_sheet.png)

See [docs/how-to/deploy.md](docs/how-to/deploy.md) for setup instructions.

## Architecture

Rust workspace with five crates:

| Crate | Purpose |
|-------|---------|
| `yard-cli` | Thin CLI wrapper -- parses args, calls core, formats output |
| `yard-core` | Orchestrator -- plugin host, state, storage, validation, config cascade |
| `yard-structs` | Shared types -- job definitions, state, config, plugin protocol |
| `yard-plugin-sdk` | SDK for building provider plugins (implements PluginHandler trait) |
| `yard-server` | Web dashboard -- Dioxus fullstack, axum API, DynamoDB |

Provider plugins are external binaries. Build one by implementing the `PluginHandler` trait from `yard-plugin-sdk`.

See [docs/explanation/architecture.md](docs/explanation/architecture.md) for a deeper walk-through.

## Documentation

See [docs/](docs/README.md) for the full documentation tree.

- [docs/quickstart.md](docs/quickstart.md) — install, prerequisites, first run
- [docs/examples/](docs/examples/) — copy-paste-ready example projects

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

Business Source License 1.1. See [LICENSE](LICENSE) for the full text.

## AI Disclosure
Claude was used as follows:
- yard-server
    - UI creation as I'm horrible at FE but wanted to try Dioxus
- Documentation: This README & `docs/**`
- General: a partner "architect"
    - example: "I think I want to design feature X like this, give me pros, cons, and any critical issues"
- General: repeating work I had already done
    - example: I wrote the initial commands in yard-cli/src/parser.rs, and would ask Claude to fill in new ones by copying what I did
- General: helping me find tech debt early
    - example: "Find all of the `unwrap()`s I missed"
