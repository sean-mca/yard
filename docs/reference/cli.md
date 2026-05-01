# CLI reference

This page documents every `yard` subcommand and every flag it accepts. Output is captured from `cargo run -p yard -- <cmd> --help` and updated on every PR by the verify-cli-docs CI guard ([scripts/verify-cli-docs.sh](../../scripts/verify-cli-docs.sh)).

- [Global flags](#global-flags)
- [yard apply](#yard-apply)
- [yard destroy](#yard-destroy)
- [yard force-unlock](#yard-force-unlock)
- [yard init](#yard-init)
- [yard list](#yard-list)
- [yard plan](#yard-plan)
- [yard show](#yard-show)
- [yard validate](#yard-validate)
- [See also](#see-also)

---

## Global flags

The following flags are accepted on every subcommand. `--help` and `--version` are clap-injected; the remaining two are defined on `Cli` in `yard-cli/src/parser.rs` with `global = true`.

| Flag | Required | Default | Description |
|------|----------|---------|-------------|
| `--no-color` | No | unset | Disable colored output. Equivalent to setting `NO_COLOR=1` (see [configuration.md "yard CLI environment variables"](configuration.md#yard-cli-environment-variables)). |
| `--colorblind` | No | unset | Use colorblind-friendly palette (cyan/blue/magenta instead of green/yellow/red). |
| `--help` | No | — | Print help. Also accepted as `-h`. Available on every subcommand. |
| `--version` | No | — | Print version. Also accepted as `-V`. Root command only. |

Example:

```bash
yard --version
yard plan --no-color
```

---

## yard apply

Synopsis:

```
yard apply [OPTIONS] [DIRECTORY]
```

Apply pending changes to AWS. For each target in the project, codegen renders the script, the provider deploys it (Glue `UpdateJob`/`CreateJob`, EMR `AddJobFlowSteps`, S3 script upload), and yard updates state. Use `--target <NAME>` to limit scope to a single job; use `--dry-run` to skip provider deployment and exercise codegen + state only.

| Flag | Required | Default | Description |
|------|----------|---------|-------------|
| `--dry-run` | No | unset | Skip provider deployment (codegen and state only). |
| `--auto-approve` | No | unset | Skip confirmation prompt. |
| `--target <TARGET>` | No | unset | Only apply a specific job. |
| Global flags | — | — | Inherits `--no-color`, `--colorblind`, `--help`. See [Global flags](#global-flags). |

Examples:

```bash
yard apply
yard apply --target etl-daily --dry-run
```

See [airflow-dag.md](airflow-dag.md) for what `yard apply` does to DAG resources.

---

## yard destroy

Synopsis:

```
yard destroy [OPTIONS] [JOB_NAME] [DIRECTORY]
```

Destroy deployed jobs and remove state. The optional positional `JOB_NAME` narrows the destroy to a single job; omit it to destroy every job in the project.

| Flag | Required | Default | Description |
|------|----------|---------|-------------|
| `--dry-run` | No | unset | Skip provider teardown (state and local files only). |
| `--auto-approve` | No | unset | Skip confirmation prompt. |
| Global flags | — | — | Inherits `--no-color`, `--colorblind`, `--help`. See [Global flags](#global-flags). |

Examples:

```bash
yard destroy etl-daily --auto-approve
yard destroy --dry-run
```

See [airflow-dag.md](airflow-dag.md) for DAG destroy semantics.

---

## yard force-unlock

Synopsis:

```
yard force-unlock [OPTIONS] <JOB_NAME> [DIRECTORY]
```

Force-unlock a locked job. `<JOB_NAME>` is required and identifies the per-job lock file to delete from the configured state backend. Use this only when a previous `yard apply` was interrupted and left a stale lock behind.

| Flag | Required | Default | Description |
|------|----------|---------|-------------|
| Global flags | — | — | Inherits `--no-color`, `--colorblind`, `--help`. See [Global flags](#global-flags). |

Example:

```bash
yard force-unlock etl-daily
```

---

## yard init

Synopsis:

```
yard init [OPTIONS] [DIRECTORY]
```

Initialize a new YARD project. Creates the minimal directory layout and a starter `yard.yaml` in `[DIRECTORY]` (defaults to the current working directory).

| Flag | Required | Default | Description |
|------|----------|---------|-------------|
| Global flags | — | — | Inherits `--no-color`, `--colorblind`, `--help`. See [Global flags](#global-flags). |

Example:

```bash
yard init my-project
```

---

## yard list

Synopsis:

```
yard list [OPTIONS] <COMMAND>
```

Parent command for deployment-target inventory subcommands. `yard list` itself accepts no extra flags beyond the global ones; the actionable surface lives under [`yard list targets`](#yard-list-targets).

| Flag | Required | Default | Description |
|------|----------|---------|-------------|
| Global flags | — | — | Inherits `--no-color`, `--colorblind`, `--help`. See [Global flags](#global-flags). |

### yard list targets

Synopsis:

```
yard list targets [OPTIONS] [DIRECTORY]
```

Emit all deployment targets (jobs + DAGs) as a JSON array to stdout, sorted alphabetically by `target`. Each row is `{target, kind, aws_account_id}` where `aws_account_id` is a 12-digit string or `null` (the key is always present). This command shipped in v1.3.4 and is intended for CI matrix builders that fan out `yard apply --target` per row.

| Flag | Required | Default | Description |
|------|----------|---------|-------------|
| `--json` | No | unset | Accepted for forward-compatibility; JSON is the only output mode in v1.4. The flag is a documented no-op today. |
| Global flags | — | — | Inherits `--no-color`, `--colorblind`, `--help`. See [Global flags](#global-flags). |

Examples:

```bash
yard list targets
yard list targets --json
```

---

## yard plan

Synopsis:

```
yard plan [OPTIONS] [DIRECTORY]
```

Compute the diff between current AWS state and the desired state derived from the project YAML. Renders scripts in-memory (no S3 upload), queries provider state, and reports add/update/delete/no-op per target. Use `--target <NAME>` to limit scope to a single job.

| Flag | Required | Default | Description |
|------|----------|---------|-------------|
| `--target <TARGET>` | No | unset | Only plan a specific job. |
| Global flags | — | — | Inherits `--no-color`, `--colorblind`, `--help`. See [Global flags](#global-flags). |

Examples:

```bash
yard plan
yard plan --target etl-daily
```

See [airflow-dag.md](airflow-dag.md) for DAG plan semantics.

---

## yard show

Synopsis:

```
yard show [OPTIONS] <JOB_NAME> [DIRECTORY]
```

Print the generated PySpark script for a single job to stdout. `<JOB_NAME>` is required. Useful for diffing codegen output without running `apply`.

| Flag | Required | Default | Description |
|------|----------|---------|-------------|
| Global flags | — | — | Inherits `--no-color`, `--colorblind`, `--help`. See [Global flags](#global-flags). |

Example:

```bash
yard show etl-daily > /tmp/etl-daily.py
```

See [airflow-dag.md](airflow-dag.md) for DAG show semantics.

---

## yard validate

Synopsis:

```
yard validate [OPTIONS] [DIRECTORY]
```

Validate every job and DAG configuration in the project: schema-check the YAMLs, run cross-DAG link checks, and verify provider knob acceptance. Reads (does not write) state — requires AWS credentials.

| Flag | Required | Default | Description |
|------|----------|---------|-------------|
| Global flags | — | — | Inherits `--no-color`, `--colorblind`, `--help`. See [Global flags](#global-flags). |

Example:

```bash
yard validate
```

---

## See also

- [configuration.md](configuration.md) — YAML schema and environment variables
- [providers/glue.md](providers/glue.md) — Glue provider knobs and AWS resources
- [providers/emr.md](providers/emr.md) — EMR provider knobs and AWS resources
- [airflow-dag.md](airflow-dag.md) — Airflow DAG schema, trigger semantics, and DAG-side behavior of plan/apply/show/destroy
- [codegen.md](codegen.md) — Source/sink/transform rendering rules
- [why codegen](../explanation/why-codegen.md) — Rationale behind the codegen design
