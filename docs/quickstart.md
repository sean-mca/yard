<!-- generated-by: gsd-doc-writer -->
# Getting Started

This guide walks you from zero to a deployed AWS Glue job managed by yard. It
covers prerequisites, building yard from source, scaffolding a project,
authoring one job, and running `yard plan` / `yard apply` to deploy it.

If you just want to skim, the shortest possible path is:

```bash
git clone https://github.com/sean-mca/yard.git && cd yard && cargo build --release
export PATH="$PWD/target/release:$PATH"
mkdir my-project && cd my-project && yard init
# edit yard.yaml + add one <job>.yaml, then:
yard validate
yard plan
yard apply
```

The rest of this document explains each step.

---

## Prerequisites

### Rust toolchain

yard is written in Rust and currently has no prebuilt binaries — you build
from source with `cargo`. The workspace uses Rust **edition 2024**, which
requires **Rust 1.85 or newer**. CI builds against `stable` on `ubuntu-latest`
(see `.github/workflows/ci.yml`), so any recent stable release works.

If you don't have Rust installed:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
# then restart your shell, or:
source "$HOME/.cargo/env"
```

Verify:

```bash
cargo --version   # cargo 1.85 or newer
rustc --version   # rustc 1.85 or newer
```

### AWS credentials (for the `glue` and `emr` providers)

yard does not ship its own credential manager — it uses the standard AWS SDK
default credential chain (env vars → `~/.aws/credentials` → IMDS → SSO). Any
one of these works:

- `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` exported
  in your shell.
- `aws configure --profile <name>` followed by `export AWS_PROFILE=<name>`.
- An EC2 / ECS / Lambda instance role.
- `aws sso login` against a configured SSO profile.

Verify credentials resolve:

```bash
aws sts get-caller-identity
```

Your caller identity needs permission to:

- **S3:** `PutObject`, `GetObject`, `DeleteObject`, `HeadBucket`, `ListBucket`
  on the script bucket and (if used) the state bucket.
- **Glue:** `CreateJob`, `UpdateJob`, `DeleteJob`, `GetJob`, `StartJobRun`,
  and `PassRole` for the Glue execution role referenced in each job file.
- **EMR** (only if using the EMR provider): `AddJobFlowSteps`,
  `DescribeStep`, `CancelSteps` on the target cluster.

For the full IAM policy sketches (DynamoDB runtime for `yard-server`,
Glue + EMR + S3 + STS for `yard apply` from PRs, Secrets Manager for
the v1.5 Slack webhook abstraction), see
[deploy.md → AWS resources needed](how-to/deploy.md#aws-resources-needed).

If you plan to use AssumeRole (for cross-account deploys), yard also reads
`YARD_AWS_ASSUME_ROLE`, `YARD_AWS_SESSION_NAME`, and `YARD_AWS_EXTERNAL_ID`
env vars — see [configuration](reference/configuration.md#yard-cli-environment-variables).

### S3 bucket(s)

You need at least one S3 bucket that yard can write generated PySpark scripts
to. This is the `providers.glue.script_bucket` (or
`providers.emr.script_bucket`) in `yard.yaml`. The bucket must exist before
you run `yard apply` — yard does not create it for you.

If you also want to use the S3 state backend (recommended for anything beyond
a single-developer prototype), you need a second bucket for state. Local
state is fine for getting started.

### AWS CLI (optional but useful)

Not required by yard, but handy for verifying the resources yard creates. Any
recent v2 release works:

```bash
aws --version
```

---

## Installation

No releases are published to crates.io yet. Build from source:

```bash
git clone https://github.com/sean-mca/yard.git
cd yard
cargo build --release
```

The resulting binary is at `target/release/yard`. You have three options for
invoking it:

1. **Add to PATH** (recommended for tutorials):
   ```bash
   export PATH="$PWD/target/release:$PATH"
   ```
2. **Install into `~/.cargo/bin`** so `yard` is on your PATH permanently:
   ```bash
   cargo install --path yard-cli
   ```
3. **Invoke directly** via the full path: `./target/release/yard …`.

Verify the install:

```bash
yard --version
yard --help
```

You should see the subcommands `init`, `plan`, `apply`, `show`, `validate`,
`list`, `destroy`, `force-unlock`. The `list` subcommand (added in v1.3.4)
emits `yard list targets [--json]` rows for CI matrix builders fanning out
per-account deploys; see [reference/cli.md](reference/cli.md) for the full
flag surface.

---

## Your first job — a minimal tutorial

We will create a one-job project that filters an S3 dataset with Glue.

### 1. Scaffold the project

```bash
mkdir ~/yard-tutorial
cd ~/yard-tutorial
yard init
```

`yard init` writes a starter `yard.yaml` with a `local` state backend and
creates the state directory. After it finishes you should see:

```
Created <path>/yard.yaml
Initialized state at .yard/state
```

The generated `yard.yaml` looks like:

```yaml
project: my-yard-project

state:
  type: local
  path: .yard/state

providers:
```

### 2. Fill in the Glue provider block

Open `yard.yaml` and add a `glue:` key under `providers:` pointing at the S3
bucket where yard should upload generated scripts. Replace the placeholder
values with your own bucket and region:

```yaml
project: yard-tutorial

state:
  type: local
  path: .yard/state

providers:
  glue:
    script_bucket: my-yard-scripts-bucket
    region: us-east-1
```

The full list of `providers.glue` fields (worker type, Glue version,
bookmarks, connections, etc.) is documented in
[configuration](reference/configuration.md#providersglue--aws-glue-provider-defaults).
The defaults (`script_prefix: yard-scripts/`, `glue_version: 4.0`,
`worker_type: G.1X`, `number_of_workers: 2`) are fine for this tutorial.

### 3. Add one job

Create a file `orders.yaml` next to `yard.yaml`:

```yaml
type: glue
role: arn:aws:iam::123456789012:role/GlueJobExecutionRole

sources:
  - name: orders
    type: s3
    format: parquet
    path: s3://my-data-lake/raw/orders/

transforms:
  - type: filter
    condition: "col('status') != 'cancelled'"

sink:
  type: s3
  format: parquet
  path: s3://my-data-lake/curated/orders/
  mode: overwrite
```

Replace the `role` ARN, the `sources[0].path`, and the `sink.path` with real
values in your account. The `role` is the IAM role Glue assumes when running
the job — it must be able to read from the source path and write to the sink
path.

Your project directory now looks like:

```
yard-tutorial/
  yard.yaml
  orders.yaml
  .yard/
    state/
```

The filename `orders.yaml` becomes the job name (`orders`) — yard discovers
job files by walking the directory tree from `yard.yaml` downward. For a
hierarchical multi-account layout (e.g. `aws/dev/us-east-2/orders.yaml`), see
the project structure section of the [README](../README.md#project-structure)
and the context-inheritance rules in
[configuration](reference/configuration.md#accountyaml--regionyaml-hierarchical-context).

### 4. Validate the job

```bash
yard validate
```

Expected output:

```
Validating project: yard-tutorial

[PASS] orders.yaml

Validation complete: 1 passed, 0 failed
```

If you mistyped a field (e.g. `type: gluue`), `yard validate` will print the
offending file, the field path, and an actionable error. Fix the job file and
re-run. Validation also runs implicitly before `plan` and `apply`, but it is
faster to iterate on.

### 5. Plan the change

```bash
yard plan
```

Expected output for a first run:

```
--- Plan for yard-tutorial ---

  + Create job [orders]
```

The `+` means "create" — yard has no existing state for `orders` and will
create it on apply. Subsequent runs after an `apply` show `No changes`
until you edit `orders.yaml`, at which point a `~ Modify` line appears with
the changed field names.

You can inspect the PySpark script that will be uploaded without deploying
anything:

```bash
yard show orders
```

This prints the generated Python to stdout. Pipe it to a file if you want to
review it (`yard show orders > orders.py`).

### 6. Apply

```bash
yard apply
```

yard prints the same plan again and then prompts:

```
Do you want to apply these changes? (y/n)
```

Type `y` (or re-run with `--auto-approve` to skip the prompt). yard then:

1. Uploads the generated script to
   `s3://my-yard-scripts-bucket/yard-scripts/orders.py` (bucket + prefix from
   your `providers.glue` config).
2. Calls `glue:CreateJob` to create the Glue job named `orders`.
3. Writes the deployment record to `.yard/state/orders.json`.

Expected output:

```
Applying...
  + Created: orders

State updated successfully.
```

That's a complete deploy.

---

## Verifying the job ran

`yard apply` creates the Glue job definition but does not execute a run — you
still control when the job runs. Verify the deploy landed in AWS:

### Check the Glue job exists

```bash
aws glue get-job --job-name orders --region us-east-1
```

You should see a `Job` payload with `Role`, `Command.ScriptLocation` pointing
at your `s3://…/yard-scripts/orders.py`, and the default `GlueVersion`,
`WorkerType`, and `NumberOfWorkers` from your `providers.glue` block.

### Check the uploaded script

```bash
aws s3 ls s3://my-yard-scripts-bucket/yard-scripts/
```

You should see `orders.py` with a recent timestamp.

### Check yard's state

```bash
cat .yard/state/orders.json
```

This is the per-job state file. It contains a `config_hash` (BLAKE3 of the
script + merged config), the `resources` list yard created, the applied
timestamp, and the full merged config used at apply time.

### Run the job (optional)

If you actually want Glue to execute the job:

```bash
aws glue start-job-run --job-name orders --region us-east-1
```

Then watch the run:

```bash
aws glue get-job-runs --job-name orders --region us-east-1 --max-items 1
```

Running the job exercises yard's generated PySpark script against your real
data, not yard itself. yard's responsibility ends at the deploy.

---

## Common setup issues

**`aws sts get-caller-identity` fails with "Unable to locate credentials."**

The AWS SDK default chain could not find credentials. Run `aws configure`
to set up `~/.aws/credentials`, or `export AWS_ACCESS_KEY_ID=…` directly,
or `aws sso login` against an SSO profile. Verify with
`aws sts get-caller-identity` before re-running yard.

**`yard apply` fails with `providers.glue.script_bucket is required`.**

You skipped step 2 — add a `glue:` block with a `script_bucket` under
`providers:` in `yard.yaml`.

**`yard apply` fails with `Job "orders" requires a "role"`.**

The job file needs a top-level `role:` field naming the IAM role Glue should
assume. This must be an ARN, not just a role name
(`arn:aws:iam::ACCOUNT:role/ROLE_NAME`).

**`yard apply` fails with `Failed to reach S3 bucket … in …`.**

The script bucket doesn't exist, is in a different region, or your
credentials can't see it. Create it first
(`aws s3 mb s3://my-yard-scripts-bucket --region us-east-1`), or update
`providers.glue.region` to match where the bucket actually lives.

**`cargo build --release` fails with an edition / toolchain error.**

Your Rust toolchain is older than 1.85. Update with `rustup update stable`
and try again.

**A second `yard apply` hangs or reports `stale lock`.**

yard uses per-job lock files in the state backend. If a previous run was
interrupted (SIGKILL, crashed CI runner), a stale lock may remain. Remove it
with:

```bash
yard force-unlock orders
```

Then re-run `yard apply`.

**Running `yard` commands outside the project root.**

Every subcommand takes an optional trailing directory argument
(`yard plan ~/yard-tutorial`), and internally `yard` walks upward looking for
`yard.yaml`. If you see "failed to find yard.yaml", you're either not inside
a yard project or you passed the wrong directory.

---

## Next steps

You now have a working single-job yard project. From here:

- **[architecture](explanation/architecture.md)** — how the yard-cli / yard-core /
  yard-structs / yard-server crates fit together, the provider trait, state
  storage, and the end-to-end data flow for `plan` / `apply`.
- **[configuration](reference/configuration.md)** — the full reference for
  `yard.yaml`, `account.yaml`, `region.yaml`, `dag.yaml`, per-job fields
  (sources, sinks, transforms, partitioning, Airflow metadata), and every
  environment variable the CLI reads.
- **[development](contributing/development.md)** — how to work on yard itself — build
  commands, running tests, linting, and the workspace layout.
- **README project structure** — the [hierarchical multi-account
  layout](../README.md#project-structure) for teams managing many accounts
  and regions.
- **[deploy](how-to/deploy.md)** — deploying yard-server (the companion
  service for GitHub-webhook-driven PR workflows and drift detection). Not
  needed for the CLI-only workflow you just set up.
