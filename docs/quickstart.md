<!-- generated-by: gsd-doc-writer -->
# Getting Started

This guide goes from nothing to a first `yard apply`. It covers installing
yard, scaffolding a project, writing one job file, and running
`yard validate`, `yard plan`, and `yard apply`.

yard itself does not create anything in a cloud account. Every job names a
provider plugin, and the plugin does the deploying. This guide is written for
any provider; wherever the plugin decides what happens, it says so and points
you at the plugin's documentation.

If you just want to skim, the shortest possible path is:

```bash
brew install yggdrasilLabs/yard/yard
mkdir my-project && cd my-project && yard init
# edit yard.yaml + add one <job>.yaml, then:
yard validate
yard plan
yard apply
```

The rest of this document explains each step.

---

## Prerequisites

### A provider plugin

Decide which provider you are deploying to and find its plugin. From the
plugin's documentation you need three things:

- **A release to point at.** Each job file carries `plugin_source` (usually
  the plugin's GitHub repository URL) and `plugin_version`. yard fetches the
  release asset for the machine it runs on; you never name a platform.
- **The config it accepts.** The block under `providers.<type>` in
  `yard.yaml`, and any provider-specific fields in job files, are defined by
  the plugin. yard validates them against the schema the plugin reports and
  passes them through.
- **The resources it needs.** A plugin typically needs credentials with
  specific permissions, and often things like an IAM role, a bucket for
  generated scripts, or a cluster to submit to. yard does not create any of
  these. Set them up before `yard apply`, following the plugin's docs.

### Credentials

Plugins run as child processes of yard and inherit its environment, so
credentials exported in your shell, a named profile, an instance role, or an
SSO session are all visible to them. What permissions a plugin needs is in
its documentation.

yard uses AWS credentials itself only for the S3 state backend, through the
standard AWS SDK default chain. For cross-account setups yard also reads
`YARD_AWS_ASSUME_ROLE`, `YARD_AWS_SESSION_NAME`, and `YARD_AWS_EXTERNAL_ID`
(passed on to plugins) and `YARD_STATE_AWS_*` (state bucket only). See
[configuration](reference/configuration.md#yard-cli-environment-variables).

### Rust toolchain (source builds only)

If you install yard with Homebrew or a release binary, skip this section.

Building from source needs `cargo`. The workspace uses Rust **edition 2024**,
which requires **Rust 1.85 or newer**. CI builds against `stable` on
`ubuntu-latest` (see `.github/workflows/ci.yml`), so any recent stable release
works.

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

---

## Installation

### Homebrew (recommended)

Works on macOS and Linux:

```bash
brew install yggdrasilLabs/yard/yard
```

Later releases are picked up with `brew upgrade yard`.

### Release binary

Every release attaches a tarball per platform (macOS Apple Silicon and Intel,
Linux x86_64 and ARM) to [GitHub Releases](https://github.com/sean-mca/yard/releases).
Download the one for your platform, extract it, and put the `yard` binary on
your `PATH`. See [Upgrade yard](how-to/upgrade-yard.md#from-a-github-release-binary)
for the checksum-verified procedure.

### Build from source

For contributors, or platforms without a prebuilt binary. Requires the Rust
toolchain from [Prerequisites](#rust-toolchain-source-builds-only).

```bash
git clone https://github.com/sean-mca/yard.git
cd yard
cargo build --release
```

The resulting binary is at `target/release/yard`. Either add that directory to
your `PATH` for the session:

```bash
export PATH="$PWD/target/release:$PATH"
```

or install it into `~/.cargo/bin` permanently:

```bash
cargo install --path yard-cli
```

### Verify the install

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

We will create a one-job project that reads a dataset, filters it, and writes
the result. In the snippets below, `<type>` stands for your provider plugin's
job type (the name the plugin registers under, such as `glue` for a Glue
plugin) and `<version>` and `<url>` for the release you picked in
[Prerequisites](#a-provider-plugin).

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

### 2. Fill in the provider block

Open `yard.yaml` and add a block under `providers:` keyed by your plugin's
job type. The fields inside it are whatever the plugin documents. A Glue
plugin wants a bucket for generated scripts and a region; an Airflow plugin
wants the bucket its DAG files go to.

```yaml
project: yard-tutorial

state:
  type: local
  path: .yard/state

providers:
  <type>:
    # fields defined by the plugin -- see its documentation
```

Everything in this block is passed to the plugin with every job of that type,
after being merged with the `<type>:` block in the job file, if there is one.
yard checks the block against the plugin's schema during `validate`, so a
missing required field is reported before anything is deployed.

### 3. Add one job

Create a file `orders.yaml` next to `yard.yaml`. Three fields are yard's:

```yaml
type: <type>
plugin_version: "<version>"
plugin_source: "<url>"
```

`type` selects the plugin. `plugin_source` says where it comes from: a GitHub
repository (`https://github.com/<org>/yard-plugin-<type>`, the release asset
for your platform is picked automatically), a git repository to build from
(`git::<url>`), or a local binary (`file://<path>`). `plugin_version` is the
release or git ref to use, and is ignored for `file://` sources. See
[configuration](reference/configuration.md#plugin_source-forms) for every
form.

Everything else in the file is defined by the plugin. yard passes it through
and validates it against the schema the plugin reports, so the shape of a job
depends entirely on which plugin it names. Two examples, using the Glue and
Airflow plugins:

<!-- tabs:start -->
<!-- tab: Glue plugin -->
**Glue plugin**

A Glue plugin describes a Spark job as sources, transforms, and a sink, and
needs the IAM role the job runs as:

```yaml
type: glue
plugin_version: "0.1.0"
plugin_source: "https://github.com/<org>/yard-plugin-glue"
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
<!-- tab: Airflow plugin -->
**Airflow plugin**

An Airflow plugin describes a DAG instead: where the DAG file goes, when it
runs, and its tasks. There are no sources or sinks.

```yaml
type: airflow
plugin_version: "0.1.0"
plugin_source: "https://github.com/<org>/yard-plugin-airflow"
dags_bucket: my-airflow-dags

schedule: "@daily"
tasks:
  - task_id: refresh_orders
    task_type: bash
    command: "echo refreshing orders"
```
<!-- tabs:end -->

Which fields your plugin expects, and what resources they refer to (the role
and buckets above have to exist already), come from the plugin's
documentation. Replace the placeholder values with real ones from your
account.

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

If you mistyped a field, `yard validate` will print the offending file, the
field path, and an actionable error. Errors about provider-specific fields
come from the plugin's `validate` operation and read the same way. Fix the job
file and re-run. Validation also runs implicitly before `plan` and `apply`,
but it is faster to iterate on.

### 5. Plan the change

```bash
yard plan
```

Expected output for a first run:

```
Downloading yard-plugin-<type> v<version>...
Done.

--- Plan for yard-tutorial ---

  + Create job [orders]
```

The first `plan` downloads the plugin binary declared in `orders.yaml`,
caches it under `.yard/plugins/`, and records its SHA-256 checksum in
`yard.lock` at the project root. Commit `yard.lock`; later runs verify the
cached binary against it and never re-download unless `plugin_version`
changes.

The `+` means "create" — yard has no existing state for `orders` and will
create it on apply. Subsequent runs after an `apply` show `No changes`
until you edit `orders.yaml`, at which point a `~ Modify` line appears with
the changed field names.

You can inspect what the plugin generated for the job, without deploying
anything:

```bash
yard show orders
```

This asks the plugin for its `codegen` output and prints it to stdout. For a
plugin that generates scripts this is the script; pipe it to a file if you
want to review it (`yard show orders > orders.py`).

### 6. Apply

```bash
yard apply
```

yard prints the same plan again and then prompts:

```
Do you want to apply these changes? (y/n)
```

Type `y` (or re-run with `--auto-approve` to skip the prompt). yard takes a
lock on the `orders` job, hands the generated artifact and the merged config
to the plugin's `deploy` operation, and waits. The plugin creates or updates
whatever its target is and reports back the resources it made. yard records
those in `.yard/state/orders.json` and releases the lock.

Expected output:

```
Applying...
  + Created: orders

State updated successfully.
```

That's a complete deploy.

---

## After the apply

### Check yard's state

```bash
cat .yard/state/orders.json
```

This is the per-job state file. It contains a `config_hash` (BLAKE3 of the
generated artifact + merged config), the `resources` list the plugin
reported, the plugin version and source used, the applied timestamp, and the
full merged config.

### Check the deployed resource

The `resources` list in the state file names what the plugin created, with a
type and an id. Verify it with your provider's own tooling; the plugin's
documentation says what to look for.

`yard apply` deploys the job definition but does not run it. Running it, and
watching it run, is done with the provider's tooling as well. yard's
responsibility ends at the deploy.

### Make a change

Edit `orders.yaml`, then run `yard plan` again. The plan shows a `~ Modify`
line naming what changed, because the artifact and config hash no longer
match the state file. `yard apply` sends the new artifact to the plugin.

### Tear it down

```bash
yard destroy orders
```

yard passes the recorded resources to the plugin's `destroy` operation and
removes the state file once it succeeds. `--auto-approve` and `--dry-run`
work here too.

---

## Common setup issues

**`yard plan` fails with `provider '<type>' is now a plugin -- add plugin_version and plugin_source`.**

The job file is missing the two plugin fields that v2.0 requires. Add
`plugin_version` and `plugin_source` as shown in step 3; see the
[v2.0 migration guide](reference/migrations/v2.0.md) if you are upgrading an
existing project.

**`yard plan` fails while downloading the plugin (404 or checksum mismatch).**

A 404 means the `plugin_source` URL does not match a release asset — check the
version and the platform suffix. A checksum mismatch means the cached binary
differs from what `yard.lock` recorded; delete the entry from `yard.lock` only
if you expect the binary to have changed.

**`yard validate` reports a missing provider field.**

The plugin's schema marks a field under `providers.<type>` (or on the job) as
required and it is not set. Step 2 covers the `providers:` block; the plugin's
documentation says what goes in it.

**The plugin fails during `apply` with a credentials or permissions error.**

The plugin could not reach its target with the credentials in your
environment. Check that credentials resolve in the shell you run yard from,
and compare the permissions against the list in the plugin's documentation.
Resources the plugin expects to exist already, such as a bucket or a role,
must be created by you.

**`cargo build --release` fails with an edition / toolchain error (source builds).**

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
  yard-structs crates fit together, the provider trait, state
  storage, and the end-to-end data flow for `plan` / `apply`.
- **[configuration](reference/configuration.md)** — the full reference for
  `yard.yaml`, `account.yaml`, `region.yaml`, per-job fields (plugin
  selection, sources, sinks, transforms, partitioning, PII masking), and
  every environment variable the CLI reads.
- **[build a plugin](how-to/build-a-plugin.md)** — the plugin protocol and
  the Rust SDK, for adding a provider yard does not have yet.
- **[development](contributing/development.md)** — how to work on yard itself — build
  commands, running tests, linting, and the workspace layout.
- **README project structure** — the [hierarchical multi-account
  layout](../README.md#project-structure) for teams managing many accounts
  and regions.
