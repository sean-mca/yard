<!-- generated-by: gsd-doc-writer -->
# Development

This document is for contributors working on yard itself (adding providers,
fixing bugs in `yard-core`, extending the Dioxus UI, etc.). If you are
trying to *use* yard to deploy data jobs, start with
[GETTING-STARTED.md](GETTING-STARTED.md) instead.

- [Repo layout](#repo-layout)
- [Local dev setup](#local-dev-setup)
- [Building](#building)
- [Running the CLI locally](#running-the-cli-locally)
- [Running yard-server locally](#running-yard-server-locally)
- [Linting and formatting](#linting-and-formatting)
- [Coding rules](#coding-rules)
- [Adding a new provider](#adding-a-new-provider)
- [Adding a new CLI command](#adding-a-new-cli-command)

---

## Repo layout

yard is a Cargo workspace with four crates, declared in the root
`Cargo.toml`:

```toml
[workspace]
resolver = "3"
members = ["yard-cli", "yard-core", "yard-structs", "yard-server"]
```

| Crate | Role |
|-------|------|
| `yard-cli` (package name `yard`, produces the `yard` binary) | Thin CLI wrapper — parses `clap` args, delegates to `yard-core`, formats output. No business logic. |
| `yard-core` | Library. All logic: codegen, providers, storage, validation, diff, DAG generation, orchestration. |
| `yard-structs` | Shared `serde` types (`ProjectManifest`, `JobDefinition`, `JobState`, `JobDiff`, `StateBackend`, `Resource`, …). Minimal deps — `serde`, `anyhow`, `serde_json`. |
| `yard-server` | Dioxus fullstack binary — GitHub webhooks, drift polling, Axum API, Dioxus/Tailwind dashboard, DynamoDB persistence. |

The dependency graph is strict: `yard-cli` and `yard-server` depend on
`yard-core`; `yard-core` depends on `yard-structs`; `yard-structs`
depends on nothing inside the workspace. `yard-core` never depends on
`yard-cli` or `yard-server`.

For the full breakdown — component diagrams, data flow through a
`yard apply`, and per-directory descriptions — see
[ARCHITECTURE.md](ARCHITECTURE.md).

---

## Local dev setup

### Prerequisites

| Tool | Version | Notes |
|------|---------|-------|
| Rust toolchain | `stable` | CI uses `dtolnay/rust-toolchain@stable` with `clippy` and `rustfmt` components. No `rust-toolchain.toml` is pinned in the repo. |
| Cargo | Bundled with `rustup` | Workspace uses `resolver = "3"` (Edition 2024), which requires a recent enough toolchain. |
| Docker + Docker Compose | Any recent version | Only needed if you plan to run `yard-server` locally against `ministack` (S3 + DynamoDB emulator). |
| `dx` (Dioxus CLI) | Any version compatible with `dioxus = "0.7"` | Only needed for running the `yard-server` Dioxus UI. Install with `cargo install dioxus-cli`. <!-- VERIFY: exact dx version pinned for this Dioxus release --> |

### Clone and install dependencies

```bash
git clone https://github.com/sean-mca/yard.git
cd yard
cargo build --workspace
```

The first build is slow (AWS SDK + Dioxus have large dependency trees);
subsequent builds are incremental. Cargo will download and compile all
workspace members and their dependencies.

### Starting ministack (only needed for `yard-server`)

`docker-compose.yml` provisions two services:

- **`ministack`** — Localstack-like S3 + DynamoDB emulator on
  `localhost:4566`.
- **`init-aws`** — One-shot container that creates the `yard-state`
  S3 bucket and the `yard_yard` DynamoDB table (with the `PK`/`SK`/`GSI1`
  schema yard-server expects).

```bash
docker compose up -d
```

Wait for the `init-aws` container to exit cleanly (it prints
`--- ministack resources created ---` when done). After that, ministack
is ready for the server to point at via `YARD_DB_ENDPOINT_URL`.

---

## Building

All build commands target the entire workspace by default.

| Command | What it does |
|---------|--------------|
| `cargo build` | Debug build of every crate in the workspace. |
| `cargo build --workspace` | Same as above, explicit. |
| `cargo build --release` | Optimized build. The CLI binary lands at `target/release/yard`. |
| `cargo build -p yard` | Build only the CLI (package name is `yard`, not `yard-cli`). |
| `cargo build -p yard-core` | Build only the core library. |
| `cargo build -p yard-server` | Build only the server crate (native target). |
| `cargo test` | Run the full test suite. See [TESTING.md](TESTING.md) for details. |
| `cargo clippy --all-targets -- -D warnings` | Lint. Must be clean before any PR. |
| `cargo fmt --all` | Format. Must be clean before any PR. |
| `cargo fmt --all -- --check` | CI formatting gate (doesn't modify files). |

The `yard-server` crate also has a wasm32 target for the Dioxus UI. The
Dioxus CLI (`dx`) handles target selection; you don't normally invoke
`cargo build --target wasm32-*` directly.

---

## Running the CLI locally

The CLI lives in `yard-cli/` but the package name is `yard` (so the
produced binary is `yard`, not `yard-cli`). After `cargo build`, the
debug binary is at `target/debug/yard`.

### Using `cargo run`

```bash
# From a project directory with a yard.yaml:
cargo run -p yard -- plan
cargo run -p yard -- apply --dry-run
cargo run -p yard -- show orders
cargo run -p yard -- validate

# From outside a project directory, pass the path:
cargo run -p yard -- plan path/to/my-project
```

The `--` separates cargo's args from the CLI's args. Everything after
`--` is forwarded verbatim to the `yard` binary.

Global flags (`--no-color`, `--colorblind`) work on any subcommand. See
`yard-cli/src/parser.rs` for the full command/flag matrix, or run
`cargo run -p yard -- --help`.

### Using the built binary

```bash
cargo build --release
./target/release/yard plan
```

### Pointing at ministack for local AWS calls

The CLI uses the standard AWS SDK credential chain. To point it at
ministack instead of real AWS, set `AWS_ENDPOINT_URL` (honored by the
SDK) along with test credentials:

```bash
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1
export AWS_ENDPOINT_URL=http://localhost:4566
cargo run -p yard -- plan
```

See [CONFIGURATION.md](CONFIGURATION.md) for every env var yard respects.

---

## Running yard-server locally

The server is a Dioxus fullstack app. In dev, you run it via the Dioxus
CLI (`dx serve`), which rebuilds the native API + wasm UI on save.

### 1. Start ministack

```bash
docker compose up -d
```

(See [Local dev setup](#local-dev-setup) above.)

### 2. Configure env vars

Copy the template and fill in your GitHub credentials:

```bash
cp env.local.example .env.local
# edit .env.local, set YARD_GITHUB_TOKEN, YARD_WEBHOOK_SECRET,
# YARD_REPO_OWNER, YARD_REPO_NAME to real values
```

The template already has `YARD_DB_ENDPOINT_URL=http://localhost:4566`
and test AWS credentials, so DynamoDB traffic will hit ministack rather
than real AWS.

### 3. Start the server

From the `yard-server/` directory (the Dioxus CLI looks for
`Dioxus.toml` in the current directory):

```bash
cd yard-server
export $(cat ../.env.local | xargs)
dx serve
```

The server binds `0.0.0.0:3001` by default (override with `YARD_PORT`).
The UI is served at [http://localhost:3001](http://localhost:3001), and
the API is mounted under `/api` on the same port.

Real-time dashboard updates use a WebSocket at `/api/ws/events`; the
Dioxus UI connects to it automatically when the page loads. See
[docs/API.md](API.md) for the full endpoint list.

To stop the server, Ctrl+C in the `dx serve` terminal. To reset
ministack state:

```bash
docker compose down -v
docker compose up -d
```

---

## Linting and formatting

yard uses the stock Rust toolchain — no custom `rustfmt.toml` or
`clippy.toml`. Default settings apply.

| Tool | Command | Enforcement |
|------|---------|-------------|
| `rustfmt` | `cargo fmt --all` | CI runs `cargo fmt --all -- --check`; fails the PR if output differs. |
| `clippy` | `cargo clippy --all-targets -- -D warnings` | CI runs the same command; **every warning is an error**. |

CI is defined in `.github/workflows/ci.yml` and runs on every pull
request targeting `main`. The workflow posts a PR comment summarizing
formatting, clippy, and test results. Any red check blocks merge.

Run both locally before you push:

```bash
cargo fmt --all
cargo clippy --all-targets -- -D warnings
cargo test
```

---

## Coding rules

These are the hard rules in `CLAUDE.md` — they apply to humans and
LLM-assisted contributions alike.

- **Never modify `Cargo.toml` without asking first.** If you need a new
  crate dependency, or want to bump a version, open an issue or ask
  before making the change. This applies to all four workspace
  `Cargo.toml` files and the root workspace manifest.
- **Never bump versions unless explicitly asked.** Don't preemptively
  bump `version = "x.y.z"` in any crate's `Cargo.toml`. Release
  versioning is a deliberate act.
- **`unwrap()` is fine in tests, never in production code.** Inside
  `#[cfg(test)]`, `#[test]`, `tests/`, or `#[tokio::test]` blocks,
  `unwrap()` / `expect()` are acceptable. Anywhere else, use `?` with
  `anyhow::Context` or a real `match`.
- **`unsafe {}` never, anywhere.** There is no `unsafe` block anywhere
  in the workspace today, and there shouldn't be. If you think you need
  one, you don't — ask first.
- **Every PR must pass `cargo clippy --all-targets -- -D warnings` with
  zero issues.** CI enforces this.
- **Prefer stdlib over adding crates for simple tasks.** Date parsing,
  string manipulation, small parsers — reach for `std` first. New
  dependencies need a justification.
- **All logic in `yard-core`; the CLI just parses args and displays.**
  `yard-cli/src/commands/*.rs` files should be 20–80 lines each:
  resolve the project, call a `yard_core::*` function, format the
  result. If you find yourself writing a loop over jobs in a command
  file, it belongs in `yard-core`.
- **Never hardcode GitHub handles or repo names as defaults.** In
  particular, `yard-server` must take `YARD_REPO_OWNER` /
  `YARD_REPO_NAME` from the environment; never bake a default org or
  repo into source. Same applies to example YAML shipped in the repo.

---

## Adding a new provider

Providers are AWS services (or any remote deploy target) that yard
uploads generated scripts to. Glue and EMR are implemented today;
Databricks and EMR Serverless are planned.

### 1. Define the struct and trait impl

Add a new file under `yard-core/src/providers/`, e.g.
`yard-core/src/providers/databricks.rs`. The `Provider` trait lives in
`yard-core/src/providers/mod.rs`:

```rust
pub trait Provider: Send + Sync {
    fn deploy(
        &self,
        job_name: &str,
        artifact: &str,
        job_config: &Value,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Resource>>> + Send + '_>>;

    fn destroy(
        &self,
        job_name: &str,
        resources: &[Resource],
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>>;

    fn verify_resources(
        &self,
        job_name: &str,
        resources: &[Resource],
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceStatus>>> + Send + '_>>;
}
```

Implement all three methods. `deploy` returns the `Resource`s it
created/updated so `yard-core` can track them in state. `verify_resources`
is called by drift detection to confirm those resources still exist in
the target service.

Look at `yard-core/src/providers/glue.rs` for a reference implementation
that covers all three methods, uses the shared `S3ScriptOps` helper for
script uploads, and parses provider config via `serde_json::Value`
lookups.

### 2. Register the file

In `yard-core/src/providers/mod.rs`, add:

```rust
pub mod databricks;
```

### 3. Register in the `get_provider` dispatch

Still in `yard-core/src/providers/mod.rs`, extend the `match` in
`get_provider`:

```rust
pub async fn get_provider(job_type: &str, provider_config: &Value) -> Result<Box<dyn Provider>> {
    match job_type {
        "glue" => Ok(Box::new(glue::GlueProvider::new(provider_config).await?)),
        "emr"  => Ok(Box::new(emr::EmrProvider::new(provider_config).await?)),
        "databricks" => Ok(Box::new(databricks::DatabricksProvider::new(provider_config).await?)),
        other => Err(anyhow!("No provider for job type: {other}")),
    }
}
```

This is the only dispatch point — orchestration, diff, storage, and
codegen already work polymorphically against `Box<dyn Provider>`.

### 4. Handle codegen

If the new provider needs a generated PySpark script, add a new Tera
template under `yard-core/src/templates/` (mirror `glue.py.tera` or
`emr.py.tera`) and extend `generate_python_script` in
`yard-core/src/codegen/mod.rs` to dispatch on the new `job_type`.

If the provider doesn't need codegen (task-only types like Airflow's
`bash` operator), have `generate_python_script` return an empty string
for the new type — the provider's `deploy` method won't receive an
artifact to upload.

See [docs/CODEGEN.md](./CODEGEN.md) for the full codegen reference —
template context variables, how source/transform/sink dispatch works,
and where to wire a new source/sink/transform type.

### 5. Document and test

- Add provider-defaults docs to `docs/CONFIGURATION.md` under the
  `providers.<type>` section.
- Add unit tests in the new `providers/<name>.rs` file (the existing
  providers use `#[cfg(test)]` modules with mocked `serde_json::Value`
  configs).
- Update the provider status table in `README.md`.

No changes to existing providers are needed.

---

## Adding a new CLI command

CLI commands are wired in two places: the `clap` derive in
`yard-cli/src/parser.rs`, and a per-command module under
`yard-cli/src/commands/`.

### 1. Add the variant to the `Commands` enum

In `yard-cli/src/parser.rs`, add a new variant to the `Commands` enum:

```rust
#[derive(Subcommand)]
pub enum Commands {
    // ... existing variants ...

    /// Describe what the command does (shown in --help)
    MyCommand {
        #[arg(index = 1)]
        directory: Option<String>,

        /// Some flag
        #[arg(long)]
        my_flag: bool,
    },
}
```

Follow the existing style:
- Positional `directory: Option<String>` as the last positional arg
  (the convention is "optional path to the project root").
- `#[arg(long)]` boolean flags for switches.
- Doc comments on the variant and each field — `clap` surfaces them in
  `--help`.

### 2. Create the command module

Add a new file `yard-cli/src/commands/my_command.rs` with an `execute`
function that takes the parsed args and does the CLI work:

```rust
use super::resolve_project;
use anyhow::Result;

pub async fn execute(directory: Option<String>, my_flag: bool) -> Result<()> {
    let project = resolve_project(directory).await?;

    // Call yard_core::* here. DO NOT put business logic in this file.
    let result = yard_core::some_function(&project.manifest, my_flag).await?;

    // Format and print.
    println!("{:?}", result);

    Ok(())
}
```

The existing `commands/*.rs` files (especially `plan.rs` and
`show.rs`) are small and good references. `resolve_project` is a helper
in `commands/mod.rs` that converts the optional directory arg into a
`ResolvedProject`.

### 3. Register the module

In `yard-cli/src/commands/mod.rs`:

```rust
pub mod my_command;
```

### 4. Dispatch in `run()`

In `yard-cli/src/lib.rs`, add a match arm inside `run()`:

```rust
parser::Commands::MyCommand { directory, my_flag } => {
    commands::my_command::execute(directory, my_flag).await?
}
```

### 5. Verify

```bash
cargo run -p yard -- my-command --help
cargo fmt --all
cargo clippy --all-targets -- -D warnings
cargo test
```

If the real logic lives in `yard-core`, add tests there. The command
file itself is too thin to warrant its own tests. See
[TESTING.md](TESTING.md) for test conventions.
