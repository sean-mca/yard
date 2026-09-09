<!-- generated-by: gsd-doc-writer -->
# Development

This document is for contributors working on yard itself (adding providers,
fixing bugs in `yard-core`, etc.). If you are
trying to *use* yard to deploy data jobs, start with
[quickstart](../quickstart.md) instead.

- [Repo layout](#repo-layout)
- [Local dev setup](#local-dev-setup)
- [Building](#building)
- [Running the CLI locally](#running-the-cli-locally)
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
members = ["yard-cli", "yard-core", "yard-structs", "yard-plugin-sdk"]
```

| Crate | Role |
|-------|------|
| `yard-cli` (package name `yard`, produces the `yard` binary) | Thin CLI wrapper — parses `clap` args, delegates to `yard-core`, formats output. No business logic. The `list` subcommand (v1.3.4) follows this rule by calling `yard_core::list_targets::list_targets(&manifest, &root_dir)` and formatting the rows. |
| `yard-core` | Library. All host-side logic: config resolution, plugin host, storage, validation, diff, orchestration. The `StorageBackend` trait at `yard-core/src/storage.rs` gives backends a single `Box<dyn StorageBackend>` interface (Local + S3 today; DynamoDB / GCS / etc. plug in by adding an impl). v2.0 deleted the compiled-in providers, the codegen module and the Airflow DAG modules, and added `plugin_host/` (download, spawner, provider). |
| `yard-structs` | Shared `serde` types (`ProjectManifest`, `JobDefinition`, `JobState`, `JobDiff`, `StateBackend`, `Resource`, `Trigger`, …). Minimal deps — `serde`, `anyhow`, `serde_json`. |
| `yard-plugin-sdk` | Library. Published SDK for provider-plugin authors — the `PluginHandler` trait, `PluginServer` run loop, fd-level stdout protection, and re-exports of the protocol types from `yard-structs`. |

The dependency graph is strict: `yard-cli` depends on
`yard-core`; `yard-core` depends on `yard-structs`; `yard-structs`
depends on nothing inside the workspace. `yard-core` never depends on
`yard-cli`.

For the full breakdown — component diagrams, data flow through a
`yard apply`, and per-directory descriptions — see
[architecture](../explanation/architecture.md).

---

## Local dev setup

### Prerequisites

| Tool | Version | Notes |
|------|---------|-------|
| Rust toolchain | `stable` | CI uses `dtolnay/rust-toolchain@stable` with `clippy` and `rustfmt` components. No `rust-toolchain.toml` is pinned in the repo. |
| Cargo | Bundled with `rustup` | Workspace uses `resolver = "3"` (Edition 2024), which requires a recent enough toolchain. |
| Docker + Docker Compose | Any recent version | Only needed for the opt-in `ministack`-backed integration tests (S3 emulator). |

### Clone and install dependencies

```bash
git clone https://github.com/sean-mca/yard.git
cd yard
cargo build --workspace
```

The first build is slow (the AWS SDK has a large dependency tree);
subsequent builds are incremental. Cargo will download and compile all
workspace members and their dependencies.

### Starting ministack (only needed for integration tests)

`docker-compose.yml` provisions two services:

- **`ministack`** — Localstack-like S3 emulator on `localhost:4566`.
- **`init-aws`** — One-shot container that creates the `yard-state`
  S3 bucket.

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
| `cargo test` | Run the full test suite. See [testing](testing.md) for details. |
| `cargo clippy --all-targets -- -D warnings` | Lint. Must be clean before any PR. |
| `cargo fmt --all` | Format. Must be clean before any PR. |
| `cargo fmt --all -- --check` | CI formatting gate (doesn't modify files). |

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

See [configuration reference](../reference/configuration.md) for every env var yard respects.

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
  read them from configuration or the environment; never bake a default
  org or repo into source. Same applies to example YAML shipped in the repo.

---

## Adding a new provider

As of v2.0, providers are **not** added to this repo. A provider is a
standalone plugin binary that yard downloads and talks to over JSON-over-stdio,
so adding one means creating a separate crate and release — no changes to
`yard-core` at all.

The full walkthrough lives in
[docs/how-to/build-a-plugin.md](../how-to/build-a-plugin.md): the Rust SDK
tutorial (`yard-plugin-sdk` + the `PluginHandler` trait), local testing against
the plugin cache, the release/naming convention, and the raw JSON protocol spec
for plugins written in other languages.

What *does* live in this repo:

- `yard-plugin-sdk/` — the SDK plugin authors depend on. Changes here are a
  published API change; bump deliberately.
- `yard-structs/src/plugin.rs` — the protocol types shared by host and SDK. A
  change to these is a protocol change and must bump `protocol_version`.
- `yard-core/src/plugin_host/` — the host side: download/caching, process
  lifecycle, checksum verification, and the `Provider` impl.

If you are extending the *protocol* (a new operation, a new response field),
touch all three, keep the handshake's `protocol_version` in sync, and add
coverage to `yard-core/tests/plugin_integration.rs` and
`yard-core/tests/sdk_plugin_integration.rs`.

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
[testing](testing.md) for test conventions.
