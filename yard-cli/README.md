<!-- generated-by: gsd-doc-writer -->
# yard-cli

Thin command-line wrapper for YARD. Parses arguments with [clap](https://crates.io/crates/clap),
delegates to `yard-core` for all business logic, and formats the result for the terminal.

Part of the [yard](../README.md) workspace. See the root [README](../README.md) and
[docs/ARCHITECTURE.md](../docs/ARCHITECTURE.md) for full project context.

## Role in the workspace

`yard-cli` produces the `yard` binary. It is intentionally a thin wrapper:

- **Parse:** turn `argv` into a typed `Commands` enum (see `src/parser.rs`).
- **Delegate:** hand off to `yard-core` — every subcommand is a small `execute()` function
  in `src/commands/` that calls into core and awaits a result.
- **Display:** print output, respecting `--no-color` / `--colorblind` and the [NO_COLOR](https://no-color.org/) env var.

**No business logic lives in this crate.** Codegen, providers, state management, validation, and DAG
generation all live in `yard-core`. If you find yourself reaching for `serde_yaml` or AWS SDKs inside
`yard-cli`, you are in the wrong crate.

## Dependencies

Internal:

- [`yard-core`](../yard-core) — all business logic (invoked from each command handler).
- [`yard-structs`](../yard-structs) — shared data types (config, state, diffs).

External (see `Cargo.toml` for exact versions):

- `clap` (derive) — argument parsing.
- `tokio` — async runtime for the `#[tokio::main]` entry point.
- `anyhow` — error propagation to the top-level `run()` function.

## Layout

```
yard-cli/
  src/
    main.rs         # #[tokio::main] entry — calls yard::run()
    lib.rs          # run() — parses CLI, dispatches to commands
    parser.rs       # clap Cli/Commands structs
    context.rs      # shared CLI-side context helpers
    utils.rs        # color / output helpers (disable_color, colorblind)
    commands/
      mod.rs
      init.rs       # yard init
      plan.rs       # yard plan
      apply.rs      # yard apply
      show.rs       # yard show <job>
      validate.rs   # yard validate
      destroy.rs    # yard destroy [job]
      force_unlock.rs # yard force-unlock <job>
```

Each file in `commands/` exposes a single `pub async fn execute(...)` that takes the parsed
arguments and returns `anyhow::Result<()>`. Add new subcommands by:

1. Adding a variant to `Commands` in `parser.rs`.
2. Adding a module under `commands/` with an `execute()` function.
3. Wiring the match arm in `lib.rs::run()`.

## Build & run

From the workspace root:

```bash
cargo build --release -p yard
# binary at target/release/yard
```

For development commands (lint, test, format) see [docs/DEVELOPMENT.md](../docs/DEVELOPMENT.md).

## License

Business Source License 1.1. See [LICENSE](../LICENSE).
