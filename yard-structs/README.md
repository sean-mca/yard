<!-- generated-by: gsd-doc-writer -->
# yard-structs

Shared data types for the [yard](../README.md) workspace — the foundational
crate that `yard-core`, `yard-cli`, and `yard-server` all depend on.

Part of the yard monorepo. See the [root README](../README.md) for an overview
and [docs/ARCHITECTURE.md](../docs/ARCHITECTURE.md) for how this crate fits
into the overall design.

## Purpose

`yard-structs` holds the serde-derived data types that move between layers:
what's written to `yard.yaml`, what's persisted to state backends, and what
flows through plan/apply diffs. No business logic lives here — just types,
their serde attributes, and trivial `Display` impls.

By isolating the type surface in its own crate, `yard-core` (providers,
codegen, DAG generation) and `yard-server` (webhooks, drift detection,
dashboard) can share a single definition of config, state, and diff shapes
without depending on each other.

## Minimal dependencies

This crate is intentionally kept lean. Only three dependencies are allowed:

```toml
serde = { version = "1", features = ["derive"] }
serde_json = "1"
anyhow = "1"
```

Any additional dependency needs explicit approval — pulling heavier crates
into `yard-structs` would propagate to every consumer in the workspace.

## Module layout

The crate is organized into four modules, all re-exported at the root via
`pub use`:

- **`config`** — Types parsed from `yard.yaml` and its hierarchical
  overrides. Covers `ProjectManifest`, `JobDefinition`, `Source`, `Sink`,
  `Transform`, `Import`, `OrderBySpec`, `StateBackend`, `YARDContext`,
  `AirflowSection`, and `AirflowJobBlock`.
- **`state`** — Types persisted to the state backend (local filesystem or
  S3). Covers `Resource`, `ResourceStatus`, `Deployment`, `ProjectState`,
  `JobState`, `LockInfo`, `DagDeployment`, and `DagState`.
- **`diff`** — Plan/apply diff types used by both the CLI and the server's
  drift detector. Covers `DiffType` (`Create` / `Modify` / `Delete`),
  `JobDiff`, and `DagDiff`.
- **`validation`** — `ValidationError` with field + message, used
  throughout the workspace to report config errors with source location.

## Usage

Add it to another workspace crate via path dependency:

```toml
[dependencies]
yard-structs = { path = "../yard-structs" }
```

Then pull types in from the crate root:

```rust
use yard_structs::{JobDefinition, JobState, JobDiff, ValidationError};
```

## License

See the workspace [LICENSE](../LICENSE) file.
