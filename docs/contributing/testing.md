<!-- generated-by: gsd-doc-writer -->
# Testing

This document is for contributors running and writing tests inside the yard
workspace. It covers the test taxonomy, how to invoke tests at different
scopes, per-crate test layout, the in-memory test harness used by
`yard-server`, the integration-test setup against the `ministack` docker
container, and how CI invokes the suite.

For general developer setup, see [development](development.md).

---

## Test framework

yard uses the **built-in Rust test harness** — `cargo test` — exclusively.
No third-party test runners or snapshot frameworks are wired in.

| Tool            | In use? | Notes                                                            |
|-----------------|---------|------------------------------------------------------------------|
| `cargo test`    | Yes     | Standard Rust harness; all tests run through it.                 |
| `tokio::test`   | Yes     | For async tests (`yard-server`, async providers). Enabled via the `tokio = { version = "1.50.0", features = ["full"] }` dep already in every crate that needs it. |
| `tempfile`      | Yes     | Present in `Cargo.lock` for tests that need scratch directories. |
| `insta` / snapshot tests | No | Not present in `Cargo.lock`. No golden-file fixtures exist in the tree. |
| `mockito` / `wiremock` | No | Not present in `Cargo.lock`. HTTP mocks are avoided in favour of the in-process in-memory DB and `ministack` for AWS. |
| `rstest` / `proptest` | No | Not wired in.                                                   |
| `cargo-nextest` | No      | Not required; `cargo test` is what CI runs.                      |

No setup is required beyond what is needed to build the workspace.
`cargo test` compiles tests on first invocation and caches them in `target/`.

## Test taxonomy in this repo

There are three kinds of tests in the workspace:

1. **In-crate unit tests** — the default Rust pattern: a `#[cfg(test)] mod tests { ... }` block at the bottom of each source file. These are the bulk of the suite and cover every crate.
2. **Integration tests in `yard-core/tests/`** — two files (`emr_integration.rs`, `glue_integration.rs`) that exercise the real AWS SDK against a local `ministack` container. All of these tests are marked `#[ignore]` so they are skipped by default; they only run when you explicitly pass `-- --ignored` and have `ministack` up.
3. **Server test harness** — `yard-server` ships a `test_support` module at `yard-server/src/db/mod.rs` that provides an `InMemoryDb` implementation of the `Database` trait, used by unit tests in the `api::*` and `alerting::*` modules.

There are **no** end-to-end tests, no snapshot/golden-file fixtures, and
no HTTP integration tests that spin up a live Axum server. The WebSocket
module tests the event plumbing (`broadcast` channel round-trip and type-level
`events_router()` construction) but does not open real sockets.

## Running tests

### Full suite

```bash
cargo test
```

This runs every `#[test]` and `#[tokio::test]` in every crate **except**
the `#[ignore]`-marked integration tests in `yard-core/tests/`. This is
what CI runs.

### Single crate

```bash
cargo test -p yard-core
cargo test -p yard-server
cargo test -p yard-cli
cargo test -p yard-structs
```

Note that the CLI crate is registered under the package name `yard`
(see `yard-cli/Cargo.toml`), so you can also run `cargo test -p yard`.

### Single test file or module

```bash
# All tests in yard-core's diff module
cargo test -p yard-core diff::

# A single test by name substring
cargo test -p yard-server test_insert_and_list_webhook_events

# All tests in a specific integration-test file
cargo test -p yard-core --test glue_integration
```

`cargo test` matches on a substring of the full test path, so a fragment
of the function name or module path is enough.

### With test output visible

By default the harness captures stdout from passing tests. To see it:

```bash
cargo test -- --nocapture
```

Combine with `--test-threads=1` if interleaved output is a problem:

```bash
cargo test -- --nocapture --test-threads=1
```

### Ministack-backed integration tests (opt-in)

The two files in `yard-core/tests/` (`emr_integration.rs`,
`glue_integration.rs`) talk to the AWS SDK against a local `ministack`
container declared in `docker-compose.yml`. Every test in those files is
`#[ignore]`d so they never run in the default suite.

To run them:

```bash
# 1. Bring up ministack on localhost:4566
docker compose up -d

# 2. Run the Glue suite
AWS_ENDPOINT_URL=http://localhost:4566 \
AWS_ACCESS_KEY_ID=test \
AWS_SECRET_ACCESS_KEY=test \
AWS_DEFAULT_REGION=us-east-1 \
cargo test -p yard-core --test glue_integration -- --ignored --nocapture

# 3. Or the EMR suite
AWS_ENDPOINT_URL=http://localhost:4566 \
AWS_ACCESS_KEY_ID=test \
AWS_SECRET_ACCESS_KEY=test \
AWS_DEFAULT_REGION=us-east-1 \
cargo test -p yard-core --test emr_integration -- --ignored --nocapture
```

The exact invocation is duplicated as a doc comment at the top of each
integration file.

## Per-crate test layout

### `yard-structs`

No tests. This crate is pure serde data types with no logic to exercise.

### `yard-cli`

Unit tests live alongside the source in `#[cfg(test)] mod tests { ... }`
blocks. Notable modules with tests:

- `yard-cli/src/utils.rs` — argument / path helpers
- `yard-cli/src/context.rs` — CLI context construction

The CLI is a thin wrapper by design; the bulk of logic-level testing lives
in `yard-core`.

### `yard-core`

In-crate unit tests across every module that contains logic:

- `src/config_merge.rs`, `src/parsing.rs`, `src/resolve.rs` — YAML merge + resolution
- `src/diff.rs`, `src/orchestrate.rs`, `src/dag_lifecycle.rs` — plan/apply/DAG lifecycle
- `src/storage.rs`, `src/utils.rs`, `src/validation/mod.rs`
- `src/codegen/mod.rs`, `src/airflow_dag/mod.rs` — PySpark and Airflow DAG codegen

The `tests/` directory holds **only** the two `#[ignore]`d AWS-integration
files described above.

### `yard-server`

In-crate unit tests in nearly every module:

- `src/api/drift.rs`, `src/api/dashboard.rs`, `src/api/settings.rs`, `src/api/events.rs`, `src/api/error.rs` — HTTP handler tests that drive axum extractors (`State`, `Query`, `Json`) directly and assert on the `IntoResponse` output
- `src/github/webhook.rs`, `src/github/router.rs` — GitHub webhook signature verification, action routing, and WebSocket event-emission contract tests
- `src/alerting/slack.rs`, `src/alerting/threshold.rs` — Slack payload shape + threshold/cooldown decision logic
- `src/db/mod.rs` — unit tests for the `InMemoryDb` implementation of the `Database` trait
- `src/main.rs` — a single test for `required_env`

## yard-server test harness

Because the production database is DynamoDB, `yard-server` defines an
in-process test double that implements the same trait. It lives at:

`yard-server/src/db/mod.rs`, inside `#[cfg(test)] pub mod test_support`.

```rust
use crate::db::test_support::InMemoryDb;
use crate::db::Database;
use std::sync::Arc;

let db: Arc<dyn Database> = Arc::new(InMemoryDb::new());
```

`InMemoryDb` is a `Mutex`-wrapped collection of `Vec`s and `HashMap`s that
implements every method on the `Database` trait — webhooks, plan results,
drift snapshots, settings, and cache. It has no persistence, no network,
and no TTLs; fixtures vanish when the test ends.

### Constructing an `ApiState` for handler tests

Every axum handler takes `State<Arc<ApiState>>`, so the standard handler-test
pattern in this repo is to build a fake state from the in-memory DB and a
loose `broadcast` channel:

```rust
fn test_state() -> Arc<ApiState> {
    let db = Arc::new(InMemoryDb::new());
    let (event_tx, _rx) = tokio::sync::broadcast::channel(16);
    Arc::new(ApiState {
        github_token: "t".into(),
        repo_owner: "o".into(),
        repo_name: "r".into(),
        db: db as Arc<dyn Database>,
        event_tx,
    })
}
```

You will find this helper copy-pasted into `api/drift.rs`, `api/dashboard.rs`,
and `api/settings.rs`. Keep it aligned when you add fields to `ApiState`.

### WebSocket / broadcast tests

There is no live WebSocket test harness. The event bus is exercised in two
ways:

1. **Round-trip through the broadcast channel** — `new_event_channel()`
   returns a `(Sender, Receiver)` pair; tests send an `Event` on the
   sender and `recv` on the receiver under a `tokio::time::timeout` guard.
   See `broadcast_round_trip_delivers_event` in `src/api/events.rs`.
2. **Type-level router construction** — `events_router_compiles_with_api_state`
   in the same file constructs `events_router(state)` with a fake `ApiState`
   to prove the `WebSocketUpgrade`/`State`/`broadcast::Sender` type graph
   still links. If that test compiles, the router builds.

End-to-end WebSocket tests (real socket, real client) are intentionally out
of scope; see the comment on `events_router_compiles_with_api_state` for
rationale.

### GitHub webhook signature tests

`src/github/webhook.rs` builds `HeaderMap`s by hand, computes
`sha256=` HMAC signatures with the test secret, and asserts on the parsed
`WebhookAction`. No HTTP server is started.

## Coverage requirements

**No coverage threshold is configured.** There is no `tarpaulin`, `grcov`,
or `cargo-llvm-cov` configuration in the repo, and CI does not enforce a
coverage floor. The CI bar is green tests + zero clippy warnings + formatting.

## CI integration

CI runs on every pull request to `main` via `.github/workflows/ci.yml`
(job name `Build & Test`). The job runs three checks in sequence on
`ubuntu-latest` with the stable Rust toolchain:

| Step               | Command                                  |
|--------------------|------------------------------------------|
| Check formatting   | `cargo fmt --all -- --check`             |
| Run clippy         | `cargo clippy --all-targets -- -D warnings` |
| Run tests          | `cargo test`                             |

Each step uses `continue-on-error: true` and then posts a single summary
comment to the PR with a pass/fail table and collapsed logs for any failed
step. A final step fails the job if any of formatting, clippy, or tests
failed.

Because CI runs plain `cargo test`, only the default-enabled tests run —
the `#[ignore]`d `ministack` integration tests never execute in CI. If you
change or add integration tests, run them locally against `ministack`
before asking for review.

## Coding rules for tests

From `CLAUDE.md`:

- **`unwrap()` is fine in tests**, never in production code. Tests can and
  do use `.unwrap()`, `.expect(...)`, and panics freely.
- **`unsafe {}` is never permitted**, including in tests.
- **`cargo clippy -D warnings` must pass** including on test code
  (`--all-targets` in CI covers tests). Lint-fix test code like you would
  production code.

## How to add a new test

### Adding a unit test to an existing module

1. If the file does not already have a `#[cfg(test)] mod tests { ... }`
   block, append one at the bottom.
2. Inside it, write a `#[test]` function for synchronous logic or
   `#[tokio::test]` for async logic.
3. Prefer small, focused tests. Reuse `Default::default()` and local helper
   constructors (see `make_job`, `make_webhook`, `drift(count)` throughout
   the codebase) instead of repeating fixture setup.
4. Run `cargo test -p <crate>` to confirm.

### Adding a test for a new CLI command

1. Put all command logic in `yard-core` first — the CLI crate exists only
   to parse args and print. Unit-test the core logic in the corresponding
   `yard-core/src/*.rs` module.
2. If the command needs CLI-level parsing or context assembly covered,
   extend the tests in `yard-cli/src/context.rs` or `yard-cli/src/utils.rs`.

### Adding a test for a new provider

Providers live in `yard-core/src/providers/{glue.rs, emr.rs, ...}`. For a
new provider:

1. Add provider-level unit tests directly in the new provider module
   (`#[cfg(test)] mod tests { ... }`), exercising pure logic — config
   parsing, diff computation, resource shape — without the AWS SDK.
2. Pattern-match on the existing provider unit tests (see `glue.rs`,
   `emr.rs`) for fixture shape.
3. If you want to exercise the real AWS SDK paths, add a new file
   `yard-core/tests/<provider>_integration.rs` modelled on
   `glue_integration.rs`:
    - Point at `http://localhost:4566` (ministack).
    - Mark every test `#[ignore]` so it is excluded from `cargo test` by default.
    - Document the exact opt-in invocation in a doc comment at the top of the file.
4. Update `docker-compose.yml` if the provider needs a new ministack-side
   resource (bucket, table, IAM role).

### Adding a test for a new `yard-server` API handler

1. Add the handler to `src/api/*.rs` as usual.
2. In the same file, inside `#[cfg(test)] mod tests { ... }`, copy the
   local `test_state()` helper from `src/api/drift.rs` or
   `src/api/dashboard.rs`.
3. Call the handler directly with `State(state)` and any extractor wrappers
   (`Query(...)`, `Json(...)`) it takes.
4. Assert on either the strongly-typed return value or, for error paths,
   the `StatusCode` you get from `.into_response()`.
5. If the handler emits events, subscribe to the broadcast receiver
   returned by `test_state()` (do not drop the `_rx`) and `recv()` under a
   `tokio::time::timeout`.
