<!-- generated-by: gsd-doc-writer -->
# yard-server

The server side of [yard](../README.md) — an Atlantis-like service for
YAML-driven data pipelines. It receives GitHub pull-request webhooks,
runs `yard` plans against PR branches, detects drift between committed
configs and deployed state, and ships a Dioxus-fullstack dashboard UI.

Part of the [yard](../README.md) workspace.

## What it does

- **GitHub webhook receiver** — validates HMAC signatures and reacts to
  `pull_request` / `issue_comment` events, the same PR-driven workflow
  Atlantis uses for Terraform.
- **Drift detection** — a background poll loop compares deployed
  infrastructure to the YAML job definitions committed in the target repo
  and flags drifted jobs.
- **Drift threshold alerting** — fires Slack webhooks when the number of
  drifted jobs crosses a configurable threshold (with cooldown).
- **Dashboard UI** — a Dioxus-fullstack app (`/`, `/jobs`, `/drift`,
  `/settings`) served from the same binary as the API.
- **WebSocket live updates** — pushes drift / job / webhook events to the
  UI so the dashboard reflects changes without polling.
- **DynamoDB persistence** — single-table design storing webhook events,
  job runs, drift snapshots, and user settings.

## Module layout

Explore `src/` for details. Top-level modules:

| Module       | Purpose                                                                 |
| ------------ | ----------------------------------------------------------------------- |
| `api/`       | axum HTTP handlers: `dashboard`, `drift`, `jobs`, `settings`, `events` (WS). |
| `alerting/`  | Drift threshold logic (`threshold`) and Slack webhook sender (`slack`). |
| `db/`        | `Database` trait + `DynamoDatabase` implementation.                     |
| `github/`    | GitHub REST client, webhook signature verification, PR git ops, router. |
| `ui/`        | Dioxus components for dashboard, jobs, drift, settings, sidebar, etc.   |
| `types.rs`   | Shared types used by both server and UI sides.                          |
| `main.rs`    | Wires the router, spawns poll loops, binds the listener.                |

## Running locally

From the workspace root:

```bash
# 1. Start DynamoDB + S3 (ministack) for the backing store
docker-compose up -d

# 2. Export the required env vars (see env.local.example)
#    YARD_GITHUB_TOKEN, YARD_WEBHOOK_SECRET, YARD_REPO_OWNER, YARD_REPO_NAME
source env.local.example   # or set them however you prefer

# 3. Run the server
cargo run -p yard-server
```

The server binds to `0.0.0.0:3001` by default (override with
`YARD_PORT`). The dashboard is then available at
<http://localhost:3001>. For hot-reloading UI development, use
`dx serve` (Dioxus CLI) — the UI defaults to talking to
`http://127.0.0.1:3001` for its API calls.

## Required environment variables

These are checked at startup and the server will exit if any are missing:

- `YARD_GITHUB_TOKEN` — PAT or app token used by the GitHub client.
- `YARD_WEBHOOK_SECRET` — shared secret for webhook HMAC verification.
- `YARD_REPO_OWNER`, `YARD_REPO_NAME` — repo the server operates on.

See [../docs/CONFIGURATION.md](../docs/CONFIGURATION.md) for the full
list, including DynamoDB and alerting settings.

## Further reading

- [../docs/API.md](../docs/API.md) — HTTP endpoint reference.
- [../docs/DEPLOYMENT.md](../docs/DEPLOYMENT.md) — production deployment.
- [../docs/ARCHITECTURE.md](../docs/ARCHITECTURE.md) — how yard-server
  fits into the broader workspace.
- [../README.md](../README.md) — product overview and workspace layout.

## License

Part of the yard workspace — licensed under BSL 1.1. See
[../LICENSE](../LICENSE).
