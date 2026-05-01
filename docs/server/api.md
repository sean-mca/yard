<!-- generated-by: gsd-doc-writer -->
# API Reference

> **Stability: early / unstable.** `yard-server` is v0.2.x and its HTTP and
> WebSocket surface has no versioning guarantees yet. Routes, request shapes,
> response shapes, and event payloads may change without notice. Treat this
> document as a snapshot of the current code, not a stable contract.

This document describes the HTTP and WebSocket API exposed by `yard-server`
(the companion server binary in `yard-server/`). The CLI tool `yard` does not
expose an API — see the root [README.md](../../README.md) for CLI usage.

## Overview

`yard-server` is an [Axum](https://github.com/tokio-rs/axum)-based HTTP server
that exposes:

- **Dashboard read API** (`/api/dashboard`, `/api/dashboard/cached`) — PR list
  and job counts, pulled live from GitHub or from the DynamoDB cache.
- **Jobs API** (`/api/jobs`, `/api/jobs/file`) — enumerate job YAML files and
  fetch file contents from the tracked GitHub repository.
- **Drift API** (`/api/drift`, `/api/drift/cached`, `/api/drift/summary`) —
  on-demand and cached drift-check results plus a lightweight summary pulled
  from DynamoDB.
- **Settings API** (`/api/settings`) — key/value configuration store, used by
  the UI for theme, polling intervals, and Slack alert configuration.
- **Real-time events** (`/api/ws/events`) — WebSocket channel that broadcasts
  "refresh now" signals to connected UI clients.
- **GitHub webhook** (`/api/webhook/github`) — receives `pull_request` and
  `issue_comment` events to drive `yard plan` and `yard apply` on PRs.

The server binds to `0.0.0.0:${YARD_PORT}` (defaulting to `3001`), wraps the
whole router in a CORS layer (`Any` origin/method/headers), a Tower Governor
rate limiter (30 req/s with a burst of 60), and an HTTP tracing layer. Source:
`yard-server/src/main.rs::start_api_server`.

## Authentication

### HTTP endpoints (`/api/dashboard*`, `/api/jobs*`, `/api/drift*`, `/api/settings`)

**There is no authentication on these endpoints.** They are not protected by
bearer tokens, session cookies, OAuth, or any other client-side auth
mechanism — the router applies only CORS and rate-limiting middleware. The
server itself authenticates outbound to GitHub using a server-side Personal
Access Token (`YARD_GITHUB_TOKEN`), but incoming HTTP requests are accepted
from any origin.

This is a development-grade posture. Do not expose `yard-server` directly to
the public internet without fronting it with authentication at the reverse
proxy / ingress layer. <!-- VERIFY: deployment auth posture for production environments -->

### GitHub webhook (`/api/webhook/github`)

Webhook requests are authenticated via GitHub's HMAC-SHA256 request signing.
Every request must include:

| Header | Description |
|---|---|
| `X-Hub-Signature-256` | Required. Must be `sha256=<hex HMAC-SHA256 of body with YARD_WEBHOOK_SECRET>`. Requests without this header or with a bad signature are rejected with `401 Unauthorized`. |
| `X-GitHub-Event` | Required. One of `pull_request`, `issue_comment`, or any other event name (others are accepted but ignored). Missing header returns `400 Bad Request`. |

Source: `yard-server/src/github/webhook.rs::parse_webhook`.

### WebSocket (`/api/ws/events`)

The WebSocket upgrade handshake has no authentication. Any client that can
reach the server can subscribe. The rate limiter applies only to the upgrade
handshake; once upgraded, frames flow freely. Source:
`yard-server/src/api/events.rs::events_router`.

## Endpoints

| Method | Path | Description | Auth |
|---|---|---|---|
| GET | `/api/dashboard` | Live-fetch PR list + job counts from GitHub. | None |
| GET | `/api/dashboard/cached` | Paginated read from the DynamoDB dashboard cache. | None |
| GET | `/api/jobs` | List job YAML files in the tracked repo's HEAD commit. | None |
| GET | `/api/jobs/file?path=…` | Fetch the raw UTF-8 contents of a single file from the tracked repo. | None |
| GET | `/api/drift` | Run a full drift check (clone + resolve + diff + AWS verify). Expensive. | None |
| GET | `/api/drift/cached` | Return the most recent cached drift result. | None |
| GET | `/api/drift/summary` | Lightweight `{drifted, in_sync}` counts derived from DynamoDB snapshots. | None |
| GET | `/api/settings` | Return all key/value settings. | None |
| POST | `/api/settings` | Validate and persist a batch of settings. | None |
| GET | `/api/ws/events` | WebSocket upgrade for server-push refresh events. | None |
| POST | `/api/webhook/github` | GitHub webhook receiver (plan on PR, apply via comment). | HMAC-SHA256 |

All route registrations live in `yard-server/src/main.rs::start_api_server`,
and each sub-router is defined in its corresponding module
(`api/dashboard.rs`, `api/drift.rs`, `api/jobs.rs`, `api/settings.rs`,
`api/events.rs`, `github/router.rs`).

## Request / Response Formats

All HTTP request and response bodies are JSON unless noted otherwise. The
concrete structs are defined in `yard-server/src/types.rs` and derive `serde`
`Serialize`/`Deserialize` with the default (PascalCase) enum tagging.

### GET `/api/dashboard`

Live fetch from GitHub — expensive, and rate-limited by the GitHub API.

**Query parameters:**

| Param | Type | Default | Notes |
|---|---|---|---|
| `page` | `u32` | `1` | Clamped to `>= 1`. |
| `per_page` | `u32` | `15` | Clamped to `<= 50`. |

**Response:** `200 OK` with a `DashboardData`:

```json
{
  "prs": [
    {
      "number": 42,
      "title": "feat: add new job",
      "author": "octocat",
      "state": "Open",
      "plan_result": "Pass",
      "updated": "3 hours ago",
      "url": "https://github.com/org/repo/pull/42"
    }
  ],
  "open_prs": 3,
  "jobs_tracked": 12,
  "page": 1,
  "per_page": 15,
  "has_more": false
}
```

- `state` is one of `"Open"`, `"Merged"`, `"Closed"`.
- `plan_result` is one of `"Pass"`, `"Fail"`, `"Pending"`, `"None"`.

Errors: `502 Bad Gateway` if the GitHub API call fails.

### GET `/api/dashboard/cached`

Same query params as `/api/dashboard`. Reads the `dashboard` cache entry from
DynamoDB (written by the background `dashboard_poll_loop` and by webhook
handlers), then paginates in-process via `DashboardCache::paginate`.

Errors:
- `503 Service Unavailable` if the cache is empty or corrupt.
- `500 Internal Server Error` if the DynamoDB read fails.

### GET `/api/jobs`

No query parameters.

**Response:** `200 OK` with a `JobsData`:

```json
{
  "jobs": [
    {
      "name": "my-etl",
      "path": "glue/dev/us-east-1/my-etl.yaml",
      "environment": "dev",
      "region": "us-east-1"
    }
  ]
}
```

Errors: `502 Bad Gateway` if the GitHub tree fetch fails.

### GET `/api/jobs/file`

**Query parameters:**

| Param | Type | Required | Notes |
|---|---|---|---|
| `path` | `string` | yes | Repository-relative file path, e.g. `glue/dev/us-east-1/my-etl.yaml`. |

**Response:** `200 OK` with the raw UTF-8 file contents as the body (not JSON
— the handler returns `Result<String, ApiError>`).

Errors: `502 Bad Gateway` if the GitHub contents call fails, the base64 decode
fails, or the file is not valid UTF-8.

### GET `/api/drift`

Runs a full drift check synchronously. Clones the tracked repo at HEAD, runs
`yard-core`'s resolver and diff, verifies deployed resources against AWS, and
persists drift snapshots to DynamoDB before returning.

**Response:** `200 OK` with a `DriftData`:

```json
{
  "items": [
    {
      "name": "my-etl",
      "environment": "dev",
      "region": "us-east-1",
      "drift_type": "Modified",
      "fields_changed": ["role_arn", "timeout_minutes"],
      "old_config": null,
      "new_config": "name: my-etl\n..."
    }
  ],
  "in_sync": 11,
  "drifted": 1
}
```

- `drift_type` is one of `"Modified"`, `"New"`, `"Deleted"`, `"ResourceMissing"`.
- `ResourceMissing` indicates a job whose YAML matches state but whose deployed
  AWS resource no longer exists (out-of-band deletion).

Errors: `500 Internal Server Error` if the clone, resolve, diff, or AWS verify
step fails.

### GET `/api/drift/cached`

No parameters. Returns the most recent `DriftData` cached in DynamoDB under
the `drift` key (written by the background `drift_poll_loop` and by every
successful `/api/drift` call).

Errors:
- `503 Service Unavailable` if the cache is empty or corrupt.
- `500 Internal Server Error` if the DynamoDB read fails.

### GET `/api/drift/summary`

No parameters. Returns aggregate counts derived from the DynamoDB drift
snapshot table (latest snapshot per job, last 500 snapshots scanned):

```json
{
  "drifted": 1,
  "in_sync": 11
}
```

Errors: `500 Internal Server Error` if the DynamoDB scan fails.

### GET `/api/settings`

No parameters.

**Response:** `200 OK` with a `SettingsResponse`:

```json
{
  "settings": {
    "theme": "dark",
    "drift_interval": "5",
    "slack_enabled": "true"
  }
}
```

All values are strings — the API normalises everything to `HashMap<String, String>`.

### POST `/api/settings`

**Request body:**

```json
{
  "settings": {
    "theme": "dark",
    "drift_interval": "5"
  }
}
```

Every key in the payload is validated against an allowlist before any write.
If any key/value fails validation, the handler returns `400 Bad Request` and
no changes are persisted.

**Allowed keys and values** (from
`yard-server/src/api/settings.rs::validate_setting`):

| Key | Allowed values |
|---|---|
| `theme` | `"light"`, `"dark"`, `"system"` |
| `drift_interval` | `"1"`, `"3"`, `"5"`, `"10"` (minutes) |
| `dashboard_interval` | Any positive integer (minutes) |
| `slack_enabled` | `"true"`, `"false"` |
| `slack_webhook_url` | Any string (not validated as URL) |
| `alert_drift_threshold` | Any integer `>= 1` |
| `alert_cooldown_minutes` | Any integer `>= 1` |
| `alert_last_sent_at` | Any string (server-written; pass-through) |

**Response:** `200 OK` with an empty body on success.

Errors:
- `400 Bad Request` if any key is unknown or any value fails validation.
- `500 Internal Server Error` if a DynamoDB write fails.

### POST `/api/webhook/github`

GitHub webhook receiver. See [Authentication](#github-webhook-apiwebhookgithub)
for required headers. The body must be the raw JSON payload GitHub sends (the
HMAC is computed over those exact bytes).

**Accepted event types:**

| `X-GitHub-Event` | Action | Behaviour |
|---|---|---|
| `pull_request` with `action: "opened"` or `"synchronize"` | Plan | Clones the PR head SHA, runs `yard-core` resolve+diff, posts the plan output as a PR comment, persists a `plan_result` row, and emits `dashboard_refreshed` + `webhook_received` WS events. |
| `pull_request` with any other action | Ignore | Returns `200 OK`. |
| `issue_comment` with `action: "created"` and body exactly `"yard apply"` (case-insensitive, trimmed) | Apply | Resolves the PR head SHA via GitHub API, clones, runs `yard-core::apply`, posts the apply output as a PR comment, and emits the same WS events as Plan. |
| `issue_comment` with any other body or action | Ignore | Returns `200 OK`. |
| Any other event type | Ignore | Returns `200 OK`. |

**Response:**
- `200 OK` on success (plan/apply completed and comment posted).
- `200 OK` for ignored events.
- `401 Unauthorized` for missing/bad signature.
- `400 Bad Request` for missing `X-GitHub-Event` or malformed JSON.
- `500 Internal Server Error` if posting the PR comment fails or (for apply) if the PR head SHA cannot be resolved.

Source: `yard-server/src/github/router.rs::handle_webhook`,
`yard-server/src/github/webhook.rs::parse_webhook`.

## WebSocket Events

### Endpoint

`GET /api/ws/events` — HTTP upgrade to WebSocket. Any HTTP verb that includes
the WebSocket upgrade headers will work (`axum::routing::any`).

### Subscription model

There is no subscribe/unsubscribe protocol. Once the socket is upgraded, the
server immediately starts forwarding every `Event` broadcast on the internal
`tokio::sync::broadcast` channel to that client as a JSON text frame. The
broadcast channel has a capacity of `64` events
(`yard-server/src/api/events.rs::EVENT_CHANNEL_CAPACITY`); if a client lags
behind, the server sends a WebSocket `Close` frame and the client must
reconnect and re-fetch caches fresh.

Inbound client frames (Text / Binary / Ping / Pong) are ignored by design;
only `Close` is honoured. Do not attempt to send commands over the socket.

### Event payloads

Events are serialised with `serde(tag = "event", rename_all = "snake_case")`,
so every payload has an `"event"` discriminator plus variant-specific fields.

| Event | Payload | Emitted when |
|---|---|---|
| `drift_refreshed` | `{"event":"drift_refreshed"}` | Background drift poll loop completed a check successfully. |
| `drift_failed` | `{"event":"drift_failed","reason":"<sanitised string, max 200 chars>"}` | Background drift poll loop failed. |
| `dashboard_refreshed` | `{"event":"dashboard_refreshed"}` | Dashboard cache refresh succeeded (background poll or post-webhook). |
| `dashboard_failed` | `{"event":"dashboard_failed","reason":"<sanitised string, max 200 chars>"}` | Dashboard cache refresh failed. |
| `webhook_received` | `{"event":"webhook_received"}` | A GitHub webhook (plan or apply) finished processing — emitted after dashboard events. |
| `alert_sent` | `{"event":"alert_sent","drifted_count":<u32>}` | A Slack drift threshold alert was successfully posted. |

Failure `reason` strings are truncated to 200 Unicode characters with a
trailing `…`; they are intentionally short to avoid leaking long GitHub error
bodies or stack traces (`yard-server/src/api/events.rs::sanitize_reason`).

Clients should treat every event as a "refresh now" signal and re-fetch the
relevant cached endpoint (`/api/dashboard/cached`, `/api/drift/cached`) rather
than relying on the event payload to carry data.

## Error Response Shape

All `/api/*` HTTP endpoints use a common error envelope (see
`yard-server/src/api/error.rs`). Error bodies are JSON:

```json
{
  "error": "human-readable message",
  "status": 500
}
```

The `status` field mirrors the HTTP status code and is always present.

| `ApiError` variant | HTTP status | Meaning |
|---|---|---|
| `DatabaseError` | `500 Internal Server Error` | DynamoDB read/write failed. |
| `GitHubError` | `502 Bad Gateway` | Upstream GitHub API call failed (auth, rate limit, network). |
| `NotFound` | `404 Not Found` | Resource not found. (Defined but not currently returned by any handler.) |
| `BadRequest` | `400 Bad Request` | Invalid settings key/value or malformed payload. |
| `CacheUnavailable` | `503 Service Unavailable` | Cached data not yet populated or corrupt. |
| `Internal` | `500 Internal Server Error` | Unhandled server-side failure. |

The webhook endpoint (`/api/webhook/github`) is the exception: it returns
plain `StatusCode` values with no JSON body, because GitHub does not consume
response bodies.

## Rate Limits

A global rate limiter is applied to all routes (including the WebSocket
upgrade handshake, but not in-progress WebSocket frames):

- **30 requests per second** sustained
- **60 requests** burst capacity

Implemented via `tower_governor::GovernorLayer` in
`yard-server/src/main.rs::start_api_server`. Requests over the limit are
rejected by the layer before the handler runs.

There are no per-endpoint or per-client rate limits beyond this global cap.
