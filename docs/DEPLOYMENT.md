<!-- generated-by: gsd-doc-writer -->
# Deployment

This document is for **operators deploying `yard-server`** — the web
dashboard + GitHub webhook receiver defined in the `yard-server` crate. It
does *not* cover the `yard` CLI, which runs locally on developer / CI
machines and is not deployed as a service (see
[GETTING-STARTED.md](GETTING-STARTED.md) for CLI setup).

## Deployment topology

`yard-server` is a single Rust binary that:

- Binds HTTP on `0.0.0.0:<YARD_PORT>` (default `3001` — see `main.rs:142`).
- Receives GitHub webhooks at `POST /api/webhook/github`
  (`github/router.rs:30`).
- Serves the Dioxus dashboard + API (dashboard, jobs, drift, settings,
  WebSocket events — see [API.md](API.md) for the full route list).
- Runs two background tasks inside the same process:
  - `drift_poll_loop` — clones the configured GitHub repo, runs
    `yard_core::calculate_diff` + `verify_deployed_resources`, writes
    snapshots to DynamoDB, optionally posts Slack alerts
    (`main.rs:164-311`).
  - `dashboard_poll_loop` — refreshes the cached PR list
    (`main.rs:313-349`).
- Reads and writes a single DynamoDB table for all persistence
  (webhook events, plan results, drift snapshots, settings, cache —
  `db/dynamo.rs`).

```
      GitHub                       yard-server                      AWS
  +------------+   webhook     +------------------+   DynamoDB   +-----------+
  |  PR opened | ----POST----> | POST /api/webhook|  put/get/qry | yard_yard |
  |  "yard     |               |      /github     | -----------> |  table    |
  |   apply"   |               |                  |              +-----------+
  +------------+               |  drift_poll_loop |
                               |  (git clone +    |              +-----------+
  +------------+               |   yard-core      | --STS/Glue/  | Target    |
  |  Operator  | ---HTTPS----> |   diff/verify)   |   EMR/S3-->  | AWS acct  |
  |  (browser) | <-WebSocket-- |                  |              | (via CLI- |
  +------------+               |  Axum + Dioxus   |              |  authored |
                               |  :3001 / 0.0.0.0 |              |  IAM)     |
                               +------------------+              +-----------+
```

`yard-server` has **no separate database process to manage** — DynamoDB is
the only persistence layer, and it is expected to be a real AWS DynamoDB
table in production (or `ministack` locally — see below).

### What is out of scope

- **yard CLI deployment** — the CLI runs locally; there is nothing to
  deploy.
- **Deploying the target data-engineering infrastructure** (Glue jobs, EMR
  steps, Airflow DAGs) — that is what `yard apply` does, driven by
  `yard-server` in response to webhooks. The AWS permissions needed to
  *apply* that infrastructure are the same permissions the local CLI
  needs and live with the AWS credentials `yard-server` resolves; see the
  [AWS resources needed](#aws-resources-needed) section below.

## Docker Compose (local / self-host)

The `docker-compose.yml` at the repo root provisions a **local-development
stack only** — it spins up `ministack` (a LocalStack-style AWS emulator)
and seeds it with an S3 bucket and a DynamoDB table. It does *not* build
or run `yard-server` itself. Running `dx serve` (Dioxus) against this
stack is the expected local-dev workflow.

```bash
docker compose up -d
# ministack is reachable at http://localhost:4566
# Seeded resources:
#   s3://yard-state
#   DynamoDB table: yard_yard (PK, SK, GSI1 on GSI1PK/GSI1SK, PAY_PER_REQUEST)
```

The `init-aws` service in `docker-compose.yml` runs the single-table
`create-table` call — with partition key `PK` (string), sort key `SK`
(string), and one GSI named `GSI1` on (`GSI1PK`, `GSI1SK`), all
projection-type `ALL`, billing-mode `PAY_PER_REQUEST`. This mirrors what
`DynamoDatabase::migrate()` creates at runtime against real AWS
(`db/dynamo.rs:44-132`).

<!-- VERIFY: production container image. The repo does not ship a Dockerfile for yard-server. Operators are expected to build their own image (`cargo build --release --bin yard-server`) or run the binary directly under systemd / an orchestrator. -->

<!-- VERIFY: production deployment platform (ECS / Fargate / Kubernetes / EC2 / Fly.io / etc.). Not encoded anywhere in the repo. -->

## Required environment variables

All runtime env vars are read at server start. Four are hard-required —
the server exits with `"{name} must be set"` if any are missing or empty
(`main.rs:38-44, 74-77`).

### Required (server exits if unset or empty)

| Variable | Purpose |
|----------|---------|
| `YARD_GITHUB_TOKEN` | GitHub token used for posting PR comments (via `octocrab`) and for authenticating `git clone` of the target repo (injected via `http.extraheader` — see `github/git_ops.rs:10-22`). |
| `YARD_WEBHOOK_SECRET` | Shared secret validated against `X-Hub-Signature-256` on incoming webhooks (`github/webhook.rs:12-27`). Must match what GitHub is configured with. |
| `YARD_REPO_OWNER` | GitHub org/user that owns the watched repo. Used for the dashboard PR query + drift-check repo identity. |
| `YARD_REPO_NAME` | Repo name (without owner prefix). |

### Optional runtime env vars

| Variable | Default | Purpose |
|----------|---------|---------|
| `YARD_PORT` | `3001` | TCP port. Server binds `0.0.0.0:<port>`. |
| `YARD_DB_TABLE_PREFIX` | `yard` | Final table name is `{prefix}_yard` (e.g. default → `yard_yard`). |
| `YARD_DB_REGION` | `AWS_REGION` → `us-east-1` | Region for the DynamoDB client. |
| `YARD_DB_ENDPOINT_URL` | (unset) | **Must be unset in production** so the AWS SDK hits real AWS. Used for local dev against `ministack`. |
| `RUST_LOG` | `info` | `tracing-subscriber` filter directive. |

### AWS credentials

The DynamoDB client uses the standard AWS SDK credential chain (loaded
via `aws_config::defaults(...)` in `db/dynamo.rs:27-34`). Any of the
following works in production:

- IAM role attached to the compute (EC2 instance profile, ECS task role,
  EKS IRSA, Fargate task role).
- Static `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN`.
- `AWS_PROFILE` pointing at `~/.aws/credentials`.
- SSO / `AWS_WEB_IDENTITY_TOKEN_FILE`.

<!-- VERIFY: recommended production credential mechanism for the deployment platform you choose. For ECS/Fargate the standard is the task role; for EKS it is IRSA; for bare EC2 it is the instance profile. -->

### Compile-time env var

| Variable | Default | Notes |
|----------|---------|-------|
| `YARD_API_BASE` | `http://127.0.0.1:3001` | Baked into the Dioxus wasm bundle via `option_env!`. Production builds typically set `YARD_API_BASE=""` so the UI derives the host from `window().location()`. Changing it requires a rebuild of `yard-server`. |

See [CONFIGURATION.md](CONFIGURATION.md) for the full env var reference
(including the CLI-side vars the apply path also uses).

A working local-dev template is in `env.local.example`:

```sh
YARD_GITHUB_TOKEN=ghp_your_token_here
YARD_WEBHOOK_SECRET=your_webhook_secret
YARD_REPO_OWNER=your-org
YARD_REPO_NAME=your-repo
YARD_DB_ENDPOINT_URL=http://localhost:4566
YARD_DB_TABLE_PREFIX=yard
YARD_DB_REGION=us-east-1
AWS_ACCESS_KEY_ID=test
AWS_SECRET_ACCESS_KEY=test
AWS_DEFAULT_REGION=us-east-1
```

## AWS resources needed

### DynamoDB

One table with the following schema (created automatically by
`DynamoDatabase::migrate()` on first server start — `db/dynamo.rs:44-132`):

- **Table name**: `{YARD_DB_TABLE_PREFIX}_yard` (default `yard_yard`).
- **Primary key**: `PK` (S, HASH) + `SK` (S, RANGE).
- **GSI**: `GSI1` on `GSI1PK` (S, HASH) + `GSI1SK` (S, RANGE), projection
  `ALL`.
- **Billing**: `PAY_PER_REQUEST`.

The server **creates the table on startup if it doesn't exist** (the
`ResourceInUseException` case is treated as "already exists" — see
`db/dynamo.rs:119-128`). You do *not* need to pre-create it. You *do* need
the IAM permissions for `CreateTable` + `DescribeTable` the first time the
process runs, or pre-provision the table and grant only runtime
permissions.

#### IAM permissions for yard-server (DynamoDB runtime)

Grepping `db/dynamo.rs` for AWS SDK calls shows these DynamoDB operations
are actually invoked at runtime:

- `CreateTable` — startup-only, only if the table does not exist.
- `DescribeTable` — startup-only, as part of `wait_for_table_active`.
- `PutItem` — writing webhook events, plan results, drift snapshots,
  settings, cache entries.
- `GetItem` — reading individual settings and cache entries.
- `Query` — listing webhook events by PR, plan results, drift snapshots.

A minimal steady-state IAM policy (once the table exists) looks like this:

<!-- VERIFY: IAM policy shape below. This is a reasonable starting point derived from the SDK calls the code makes, but has not been applied against a production deployment in this repo. Tighten `Resource` to the specific table/region ARN. -->

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "YardServerDynamoDB",
      "Effect": "Allow",
      "Action": [
        "dynamodb:DescribeTable",
        "dynamodb:PutItem",
        "dynamodb:GetItem",
        "dynamodb:Query"
      ],
      "Resource": [
        "arn:aws:dynamodb:<region>:<account-id>:table/yard_yard",
        "arn:aws:dynamodb:<region>:<account-id>:table/yard_yard/index/GSI1"
      ]
    }
  ]
}
```

If you allow the server to auto-create the table (i.e. you don't
pre-provision it), add `dynamodb:CreateTable` on the table ARN for the
initial bootstrap.

<!-- VERIFY: whether to grant CreateTable to the long-lived runtime role, or provision the table via Terraform/IaC and omit CreateTable from the runtime policy. The latter is the safer production pattern. -->

### AWS permissions for yard apply (webhook-triggered)

When a user comments `yard apply` on a PR, `yard-server` clones the repo,
resolves the project via `yard_core::resolve_project`, and calls
`yard_core::apply` (`github/router.rs:301-340`). That code path uses the
Glue and/or EMR providers and S3 — meaning the same IAM permissions the
local CLI would need. These include, at minimum:

<!-- VERIFY: complete IAM action list for Glue + EMR + S3 + Airflow (MWAA) deploys. The repository does not define or enforce an IAM policy for the apply path; what's listed below is inferred from the provider code in yard-core/src/providers/{glue,emr}.rs and airflow_dag/ but has not been audited against actual AWS API calls. -->

- **Glue** — `glue:CreateJob`, `glue:UpdateJob`, `glue:DeleteJob`,
  `glue:GetJob`, `glue:StartJobRun` (if the provider starts runs).
- **EMR** — `elasticmapreduce:AddJobFlowSteps`,
  `elasticmapreduce:DescribeStep`, `elasticmapreduce:DescribeCluster`.
- **S3** — `s3:PutObject`, `s3:GetObject`, `s3:DeleteObject` on the
  `script_bucket` (and, for state, on the configured state bucket); plus
  `s3:ListBucket` for state file discovery.
- **STS** — `sts:AssumeRole` on the `aws.assume_role` ARN if the project
  declares one, plus whatever role-chaining the target account requires.
- **IAM** (target account) — `iam:PassRole` for the Glue / EMR execution
  role.

The **recommended production pattern** is to give `yard-server`'s own
role only DynamoDB + `sts:AssumeRole` into one or more *target* deploy
roles (one per AWS account being deployed to), and let per-project
`aws.assume_role` config in `yard.yaml` perform the actual cross-account
privilege grant. This mirrors how the CLI's `YARD_AWS_ASSUME_ROLE` works
(see [CONFIGURATION.md](CONFIGURATION.md#yard-cli-environment-variables)).

<!-- VERIFY: cross-account AssumeRole setup. The repository supports STS AssumeRole via YAML or YARD_AWS_* env vars, but the trust-policy wiring is deployment-specific. -->

## GitHub webhook setup

### Webhook endpoint

Configure the GitHub webhook at the **repository level** (or at the org
level, if you want one `yard-server` to service multiple repos — but note
that `YARD_REPO_OWNER` + `YARD_REPO_NAME` are single-repo env vars, so the
repo-level pattern matches the code).

| Field | Value |
|-------|-------|
| Payload URL | `https://<your-yard-server-host>/api/webhook/github` |
| Content type | `application/json` |
| Secret | The value of `YARD_WEBHOOK_SECRET` |
| SSL verification | Enabled (once HTTPS is in front of the server) |

### Events to subscribe to

Inspect `github/webhook.rs` — the server handles exactly two GitHub event
types and ignores everything else (`parse_webhook` matches on
`x-github-event`):

- **`pull_request`** — on `opened` / `synchronize`, triggers `yard plan`
  and posts the result as a PR comment (`webhook.rs:145-163`).
- **`issue_comment`** — when a new comment with body `yard apply` is
  created on a PR, triggers `yard apply` (`webhook.rs:165-197`).

All other events (including `push`, `ping`, merge, close, edit) route to
`WebhookAction::Ignore` and return `200 OK` with no action.

In the GitHub webhook UI this corresponds to selecting **"Let me select
individual events"** and ticking **Pull requests** and **Issue comments**.

### Auth token scopes

`YARD_GITHUB_TOKEN` is used for three things:

1. Cloning the repo at a PR head SHA (`github/git_ops.rs:10-22`,
   injected via `http.extraheader`).
2. Resolving the PR head SHA on `issue_comment` events that don't carry
   it (`github/router.rs:102-115`, via `octocrab`).
3. Posting the plan / apply result as a PR comment (`github/client.rs`
   via `octocrab`, called from `github/router.rs:220-233, 343-356`).

<!-- VERIFY: minimum required scopes for YARD_GITHUB_TOKEN. A classic PAT with `repo` scope (or, for public repos, `public_repo`) is sufficient to read PR metadata, clone, and comment. A fine-grained PAT or GitHub App installation token needs: Pull requests: Read & Write (for comments), Contents: Read (for clone), Issues: Read & Write (for `issue_comment` events). Exact minimum set has not been audited. -->

### GitHub App vs. PAT

The repo uses the term "GitHub personal access token or app token"
(see `env.local.example` / the required-env error message). There is
**no** GitHub App manifest shipped in the repo — no `app.yml`, no
webhook setup script. Operators who want a GitHub App must create one
manually and pass its installation token via `YARD_GITHUB_TOKEN`.

<!-- VERIFY: whether a GitHub App token refresh mechanism is needed. The server reads YARD_GITHUB_TOKEN once at startup (main.rs:74) and never refreshes it, which means short-lived App installation tokens would expire. A classic PAT or a rotation mechanism external to yard-server is required for production use. -->

## Reverse proxy / HTTPS

`yard-server` itself speaks plain HTTP — there is no TLS listener in
`main.rs`. Production deployments must front it with a TLS-terminating
reverse proxy.

<!-- VERIFY: choice of reverse proxy and TLS config. Any of nginx, Caddy, an ALB, CloudFront, Traefik, or Fly.io's built-in TLS works. None of this is configured in the repo. -->

Requirements for the proxy layer:

- **Terminate TLS** in front of `yard-server` — GitHub webhooks default to
  verifying SSL and should not be pointed at plain HTTP.
- **Forward the request body unmodified** to the webhook endpoint. The
  HMAC signature in `X-Hub-Signature-256` is computed over the raw bytes
  and re-verified byte-for-byte (`webhook.rs:12-27`).
- **Forward headers** — at minimum `X-Hub-Signature-256` and
  `X-GitHub-Event`. Without these the webhook handler returns 401 / 400
  (`webhook.rs:121-137`).
- **WebSocket upgrade** on `/api/ws/events` — the dashboard's real-time
  updates use a WebSocket (`api/events.rs:71`). The proxy must be
  configured to upgrade.
- **CORS** is already permissive in `yard-server` itself (`CorsLayer` with
  `allow_origin(Any)` — `main.rs:115-118`). If you want to tighten CORS,
  do it at the proxy layer.

### Rate limiting

`yard-server` applies a `tower_governor` layer globally at 30 req/sec,
burst 60 (`main.rs:120-125`). This is a basic abuse guard and applies to
*every* route including the webhook endpoint. For production you will
likely want:

<!-- VERIFY: whether GitHub's webhook delivery rate can realistically saturate 30 req/sec. For small-to-medium fleets this is comfortably over-provisioned. For very active monorepos with many simultaneous PR events, consider a proxy-side bypass / higher allowance for the webhook path. -->

## Health check endpoints

**`yard-server` does not currently expose a dedicated `/health` or
`/healthz` endpoint.** Grepping the source for `health`, `/api/health`,
and `healthz` returns no hits in `yard-server/src/`.

For platform health checks, operators currently have two options:

- **TCP health check** on the listen port — works, but doesn't detect a
  process that is listening but unable to reach DynamoDB.
- **HTTP GET on an existing endpoint**, e.g. `GET /api/settings` (returns
  JSON, requires DynamoDB to be reachable, so doubles as a liveness +
  readiness probe). This has not been designed as a health endpoint, so
  treat it as best-effort.

<!-- VERIFY: adding a first-class /health endpoint is a known gap. If you need platform-native liveness/readiness probes for ECS/Kubernetes, consider adding one before deploying. -->

## Migrations / DynamoDB schema init

There is **no separate migration tool or command** — schema creation is
a side effect of `yard-server` startup. `DynamoDatabase::migrate()` is
called directly from `start_api_server()` (`main.rs:88-90`) after
`connect(...)`:

1. `CreateTable` is attempted.
2. If `ResourceInUseException` comes back, the table is treated as
   already existing and boot continues (`db/dynamo.rs:119-128`).
3. Any other error causes the server to exit with `"Failed to run
   DynamoDB migrations: …"`.
4. On fresh creation, `wait_for_table_active` polls `DescribeTable` for
   up to ~30 seconds before returning (`db/dynamo.rs:134-153`).

This means a fresh deployment's very first boot will take tens of
seconds longer than a warm boot. Subsequent boots are effectively no-ops
for schema.

If you prefer to provision the table out of band (Terraform, CDK, etc.),
the schema to match is:

```
Table: {prefix}_yard            (default prefix "yard" → "yard_yard")
  PK      S   HASH
  SK      S   RANGE
  GSI1PK  S
  GSI1SK  S
  BillingMode: PAY_PER_REQUEST
  GSI:
    GSI1:
      PK: GSI1PK (HASH)
      SK: GSI1SK (RANGE)
      Projection: ALL
```

The exact `create-table` CLI command is in `docker-compose.yml` lines
29-48 and can be adapted to run against real AWS by dropping
`--endpoint-url=http://ministack:4566`.

## Rollback procedure

<!-- VERIFY: rollback procedure is deployment-platform specific and not defined in the repo. General approach below. -->

Because `yard-server` is a single binary with DynamoDB as its only
persistent store, and all DynamoDB writes are additive (new rows with
timestamped sort keys for webhooks, plans, drift), rolling back a bad
release is primarily a binary-swap operation:

1. Re-deploy the previous `yard-server` build.
2. No DynamoDB schema rollback is needed — the GSI and single-table
   design have not changed across versions visible in the repo.
3. If a bad release wrote malformed settings (via the Settings page),
   fix them manually via `aws dynamodb update-item` on the
   `SETTINGS#<key>` row, or from the UI after rolling back.

Background polling loops are idempotent — restarting the server
re-runs `drift_poll_loop` and `dashboard_poll_loop` from scratch on the
next cycle (`main.rs:164, 313`).

## Monitoring

- **Logging** — `tracing` + `tracing-subscriber` with an env-filter
  (`RUST_LOG`, default `info`). `TraceLayer` is attached to the Axum
  router (`main.rs:136`) so every HTTP request is logged.
- **Slack alerts** — drift-threshold alerts are built in. When
  `slack_enabled=true`, `slack_webhook_url` is set, and
  `alert_drift_threshold` is ≥ drifted job count, the server POSTs a
  Slack Blocks payload to the configured Incoming Webhook URL
  (`main.rs:196-299`, `alerting/slack.rs`). There is one cooldown knob
  (`alert_cooldown_minutes`, default 10) and a single-attempt HTTP POST
  with a 10s timeout (`alerting/slack.rs:22, 36-44`) — no retry.
- **Real-time events** — `GET /api/ws/events` broadcasts
  `DriftRefreshed`, `DriftFailed`, `DashboardRefreshed`,
  `DashboardFailed`, `WebhookReceived`, and `AlertSent` to connected UI
  clients (`api/events.rs`). These are *not* exported metrics — they are
  UI fan-out only.
- **No Prometheus / OpenTelemetry integration** is present in the repo
  (`Cargo.toml` contains neither `prometheus`, `opentelemetry`, nor
  `metrics` crates).

<!-- VERIFY: monitoring dashboard URL, Slack workspace, and on-call routing. None of this is defined in the repo. Slack webhook URL is set at runtime via the Settings page and stored in DynamoDB, not in config. -->

<!-- VERIFY: production log shipping destination (CloudWatch Logs, Datadog, Loki, etc.). Not configured in the repo — tracing goes to stderr by default. -->

## Production hardening checklist

These items are **not enforced by the repo** — use VERIFY markers below
as a reminder that each requires deployment-specific configuration:

<!-- VERIFY: bind to a non-privileged port behind a reverse proxy (YARD_PORT=3001 is fine inside a container; front it with 443 on the proxy). -->

<!-- VERIFY: scope IAM to least privilege per the policy sketch above. Pre-provision the DynamoDB table so the runtime role doesn't need CreateTable. -->

<!-- VERIFY: rotate YARD_GITHUB_TOKEN periodically; prefer a GitHub App installation with short-lived tokens once token refresh is implemented. -->

<!-- VERIFY: rotate YARD_WEBHOOK_SECRET in lockstep with the GitHub webhook UI; there is no zero-downtime secret rotation built into yard-server. -->

<!-- VERIFY: run with a read-only root filesystem. The only writable path the server uses is std::env::temp_dir() for git clones (github/git_ops.rs:34). Mount /tmp writable. -->

<!-- VERIFY: verify outbound egress to github.com (git clone + octocrab), Slack webhook URL (if alerts enabled), and the DynamoDB regional endpoint. -->

<!-- VERIFY: configure RUST_LOG to ship logs to your log aggregator; default is INFO to stderr. -->

## Related docs

- [CONFIGURATION.md](CONFIGURATION.md) — full env var + YAML reference.
- [ARCHITECTURE.md](ARCHITECTURE.md) — yard-server component layout and
  data flow.
- [API.md](API.md) — every HTTP + WebSocket route the server exposes.
