# yard-server

yard-server is the Atlantis-like web service half of yard: a GitHub webhook
listener, drift-detection daemon, and Dioxus-rendered dashboard. This document
covers the operator-facing configuration surface introduced or changed in
v1.5 Phase 25 (auth middleware + Slack webhook secret store).

## Environment variables

| Variable | Required? | Purpose |
|----------|-----------|---------|
| `YARD_GITHUB_TOKEN` | yes | GitHub PAT used by webhook handlers and PR comment posting. |
| `YARD_WEBHOOK_SECRET` | yes | Shared secret for the GitHub webhook HMAC signature. |
| `YARD_REPO_OWNER` | yes | Repo owner. Used by webhook routing. |
| `YARD_REPO_NAME` | yes | Repo name. Used by webhook routing. |
| `YARD_API_TOKEN` | yes (always) | Bearer token required on every `Authorization: Bearer <token>` header for `/api/*` requests (SRV-01). Required even when `YARD_API_AUTH_DISABLED` is set, because non-loopback callers still go through the standard bearer check (see `YARD_API_AUTH_DISABLED`). |
| `YARD_API_AUTH_DISABLED` | no | Dev-only escape hatch. Set to a truthy value (`1`, `true`, `yes`, `on` — case-insensitive, surrounding whitespace ignored) to skip the bearer check **for loopback callers only** (`127.0.0.1`, `::1`). Non-loopback callers ALWAYS go through the standard bearer check. **Off by default.** When on, the server logs a `tracing::warn!` event and prints `[WARN] /api/* AUTH BYPASS ENABLED FOR LOOPBACK CALLERS (YARD_API_AUTH_DISABLED=1)` to stderr at startup. `YARD_API_TOKEN` is required even when this is set, so non-loopback callers can authenticate via header. Do not use in production. |
| `YARD_PORT` | no | Listen port. Defaults to `3001`. |
| `YARD_DB_TABLE_PREFIX` | no | DynamoDB table-name prefix. Defaults to `yard`. |
| `YARD_DB_REGION` / `AWS_REGION` | no | AWS region for the DynamoDB and Secrets Manager clients. |
| `YARD_DB_ENDPOINT_URL` | no | Override the DynamoDB endpoint (e.g., for LocalStack tests). |

The `Authorization: Bearer ...` header applies to every endpoint under `/api/*`
including the WebSocket upgrade at `/api/ws/events`. The GitHub webhook route
(`POST /api/webhook/github`) is HMAC-secured separately via
`YARD_WEBHOOK_SECRET` and does **not** require a bearer token.

Example `curl`:

```bash
curl -H "Authorization: Bearer $YARD_API_TOKEN" \
  https://yard.example.com/api/dashboard
```

## Bearer-token auth

`YARD_API_TOKEN` is a single shared secret. There is no per-user, per-route, or
rotation surface in v1.5 — the operator chooses how `YARD_API_TOKEN` is
injected and rotates by restarting the server with a new value.

The middleware compares the incoming token against `YARD_API_TOKEN` in
constant time, so timing attacks against the token contents are mitigated.
The constant-time compare is hand-rolled (`yard-server/src/auth/mod.rs`);
no external crypto crate is involved.

When `YARD_API_AUTH_DISABLED` is set to a truthy value (`1`, `true`, `yes`, or `on` — case-insensitive, surrounding whitespace ignored):

- The bearer check is skipped **only for callers whose source IP address is loopback** (`127.0.0.1` for IPv4 or `::1` for IPv6). Non-loopback callers still go through the standard bearer check and must present a valid `Authorization: Bearer <YARD_API_TOKEN>` header.
- `YARD_API_TOKEN` is still required at startup so non-loopback callers can authenticate via the bearer header. Boot fails fast with the standard required-env error if `YARD_API_TOKEN` is unset.
- A warning event lands in the structured log (`tracing::warn!`).
- `[WARN] /api/* AUTH BYPASS ENABLED FOR LOOPBACK CALLERS (YARD_API_AUTH_DISABLED=1)` is printed to stderr next to the listening banner (loopback = the same `127.0.0.1` / `::1` set the env-var table describes).

The loopback-only enforcement is based on the OS-reported peer SocketAddr (via axum's `ConnectInfo<SocketAddr>` extractor). The middleware does NOT consult `X-Forwarded-For`, `Forwarded`, or any other client-controlled header — only the immutable peer address from the kernel-level socket. If a reverse proxy terminates connections on `127.0.0.1`, the proxy itself becomes the loopback caller. **Do not terminate untrusted traffic on a loopback-bound proxy when the bypass is enabled.**

This is a deliberate gap-closure for ROADMAP SC #2 ("localhost-only dev bypass") and supersedes the prior "bypass is independent of bind address" behaviour documented in earlier revisions of this file.

## Slack webhook secret migration

Starting in v1.5, the Slack incoming-webhook URL is **not** stored in
DynamoDB. The Settings table holds only an AWS Secrets Manager ARN
reference (the `slack_webhook_secret_arn` key); the actual URL is resolved
on every drift-alert tick via `secretsmanager:GetSecretValue`.

If you previously configured `slack_webhook_url` in DynamoDB (any version
prior to v1.5), the server will refuse to start until you migrate.
Migration steps:

1. Copy the URL out of DynamoDB:

   ```bash
   aws dynamodb get-item --table-name <yard_table> \
     --key '{"PK":{"S":"SETTING#slack_webhook_url"},"SK":{"S":"SETTING#slack_webhook_url"}}'
   ```

2. Create a Secrets Manager secret holding that URL:

   ```bash
   aws secretsmanager create-secret \
     --name yard/slack-webhook \
     --secret-string '<THE_URL>'
   ```

   Note the `ARN` field of the `create-secret` response.

3. Delete the legacy plaintext row from DynamoDB:

   ```bash
   aws dynamodb delete-item --table-name <yard_table> \
     --key '{"PK":{"S":"SETTING#slack_webhook_url"},"SK":{"S":"SETTING#slack_webhook_url"}}'
   ```

4. POST the ARN to `/api/settings`, or paste it into the Settings UI's
   "Secret ARN" input under the Slack notification card:

   ```bash
   curl -H "Authorization: Bearer $YARD_API_TOKEN" \
        -H "Content-Type: application/json" \
        -X POST https://yard.example.com/api/settings \
        -d '{"settings":{"slack_webhook_secret_arn":"arn:aws:secretsmanager:..."}}'
   ```

5. Restart the server. It will boot now that the legacy row is gone.

## IAM permission

The yard-server IAM principal (instance role, ECS task role, k8s ServiceAccount IRSA, etc.)
needs `secretsmanager:GetSecretValue` on the relevant secret ARN(s):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "secretsmanager:GetSecretValue",
      "Resource": "arn:aws:secretsmanager:<region>:<account>:secret:yard/slack-webhook-*"
    }
  ]
}
```

yard-server **never** calls `CreateSecret`, `PutSecretValue`, or
`DeleteSecret`. Provisioning and rotation are operator responsibilities.

## Settings keys

| Key | Type | Purpose |
|-----|------|---------|
| `theme` | string (`light` / `dark` / `system`) | UI theme. |
| `drift_interval` | string (`1` / `3` / `5` / `10` minutes) | How often `drift_poll_loop` runs. |
| `dashboard_interval` | string (positive integer minutes) | How often `dashboard_poll_loop` refreshes. |
| `slack_enabled` | string (`true` / `false`) | Master switch for the Slack drift alert. |
| `slack_webhook_secret_arn` | string (ARN) | **NEW in v1.5.** Secrets Manager ARN whose secret value is the Slack incoming-webhook URL. |
| `alert_drift_threshold` | string (positive integer >= 1) | Min drifted-job count to fire an alert. |
| `alert_cooldown_minutes` | string (positive integer >= 1) | Min minutes between alerts. |
| `alert_last_sent_at` | string (RFC3339) | Server-written. Last time an alert fired. |

The legacy `slack_webhook_url` key is **decommissioned**: POSTs to
`/api/settings` containing it are rejected with HTTP 400. The server
refuses to boot if a row with this key still exists in DynamoDB (see
"Slack webhook secret migration" above).
