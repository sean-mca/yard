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
| `YARD_API_TOKEN` | yes (always) | Bearer token required on every `Authorization: Bearer <token>` header for `/api/*` requests (SRV-01). Required even when `YARD_API_AUTH_DISABLED` is set, because non-loopback callers still go through the standard bearer check (see `YARD_API_AUTH_DISABLED`). **Charset constraint:** must contain only printable ASCII bytes (`0x21..=0x7E`) excluding `;`, `,`, `"`, and `\`. The token is interpolated into a `Set-Cookie` header value, so RFC 6265-forbidden cookie-syntax bytes (whitespace, control chars, the listed delimiters) are rejected at boot. The check is fail-closed: any forbidden byte aborts startup with a clear error. |
| `YARD_API_AUTH_DISABLED` | no | Dev-only escape hatch. Set to a truthy value (`1`, `true`, `yes`, `on` — case-insensitive, surrounding whitespace ignored) to skip the bearer check **for loopback callers only** (`127.0.0.1`, `::1`). Non-loopback callers ALWAYS go through the standard bearer check. **Off by default.** When on, the server logs a `tracing::warn!` event at startup. `YARD_API_TOKEN` is required even when this is set, so non-loopback callers can authenticate via header. Do not use in production. |
| `YARD_PORT` | no | Listen port. Defaults to `3001`. |
| `YARD_DB_TABLE_PREFIX` | no | DynamoDB table-name prefix. Defaults to `yard`. |
| `YARD_DB_REGION` / `AWS_REGION` | no | AWS region for the DynamoDB and Secrets Manager clients. |
| `YARD_DB_ENDPOINT_URL` | no | Override the DynamoDB endpoint (e.g., for LocalStack tests). |
| `YARD_CORS_ORIGIN` | no | A single allowed origin (e.g. `https://dashboard.example.com`) for the CORS preflight. When set, only that origin's `fetch()` requests succeed CORS preflight against `/api/*`. When unset, the server falls back to `AllowOrigin::any()` for dev convenience — set this in production to scope cross-origin reachability of `/api/auth/session` and friends to a single known origin. The CORS layer is incompatible with credentials regardless of this knob (per the CORS spec), so the cookie-auth attack surface is unaffected; this knob narrows drive-by preflight reachability. |

The `Authorization: Bearer ...` header applies to every endpoint under `/api/*`
including the WebSocket upgrade at `/api/ws/events`. The bundled Dioxus UI uses
a same-origin `yard_session` cookie carrying the same `YARD_API_TOKEN` instead
of the header — see "Cookie-based session login" below for the cookie endpoints
and the HTTPS-only constraint. The GitHub webhook route
(`POST /api/webhook/github`) is HMAC-secured separately via
`YARD_WEBHOOK_SECRET` and does **not** require a bearer token. The
`/api/auth/session` and `/api/auth/logout` endpoints are also OUTSIDE the
bearer layer (chicken-and-egg — login can't require login) but are still
rate-limited.

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

## Cookie-based session login (`/api/auth/session` + `/api/auth/logout`)

In addition to the `Authorization: Bearer ...` header, the bearer middleware also accepts the same `YARD_API_TOKEN` carried in a `yard_session` cookie. Two unauthenticated endpoints sit OUTSIDE the bearer-auth layer (mounted at the parent router level alongside the GitHub webhook router) so the bundled Dioxus UI can establish a session without the chicken-and-egg of "login requires login":

- `POST /api/auth/session` — body `{ "token": "<YARD_API_TOKEN>" }`. On match returns `200 OK` + `Set-Cookie: yard_session=<token>; HttpOnly; SameSite=Strict; Path=/; Secure`. On mismatch returns `401`, no `Set-Cookie` header.
- `POST /api/auth/logout` — returns `204 No Content` + `Set-Cookie: yard_session=; HttpOnly; SameSite=Strict; Path=/; Max-Age=0`. Always clears the cookie; no auth required.

Both endpoints are still rate-limited by the parent-router `tower-governor` layer (30 req/sec, burst=60), which provides brute-force defence even though the endpoints are unauthenticated.

The cookie value IS the `YARD_API_TOKEN` — same security level as the bearer header (forging the cookie requires guessing the token). There is no separate session id, no session storage, no rotation. `HttpOnly` prevents XSS from reading the cookie via `document.cookie`; `SameSite=Strict` prevents CSRF.

When both a valid `Authorization: Bearer` header AND a `yard_session` cookie are present on a request, the header takes precedence (CLI / automation wins over the browser's automatic cookie inclusion). Failure messages do not distinguish which credential type was attempted.

### Why the cookie path exists

Browsers do not let a WASM bundle set arbitrary `Authorization` headers on cross-origin or WebSocket-upgrade requests. They DO automatically include same-origin cookies on every fetch and on the WebSocket handshake. Carrying the token via a `HttpOnly` cookie lets the bundled UI authenticate without ever materialising the token in JS-readable memory.

### HTTPS is required for the cookie path

The `yard_session` cookie's `Set-Cookie` header **always** includes `Secure`. yard-server does NOT attempt to detect whether the inbound request was HTTPS — that heuristic is broken behind a TLS-terminating proxy that talks plain HTTP to the application (axum sees `http://` even when the operator's browser used HTTPS). Failing closed (always-Secure) is the right direction: it guarantees the browser only sends the cookie back on HTTPS connections.

**Practical consequence for operators:**

- **Production / staging:** terminate TLS upstream (ALB, CloudFront, nginx, Traefik, etc.) and point yard-server at the resulting HTTPS origin. The cookie path works as expected. This is the recommended deployment shape.
- **Local-dev over `http://127.0.0.1:3001`:** the browser will accept the `Set-Cookie` response but will NOT send the cookie back on subsequent HTTP requests (because of the `Secure` flag). Local-dev callers must use the `Authorization: Bearer $YARD_API_TOKEN` header instead. The `yard_session` cookie path is intentionally unsupported on plain HTTP. Combine with `YARD_API_AUTH_DISABLED=1` (loopback-only bypass) if even the bearer header is friction during dev.

### Example: log in via cookie, then call /api/dashboard

```bash
# 1. Establish the cookie (HTTPS required for the browser to send it back).
curl -i -c jar.txt \
     -H "Content-Type: application/json" \
     -X POST https://yard.example.com/api/auth/session \
     -d "{\"token\":\"$YARD_API_TOKEN\"}"
# 200 OK
# Set-Cookie: yard_session=...; HttpOnly; SameSite=Strict; Path=/; Secure

# 2. Subsequent requests automatically include the cookie.
curl -b jar.txt https://yard.example.com/api/dashboard

# 3. Log out (clears the cookie; 204 No Content).
curl -X POST -b jar.txt https://yard.example.com/api/auth/logout
```

## Browser-session login

The bundled Dioxus dashboard (served from yard-server's root path, e.g.,
`https://yard.example.com/`) authenticates via the `yard_session` cookie
described in the section above. This subsection walks the operator-facing
flow that the bundled UI implements and records the WASM-bundle leak
verification step.

### Operator flow

1. Open the dashboard URL in a browser. The first call to any `/api/*`
   endpoint (e.g., `/api/dashboard/cached`) returns 401 because no
   `yard_session` cookie has been set yet.
2. The UI's shared fetch helper (`yard-server/src/ui/fetch.rs`) detects
   the 401 and pushes the browser to `/login` via the Dioxus router.
3. Paste the `YARD_API_TOKEN` value into the password field on `/login`.
   Click **Sign in**. The form POSTs `{ "token": "<typed value>" }` to
   `/api/auth/session`.
4. On match the server returns 200 + `Set-Cookie: yard_session=<token>;
   HttpOnly; SameSite=Strict; Path=/; Secure`. The UI navigates to `/`
   (the Dashboard route). On mismatch the page shows
   `Invalid token — check the value and try again` inline.
5. All subsequent fetches and the WebSocket upgrade automatically
   include the cookie (browser default for same-origin requests).
6. To sign out, open the **Settings** page and click **Sign out** under
   the "Session" card. The UI POSTs `/api/auth/logout`, which clears the
   cookie via `Set-Cookie: Max-Age=0`, then navigates back to `/login`.

The typed token is held in a single `Signal<String>` for one submit cycle
and cleared (`set(String::new())`) immediately after the POST resolves —
success or failure. The bundled WASM has no `localStorage` /
`sessionStorage` / `web_sys::Storage` access; the only post-login token
store is the browser's `HttpOnly` cookie, which WASM cannot read.

### CLI / automation callers

Unchanged. CLI tools and CI pipelines continue to use the
`Authorization: Bearer $YARD_API_TOKEN` header. The middleware accepts
either credential, and the header takes precedence when both are present.

### Verifying the WASM bundle does not leak the token

The token is never compiled into the WASM bundle. Two layers of
verification keep this honest:

1. **Source-level invariant (enforced at code review).** No file under
   `yard-server/src/` references `YARD_API_TOKEN` via `env!(...)` or
   `option_env!(...)`:

   ```bash
   grep -rE 'option_env!\("YARD_API_TOKEN|env!\("YARD_API_TOKEN' yard-server/src/
   # (should print nothing)
   ```

2. **Byte-scan after a fresh build.** Build the bundle and confirm a
   sentinel token never appears in the WASM bytes:

   ```bash
   export YARD_API_TOKEN="leak-canary-7e2c4f"
   # Build the WASM bundle (path may vary by Dioxus version).
   dx build --release   # from yard-server/
   strings yard-server/target/dx/yard-server/release/web/public/assets/*.wasm \
     | grep "leak-canary-7e2c4f"
   # (should print nothing)
   ```

The structural source-grep is the automated bound; the byte-scan is a
one-off manual check the operator can re-run any time.

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
