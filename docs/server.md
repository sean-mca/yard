# yard-server

Web dashboard with GitHub webhook integration and drift detection. Dioxus fullstack app with axum API backend and DynamoDB persistence.

## Features

- **PR-driven workflow (Atlantis-style):** plan runs automatically on PR open, apply triggered by commenting `yard apply` on the PR
- **Live drift detection:** compares repo config against deployed state on a configurable interval
- **Dashboard:** PR status, plan results, job counts, drift alerts
- **Settings persistence:** theme, drift interval, Slack webhook

## GitHub webhook setup

Configure your repo's webhook to send `pull_request` and `issue_comment` events to `https://your-server/api/webhook/github`. Set the secret to match `YARD_WEBHOOK_SECRET`.

**Flow:**
1. Open a PR -- yard-server auto-runs `yard plan` and posts the result as a comment
2. Review the plan output in the PR
3. Comment `yard apply` -- yard-server runs `yard apply` and posts the result
4. Merge the PR

## Environment variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `YARD_GITHUB_TOKEN` | Yes | -- | GitHub personal access token |
| `YARD_WEBHOOK_SECRET` | Yes | -- | Webhook HMAC secret |
| `YARD_REPO_OWNER` | Yes | -- | GitHub repo owner |
| `YARD_REPO_NAME` | Yes | -- | GitHub repo name |
| `YARD_DB_TABLE_PREFIX` | No | `yard` | DynamoDB table prefix |
| `YARD_DB_REGION` | No | `us-east-1` | AWS region for DynamoDB |
| `YARD_DB_ENDPOINT_URL` | No | -- | Custom endpoint (for local dev) |
| `YARD_API_BASE` | No | `http://127.0.0.1:3001` | API base URL (compile-time, set to `""` for production) |

AWS credentials are required for DynamoDB. The server creates the table and indexes on first startup.

## Local development

```bash
docker compose up -d                              # ministack: S3 + DynamoDB on localhost:4566
cp env.local.example .env.local                    # fill in GitHub token
set -a && source .env.local && set +a && cd yard-server && dx serve  # start the server
```

## DynamoDB permissions

`dynamodb:CreateTable`, `dynamodb:DescribeTable`, `dynamodb:PutItem`, `dynamodb:GetItem`, `dynamodb:Query`
