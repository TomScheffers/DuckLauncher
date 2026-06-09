# DuckLauncher

Coordinator-worker system for scheduling DuckDB queries onto workers tracked in PostgreSQL.

## Install

```bash
pip install ducklauncher
```

For local development:

```bash
uv sync
uv pip install -e .
```

## CLI

```bash
# Start PostgreSQL (optional helper script in repo)
./scripts/run-postgres.sh

# Coordinator (auto-loads ./init.sql when present; see init.sql.example)
cp init.sql.example init.sql   # first time only
ducklauncher coordinator \
  --database-url postgresql://postgres:postgres@localhost:5432/ducklauncher \
  --port 8000

# Worker
ducklauncher worker \
  --coordinator-url http://127.0.0.1:8000 \
  --endpoint http://127.0.0.1:8001 \
  --cpus 8 \
  --memory 16384 \
  --disk-space 102400 \
  --duckdb-path /data/duck.db \
  --port 8001

# Submit a query
curl -X POST http://127.0.0.1:8000/queries \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT 1 AS value"}'
```

Equivalent module invocation:

```bash
python -m ducklauncher coordinator --init-scripts init.sql
python -m ducklauncher worker --cpus 8 --memory 16384
```

## Architecture

- **Coordinator** accepts queries, stores them in PostgreSQL, schedules them onto workers, and dispatches via HTTP.
- **Workers** register with an advertised endpoint, run init scripts, execute DuckDB queries, and report completion.

Worker statuses: `initializing`, `running`, `unreachable`, `shutting_down`, `stopped`, `error`. Workers register as `initializing` while init scripts run, then call `/workers/{id}/ready` before accepting queries. Dispatch failures mark a worker `unreachable` until its next heartbeat.

Query statuses: `pending`, `running`, `completed`, `failed`, `cancelled`.

## API

### Coordinator

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/workers/register` | Worker registration |
| `POST` | `/workers/{worker_id}/heartbeat` | Heartbeat |
| `POST` | `/workers/{worker_id}/shutdown` | Mark worker as shutting down |
| `POST` | `/queries` | Submit a query |
| `GET` | `/queries/mine` | List query history for the signed-in user (empty when auth is off) |
| `GET` | `/queries/{query_id}` | Query status |
| `GET` | `/queries/{query_id}/events` | SSE query status stream |
| `GET` | `/queries/{query_id}/result` | Paginated query result |
| `POST` | `/queries/{query_id}/complete` | Worker completion callback |
| `POST` | `/queries/{query_id}/cancel` | Cancel a pending or running query |
| `GET` | `/sheets` | List saved SQL sheets (requires auth) |
| `POST` | `/sheets` | Create a sheet |
| `PATCH` | `/sheets/{sheet_id}` | Update a sheet |
| `DELETE` | `/sheets/{sheet_id}` | Delete a sheet |
| `GET` | `/auth/me` | Auth status |
| `GET` | `/auth/login` | Start OIDC login |
| `GET` | `/auth/callback` | OIDC callback |
| `POST` | `/auth/logout` | End session |

### Worker

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/metrics` | Resource metrics |
| `POST` | `/query` | Execute a dispatched query (202) |
| `POST` | `/query/cancel` | Cancel a specific in-flight query |

Each query uses a dedicated connection from a warm pool so cancellation only affects that query. Queries are scheduled immediately on submit when a worker has capacity; a background scheduler drains the queue.

## Configuration

CLI flags override environment variables.

| Flag / Variable | Service | Default | Description |
|-----------------|---------|---------|-------------|
| `--database-url` / `DATABASE_URL` | Coordinator | `postgresql://postgres:postgres@localhost:5432/ducklauncher` | PostgreSQL connection |
| `--init-scripts` / `INIT_SCRIPTS_PATH` | Coordinator | `./init.sql` if present | SQL init file (gitignored; copy from `init.sql.example`). `${VAR}` placeholders are expanded from the coordinator environment. |
| `--cpus` | Worker | auto-detect | CPUs advertised to coordinator |
| `--memory` | Worker | auto-detect | Available memory in MB |
| `--disk-space` | Worker | auto-detect | Available disk in MB |
| `--coordinator-url` / `COORDINATOR_URL` | Worker | `http://127.0.0.1:8000` | Coordinator base URL |
| `--endpoint` / `WORKER_ENDPOINT` | Worker | `http://127.0.0.1:8001` | Reachable worker URL |
| `--duckdb-path` / `DUCKDB_PATH` | Worker | `:memory:` | DuckDB database path |
| `--max-concurrent-queries` / `MAX_CONCURRENT_QUERIES` | Worker | `10` | Max parallel queries per worker |
| `--connection-pool-size` / `CONNECTION_POOL_SIZE` | Worker | same as max | Warm DuckDB connections |

### Authentication (optional)

When OIDC is **not** configured, DuckLauncher still assigns each browser an anonymous session cookie (random `anon:{uuid}` user). Query history and sheets persist per browser without login.

When OIDC **is** configured, anonymous sessions work the same until the user clicks Login; OIDC login replaces the anonymous session with the authenticated account.

Set all of the following on the **coordinator** to enable login:

| Variable | Required | Description |
|----------|----------|-------------|
| `OIDC_ISSUER_URL` | yes | OIDC issuer URL (discovery document is fetched automatically) |
| `OIDC_CLIENT_ID` | yes | OAuth client ID |
| `OIDC_CLIENT_SECRET` | yes | OAuth client secret (server-side only) |
| `SESSION_SECRET` | yes | Secret for OAuth state cookies |
| `OIDC_REDIRECT_URI` | no | Defaults to `{PUBLIC_BASE_URL}/auth/callback` |
| `PUBLIC_BASE_URL` | no | Public coordinator URL, default `http://127.0.0.1:8000` |
| `SESSION_TTL_HOURS` | no | Session lifetime, default `168` (7 days) |

Register the redirect URI with your IdP (for example `https://ducklauncher.example.com/auth/callback`). Worker registration and completion callbacks stay unauthenticated.

### Init script environment

`init.sql.example` shows a minimal Iceberg REST catalog attach. Copy it to `init.sql` and export variables on the **coordinator** host before starting:

| Variable | Required | Default |
|----------|----------|---------|
| `ICEBERG_WAREHOUSE` | yes | — |
| `ICEBERG_ENDPOINT` | yes | — |
| `ICEBERG_CLIENT_ID` | yes | — |
| `ICEBERG_CLIENT_SECRET` | yes | — |
| `ICEBERG_REGION` | yes | — |
| `ICEBERG_OAUTH_URI` | no | `{ICEBERG_ENDPOINT}/v1/oauth/tokens` |
| `ICEBERG_OAUTH_SCOPE` | no | `PRINCIPAL_ROLE:ALL` |
| `ICEBERG_ACCESS_MODE` | no | `vended_credentials` |

## Development

```bash
./scripts/run-postgres.sh
export DATABASE_URL='postgresql://postgres:postgres@localhost:5432/ducklauncher'
uv run pytest tests/ -v
uv build
```

## Docker (public image)

Images are published to **[GitHub Container Registry](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry)** (GHCR) — free and public for open source:

```
ghcr.io/tomscheffers/ducklauncher:latest
ghcr.io/tomscheffers/ducklauncher:0.1.0
```

No `imagePullSecrets` required on EKS/Kubernetes when the package visibility is **public**.

### Build locally

```bash
docker build -t ducklauncher:local .
docker run --rm -p 8000:8000 \
  -e DATABASE_URL=postgresql://postgres:postgres@host.docker.internal:5433/ducklauncher \
  ducklauncher:local coordinator --port 8000
```

### Publish a release image

Push a semver tag; GitHub Actions builds and pushes to GHCR:

```bash
git tag v0.1.0
git push origin v0.1.0
```

After the workflow completes, make the package public once under **GitHub → Packages → ducklauncher → Package settings → Change visibility**.

### Kubernetes / EKS (Helm)

A Helm chart lives in [`chart/`](chart/):

```bash
helm install ducklauncher ./chart \
  --namespace ducklauncher --create-namespace \
  --set database.url="$DATABASE_URL" \
  --set image.tag=0.1.0 \
  --set worker.replicas=3
```

See [`chart/README.md`](chart/README.md) for all options. Workers run as a **StatefulSet** with a headless Service so each pod has a stable endpoint for query dispatch.

## Publish (PyPI)

```bash
uv build
uv publish
```
