# DuckLauncher Helm Chart

Deploy DuckLauncher on Kubernetes (EKS, etc.) with a coordinator `Deployment` and worker `StatefulSet`.

Workers use a **headless Service** so each pod gets a stable DNS name for HTTP push from the coordinator:

```
http://<release>-worker-0.<release>-worker.<namespace>.svc.cluster.local:8001
```

## Prerequisites

- Kubernetes 1.24+
- Helm 3
- PostgreSQL (RDS recommended for production)
- Public image: `ghcr.io/tomscheffers/ducklauncher`

## Install

```bash
helm install ducklauncher ./chart \
  --namespace ducklauncher --create-namespace \
  --set database.url='postgresql://user:pass@host:5432/ducklauncher' \
  --set image.tag=0.1.0
```

Or use an existing secret:

```bash
kubectl create secret generic ducklauncher-db \
  --namespace ducklauncher \
  --from-literal=database-url='postgresql://...'

helm install ducklauncher ./chart \
  --namespace ducklauncher \
  --set database.existingSecret=ducklauncher-db
```

## Upgrade

```bash
helm upgrade ducklauncher ./chart \
  --namespace ducklauncher \
  --reuse-values
```

## Uninstall

```bash
helm uninstall ducklauncher --namespace ducklauncher
```

## Key values

| Value | Default | Description |
|-------|---------|-------------|
| `image.repository` | `ghcr.io/tomscheffers/ducklauncher` | Container image |
| `image.tag` | `0.1.0` | Image tag |
| `database.url` | — | PostgreSQL URL (creates Secret) |
| `database.existingSecret` | — | Use existing Secret instead |
| `coordinator.replicas` | `1` | Coordinator pods |
| `coordinator.initScripts` | — | SQL distributed to workers on register |
| `worker.replicas` | `3` | Worker pods |
| `worker.maxConcurrentQueries` | `4` | Per-worker concurrency |
| `worker.persistence.size` | `100Gi` | DuckDB data volume per worker |

## Example with init scripts

```bash
helm install ducklauncher ./chart \
  --set database.url="$DATABASE_URL" \
  --set coordinator.initScripts=$'INSTALL httpfs;\nINSTALL parquet;'
```

## Render templates locally

```bash
helm template ducklauncher ./chart \
  --set database.url='postgresql://postgres:postgres@localhost:5432/ducklauncher'
```
