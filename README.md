# Matatu Pulse — GPS Pipeline Orchestrator

> **Target:** 100 000 concurrent vehicles · ~20 000 GPS events/sec · sub-200 ms end-to-end latency

The root `main.go` is the **orchestrator** for the full GPS ingestion pipeline. It spawns, supervises, health-monitors, and auto-scales three child services as OS processes:

| Service | Role | Default replicas |
|---|---|---|
| `mqtt-consumer` | Parse NMEA, apply movement filter, fan-out to Redis streams | 4 – 32 (auto-scaled) |
| `batch-writer` | Drain cold-path stream, bulk-insert to ClickHouse | 2 – 8 |
| `websocket-gateway` | Push real-time position updates to browser clients | 2 – 10 |

The orchestrator itself exposes a **control-plane** HTTP API on `:7070` (health, status, manual scaling, aggregated Prometheus metrics).

---

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [Repository Layout](#2-repository-layout)
3. [Environment Variables Reference](#3-environment-variables-reference)
4. [Development Environment](#4-development-environment)
5. [Production Environment](#5-production-environment)
6. [Control Plane API](#6-control-plane-api)
7. [Auto-Scaling Logic](#7-auto-scaling-logic)
8. [Shutdown Behaviour](#8-shutdown-behaviour)
9. [Observability](#9-observability)
10. [Troubleshooting](#10-troubleshooting)

---

## 1. Prerequisites

### Common

| Tool | Minimum version | Purpose |
|---|---|---|
| Go | 1.21 | Build all binaries |
| Redis | 7.x (Cluster mode) | Hot-path streams + latest state |
| ClickHouse | 23.x | Cold-path durable storage |
| Mosquitto | 2.x | MQTT broker |
| Jaeger | 1.x | Distributed tracing (optional but recommended) |

### Development only

- `docker compose` — used to spin up Redis Cluster, ClickHouse, and Mosquitto locally with a single command.

### Production only

- Kubernetes (k3s or full K8s) with `kubectl` — the orchestrator runs as a single Deployment pod; the child processes run inside that pod.
- A pre-built container image pushed to your registry.
- Secrets managed via Kubernetes Secrets or a Vault sidecar.

---

## 2. Repository Layout

```
FLTM/build/
├── main.go                  # ← Orchestrator (this document)
├── go.mod
├── go.sum
├── mqtt-consumer/
│   ├── main.go
│   └── config/
│       └── config.json
├── batch-writer/
│   ├── main.go
│   └── clickhouse-gps_schema.sql
└── websocket-gateway/
    └── main.go
```

The orchestrator treats each subdirectory as a standalone service. In **development** it calls `go run ./mqtt-consumer` etc. In **production** it expects pre-compiled binaries.

---

## 3. Environment Variables Reference

All variables are optional; defaults are shown. Variables marked **required** have no sane default and the orchestrator will fail without them.

### Orchestrator

| Variable | Default | Description |
|---|---|---|
| `BINARY_DIR` | `.` | Directory containing compiled service binaries (ignored when `USE_GO_RUN=true`) |
| `USE_GO_RUN` | `false` | Use `go run ./service-dir` instead of pre-built binaries. Set `true` in dev. |
| `CONTROL_PLANE_ADDR` | `:7070` | Listen address for the admin HTTP API |
| `SCALE_INTERVAL` | `15s` | How often the auto-scaler evaluates Redis stream lag |
| `HEALTH_CHECK_INTERVAL` | `10s` | How often each replica's health endpoint is polled |
| `HEALTH_CHECK_TIMEOUT` | `3s` | Per-poll HTTP timeout |
| `UNHEALTHY_THRESHOLD` | `3` | Consecutive health failures before a replica is restarted |
| `RESTART_BACKOFF_MAX` | `1m` | Cap on exponential restart back-off |
| `SCALE_UP_LAG` | `50000` | Redis stream lag (messages) that triggers scale-up |
| `SCALE_DOWN_LAG` | `5000` | Redis stream lag below which a replica is removed |

### MQTT Consumer pool

| Variable | Default | Description |
|---|---|---|
| `MQTT_CONSUMER_MIN` | `4` | Minimum replicas |
| `MQTT_CONSUMER_MAX` | `32` | Maximum replicas |
| `MQTT_CONSUMER_METRICS_BASE_PORT` | `9100` | Replica N binds to `9100 + N` |
| `MQTT_CONSUMER_HEALTH_BASE_PORT` | `8100` | Replica N binds to `8100 + N` |

### Batch Writer pool

| Variable | Default | Description |
|---|---|---|
| `BATCH_WRITER_MIN` | `2` | Minimum replicas |
| `BATCH_WRITER_MAX` | `8` | Maximum replicas |
| `BATCH_WRITER_METRICS_BASE_PORT` | `9200` | Replica N binds to `9200 + N` |
| `BATCH_WRITER_HEALTH_BASE_PORT` | `8200` | Replica N binds to `8200 + N` |

### WebSocket Gateway pool

| Variable | Default | Description |
|---|---|---|
| `WS_GATEWAY_MIN` | `2` | Minimum replicas |
| `WS_GATEWAY_MAX` | `10` | Maximum replicas |
| `WS_GATEWAY_METRICS_BASE_PORT` | `9300` | Replica N binds to `9300 + N` |
| `WS_GATEWAY_HEALTH_BASE_PORT` | `8300` | Replica N binds to `8300 + N` |

### Shared secrets (required in all environments)

| Variable | Description |
|---|---|
| `MQTT_BROKER` | `host:port` of the Mosquitto broker |
| `MQTT_USERNAME` | MQTT client username |
| `MQTT_PASSWORD` | MQTT client password |
| `REDIS_PASSWORD` | Redis Cluster auth password |
| `CLICKHOUSE_HOST` | ClickHouse host:port |
| `CLICKHOUSE_USERNAME` | ClickHouse user |
| `CLICKHOUSE_PASSWORD` | ClickHouse password |
| `AUTH_SERVICE_URL` | Internal URL of the Go auth microservice |
| `JWT_SECRET` | Secret used to validate WebSocket client JWTs |
| `JAEGER_ENDPOINT` | Jaeger collector endpoint (e.g. `http://jaeger:14268/api/traces`) |

---

## 4. Development Environment

### 4.1 Start infrastructure with Docker Compose

Create `docker-compose.dev.yml` in the repo root:

```yaml
version: "3.9"
services:
  redis-01:
    image: redis:7-alpine
    command: redis-server --cluster-enabled yes --cluster-config-file nodes.conf --cluster-node-timeout 5000 --appendonly yes --requirepass devpassword
    ports: ["6379:6379"]

  redis-02:
    image: redis:7-alpine
    command: redis-server --cluster-enabled yes --cluster-config-file nodes.conf --cluster-node-timeout 5000 --appendonly yes --requirepass devpassword --port 6380
    ports: ["6380:6380"]

  redis-03:
    image: redis:7-alpine
    command: redis-server --cluster-enabled yes --cluster-config-file nodes.conf --cluster-node-timeout 5000 --appendonly yes --requirepass devpassword --port 6381
    ports: ["6381:6381"]

  clickhouse:
    image: clickhouse/clickhouse-server:23-alpine
    ports: ["8123:8123", "9000:9000"]
    environment:
      CLICKHOUSE_USER: dev
      CLICKHOUSE_PASSWORD: devpassword
      CLICKHOUSE_DB: default

  mosquitto:
    image: eclipse-mosquitto:2
    ports: ["1883:1883"]
    volumes:
      - ./mosquitto.dev.conf:/mosquitto/config/mosquitto.conf

  jaeger:
    image: jaegertracing/all-in-one:latest
    ports: ["16686:16686", "14268:14268"]
```

```bash
docker compose -f docker-compose.dev.yml up -d
```

### 4.2 Initialise ClickHouse schema

```bash
cat batch-writer/clickhouse-gps_schema.sql | \
  docker exec -i $(docker ps -qf name=clickhouse) \
  clickhouse-client --user dev --password devpassword
```

### 4.3 Set environment variables

Create a `.env.dev` file (never commit this):

```bash
# .env.dev
USE_GO_RUN=true
BINARY_DIR=.

MQTT_BROKER=localhost:1883
MQTT_USERNAME=dev
MQTT_PASSWORD=devpassword

REDIS_PASSWORD=devpassword

CLICKHOUSE_HOST=localhost:8123
CLICKHOUSE_USERNAME=dev
CLICKHOUSE_PASSWORD=devpassword

AUTH_SERVICE_URL=http://localhost:8090
JWT_SECRET=dev-jwt-secret-change-in-prod

JAEGER_ENDPOINT=http://localhost:14268/api/traces

# Smaller replica counts for local development
MQTT_CONSUMER_MIN=1
MQTT_CONSUMER_MAX=4
BATCH_WRITER_MIN=1
BATCH_WRITER_MAX=2
WS_GATEWAY_MIN=1
WS_GATEWAY_MAX=2

SCALE_UP_LAG=1000
SCALE_DOWN_LAG=100
SCALE_INTERVAL=10s
```

### 4.4 Run the orchestrator

```bash
# Load env and start
set -a && source .env.dev && set +a
go run ./main.go
```

You will see prefixed log lines from each child process:

```
[batch-writer-0] Starting Batch Writer for GPS architecture...
[mqtt-consumer-0] Starting MQTT Consumer for GPS architecture...
[websocket-gateway-0] Starting WebSocket Gateway for GPS architecture...
```

### 4.5 Verify it is working

```bash
# Orchestrator control plane
curl http://localhost:7070/health
# → {"status":"ok","pools":{"batch-writer":{"healthy":1,"total":1},...}}

# Trigger a manual scale-up to test the auto-scaler
curl -X POST "http://localhost:7070/scale?service=mqtt-consumer&replicas=2"

# Watch aggregated Prometheus metrics
curl http://localhost:7070/metrics | grep orchestrator_
```

### 4.6 Run tests

Each service has its own test suite. Run them individually:

```bash
go test ./mqtt-consumer/...
go test ./batch-writer/...
go test ./websocket-gateway/...
```

---

## 5. Production Environment

### 5.1 Build binaries

```bash
# From the repo root
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
  go build -ldflags="-s -w" -o bin/mqtt-consumer      ./mqtt-consumer
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
  go build -ldflags="-s -w" -o bin/batch-writer        ./batch-writer
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
  go build -ldflags="-s -w" -o bin/websocket-gateway   ./websocket-gateway
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
  go build -ldflags="-s -w" -o bin/orchestrator        .
```

### 5.2 Dockerfile

```dockerfile
FROM golang:1.21-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o bin/mqtt-consumer      ./mqtt-consumer && \
    CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o bin/batch-writer        ./batch-writer  && \
    CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o bin/websocket-gateway   ./websocket-gateway && \
    CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o bin/orchestrator        .

FROM gcr.io/distroless/static:nonroot
WORKDIR /app
COPY --from=builder /app/bin/ ./bin/

ENV BINARY_DIR=/app/bin
ENV USE_GO_RUN=false

EXPOSE 7070
ENTRYPOINT ["/app/bin/orchestrator"]
```

Build and push:

```bash
docker build -t your-registry/matatu-pulse-gps:$(git rev-parse --short HEAD) .
docker push your-registry/matatu-pulse-gps:$(git rev-parse --short HEAD)
```

### 5.3 Kubernetes deployment

#### Secrets

```bash
kubectl create secret generic gps-pipeline-secrets \
  --from-literal=MQTT_PASSWORD="$MQTT_PASSWORD" \
  --from-literal=REDIS_PASSWORD="$REDIS_PASSWORD" \
  --from-literal=CLICKHOUSE_PASSWORD="$CLICKHOUSE_PASSWORD" \
  --from-literal=JWT_SECRET="$JWT_SECRET" \
  -n matatu-pulse
```

#### Deployment manifest

```yaml
# k8s/gps-orchestrator-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: gps-orchestrator
  namespace: matatu-pulse
  labels:
    app: gps-orchestrator
spec:
  replicas: 1           # One orchestrator pod manages all child processes
  selector:
    matchLabels:
      app: gps-orchestrator
  template:
    metadata:
      labels:
        app: gps-orchestrator
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "7070"
        prometheus.io/path: "/metrics"
    spec:
      terminationGracePeriodSeconds: 120   # Allows batch-writer drain window

      containers:
        - name: orchestrator
          image: your-registry/matatu-pulse-gps:abc1234
          imagePullPolicy: Always

          ports:
            - name: control-plane
              containerPort: 7070

          env:
            - name: BINARY_DIR
              value: /app/bin
            - name: USE_GO_RUN
              value: "false"
            - name: CONTROL_PLANE_ADDR
              value: ":7070"

            # Replica counts — tune per load test results
            - name: MQTT_CONSUMER_MIN
              value: "8"
            - name: MQTT_CONSUMER_MAX
              value: "32"
            - name: BATCH_WRITER_MIN
              value: "2"
            - name: BATCH_WRITER_MAX
              value: "8"
            - name: WS_GATEWAY_MIN
              value: "4"
            - name: WS_GATEWAY_MAX
              value: "10"

            # Auto-scaler thresholds (tuned for 100K vehicles)
            - name: SCALE_UP_LAG
              value: "50000"
            - name: SCALE_DOWN_LAG
              value: "5000"
            - name: SCALE_INTERVAL
              value: "15s"

            # Infrastructure endpoints
            - name: MQTT_BROKER
              value: "mosquitto.matatu-pulse.svc.cluster.local:1883"
            - name: CLICKHOUSE_HOST
              value: "clickhouse.matatu-pulse.svc.cluster.local:8123"
            - name: AUTH_SERVICE_URL
              value: "http://auth-service.matatu-pulse.svc.cluster.local:8090"
            - name: JAEGER_ENDPOINT
              value: "http://jaeger-collector.observability.svc.cluster.local:14268/api/traces"

            # Secrets
            - name: MQTT_USERNAME
              valueFrom:
                secretKeyRef:
                  name: gps-pipeline-secrets
                  key: MQTT_USERNAME
            - name: MQTT_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: gps-pipeline-secrets
                  key: MQTT_PASSWORD
            - name: REDIS_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: gps-pipeline-secrets
                  key: REDIS_PASSWORD
            - name: CLICKHOUSE_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: gps-pipeline-secrets
                  key: CLICKHOUSE_PASSWORD
            - name: JWT_SECRET
              valueFrom:
                secretKeyRef:
                  name: gps-pipeline-secrets
                  key: JWT_SECRET

          resources:
            requests:
              cpu: "4"        # Headroom for 32 MQTT consumer workers
              memory: "4Gi"
            limits:
              cpu: "16"
              memory: "16Gi"

          livenessProbe:
            httpGet:
              path: /health
              port: 7070
            initialDelaySeconds: 30
            periodSeconds: 15
            failureThreshold: 3

          readinessProbe:
            httpGet:
              path: /health
              port: 7070
            initialDelaySeconds: 20
            periodSeconds: 10

---
apiVersion: v1
kind: Service
metadata:
  name: gps-orchestrator
  namespace: matatu-pulse
spec:
  selector:
    app: gps-orchestrator
  ports:
    - name: control-plane
      port: 7070
      targetPort: 7070
  type: ClusterIP
```

Apply:

```bash
kubectl apply -f k8s/gps-orchestrator-deployment.yaml
kubectl rollout status deployment/gps-orchestrator -n matatu-pulse
```

### 5.4 Verify production deployment

```bash
# Port-forward the control plane
kubectl port-forward svc/gps-orchestrator 7070:7070 -n matatu-pulse &

# Full health check
curl http://localhost:7070/health | jq .

# Detailed per-instance status
curl http://localhost:7070/status | jq .

# Current stream lag and replica counts
curl http://localhost:7070/metrics | grep -E 'orchestrator_(stream_lag|pool_replicas)'
```

### 5.5 Production scaling recommendations

Derived from the architecture doc capacity model (100 000 vehicles at 5 s intervals = 20 000 events/sec):

| Pool | Recommended starting replicas | CPU per replica | Memory per replica |
|---|---|---|---|
| `mqtt-consumer` | 8 | 1 core | 512 Mi |
| `batch-writer` | 4 | 2 cores | 1 Gi |
| `websocket-gateway` | 4 | 1 core | 512 Mi |

Allow the auto-scaler to adjust `mqtt-consumer` upward during peak hours. Monitor `orchestrator_stream_lag_messages` in Grafana and set an alert at 100 000 (5-minute buffer ceiling).

---

## 6. Control Plane API

The control plane listens on `CONTROL_PLANE_ADDR` (default `:7070`).

### `GET /health`

Returns `200 OK` while all pools have at least one healthy replica. Returns `503` if any pool is fully down.

```jsonc
// 200 OK
{
  "status": "ok",
  "pools": {
    "mqtt-consumer":     { "healthy": 8, "total": 8 },
    "batch-writer":      { "healthy": 4, "total": 4 },
    "websocket-gateway": { "healthy": 4, "total": 4 }
  }
}
```

### `GET /status`

Per-instance detail including state, restart count, and uptime.

```jsonc
{
  "orchestrator_uptime_seconds": 3600,
  "stream_lag_messages": 1240,
  "pools": [
    {
      "name": "mqtt-consumer",
      "replicas": 8,
      "instances": [
        {
          "id": "mqtt-consumer-0",
          "state": "running",
          "metrics_port": 9100,
          "restarts": 0,
          "uptime_seconds": 3598
        }
      ]
    }
  ]
}
```

Instance states: `starting` · `running` · `unhealthy` · `stopped`

### `POST /scale?service=<name>&replicas=<n>`

Manually override the replica count. The auto-scaler continues to run and may override this on its next tick.

```bash
# Scale up before a known peak (e.g. evening rush hour)
curl -X POST "http://localhost:7070/scale?service=mqtt-consumer&replicas=24"

# Scale WebSocket gateways for an expected surge in dashboard clients
curl -X POST "http://localhost:7070/scale?service=websocket-gateway&replicas=8"
```

Valid service names: `mqtt-consumer`, `batch-writer`, `websocket-gateway`

Replicas are clamped to `[MinReplicas, MaxReplicas]` silently.

### `GET /metrics`

Standard Prometheus text format. Scrape this with your Prometheus instance.

Key metrics exposed:

| Metric | Type | Description |
|---|---|---|
| `orchestrator_pool_replicas{pool}` | Gauge | Current replica count per pool |
| `orchestrator_instance_healthy{instance}` | Gauge | 1 = healthy, 0 = unhealthy |
| `orchestrator_instance_restarts_total{pool}` | Counter | Cumulative restarts per pool |
| `orchestrator_stream_lag_messages` | Gauge | Max Redis batch stream lag across all batch-writer instances |

---

## 7. Auto-Scaling Logic

The scaler reads `batch_writer_redis_stream_lag` from every batch-writer `/metrics` endpoint every `SCALE_INTERVAL` (default 15 s). It takes the **maximum** observed value across all instances.

```
lag > SCALE_UP_LAG   → mqtt-consumer replicas += 2  (up to MaxReplicas)
lag < SCALE_DOWN_LAG → mqtt-consumer replicas -= 1  (down to MinReplicas)
```

WebSocket gateways scale as a trailing ratio: `ceil(mqtt_replicas / 3)`, minimum `WS_GATEWAY_MIN`.

Batch writers are **not** auto-scaled — their bottleneck is ClickHouse write throughput, not event volume. Scale them manually if `batch_writer_batch_write_duration_ms{quantile="0.99"}` exceeds 5 000 ms.

### Scaling thresholds for 100 000 vehicles

At steady state the batch stream holds ~100 s of buffered events (100 000 messages at 20 000/s). Conservative thresholds:

| Threshold | Value | Rationale |
|---|---|---|
| `SCALE_UP_LAG` | `50 000` | 2.5 s of buffer — consumers are falling behind |
| `SCALE_DOWN_LAG` | `5 000` | 0.25 s of buffer — comfortable headroom |

---

## 8. Shutdown Behaviour

On `SIGINT` or `SIGTERM` the orchestrator tears down the pipeline in reverse dependency order:

1. **WebSocket Gateway** — `SIGTERM` to all replicas, 30 s drain window. Existing WebSocket clients will disconnect; the JS client should handle reconnect.
2. **MQTT Consumer** — `SIGTERM` to all replicas, 30 s drain window. No more events enter the pipeline after this point.
3. **Batch Writer** — `SIGTERM` to all replicas, **60 s** drain window (double the others). This allows the service to flush all pending Redis stream messages to ClickHouse before exiting.

If any pool exceeds its drain window, surviving processes receive `SIGKILL`.

Set `terminationGracePeriodSeconds: 120` on the Kubernetes pod to give the full sequence time to complete.

---

## 9. Observability

### Prometheus scrape config

```yaml
# prometheus.yml
scrape_configs:
  - job_name: gps-orchestrator
    static_configs:
      - targets: ["gps-orchestrator.matatu-pulse.svc.cluster.local:7070"]
    metrics_path: /metrics

  # Individual service replicas (optional — fine-grained per-instance metrics)
  - job_name: mqtt-consumer
    static_configs:
      - targets:
          - "gps-orchestrator:9100"   # replica 0
          - "gps-orchestrator:9101"   # replica 1
          # ... up to replica 31 for MaxReplicas=32

  - job_name: batch-writer
    static_configs:
      - targets:
          - "gps-orchestrator:9200"
          - "gps-orchestrator:9201"

  - job_name: websocket-gateway
    static_configs:
      - targets:
          - "gps-orchestrator:9300"
          - "gps-orchestrator:9301"
```

### Recommended Grafana alerts

| Alert | Condition | Severity |
|---|---|---|
| Stream lag critical | `orchestrator_stream_lag_messages > 100000` | Critical |
| Pool has no healthy replicas | `min(orchestrator_instance_healthy) by (pool) == 0` | Critical |
| High restart rate | `rate(orchestrator_instance_restarts_total[5m]) > 0.1` | Warning |
| Batch write latency | `batch_writer_batch_write_duration_ms{quantile="0.99"} > 5000` | Warning |
| WebSocket client buffer pressure | `ws_client_buffer_pressure > 0.5` | Warning |

---

## 10. Troubleshooting

### A replica keeps restarting

Check the prefixed logs for the replica in question:

```bash
# Kubernetes
kubectl logs deployment/gps-orchestrator -n matatu-pulse | grep '\[mqtt-consumer-3\]'
```

The restart back-off grows exponentially up to `RESTART_BACKOFF_MAX`. If the underlying service binary is crashing on startup (e.g. bad env var), fix the root cause — the orchestrator will not give up retrying.

### Stream lag is climbing despite more MQTT consumers

The bottleneck has shifted to the **batch-writer** → ClickHouse path. Check:

```bash
curl http://localhost:7070/metrics | grep batch_writer_batch_write_duration_ms
```

If p99 > 5 000 ms, manually scale the batch-writer pool:

```bash
curl -X POST "http://localhost:7070/scale?service=batch-writer&replicas=6"
```

### Port conflicts when running locally

Each replica binds to a unique port (`BasePort + replicaIndex`). If you see `address already in use`, an old replica process did not exit cleanly. Find and kill it:

```bash
lsof -i :9100 -i :9101 -i :9200 | grep LISTEN
kill <pid>
```

### Redis stream lag is 0 but map positions are stale

The hot path (realtime stream → WebSocket gateway) is independent of the batch path. A lag of 0 on the batch stream does not indicate the realtime stream is healthy. Check WebSocket gateway logs:

```bash
kubectl logs deployment/gps-orchestrator | grep '\[websocket-gateway'
```

And check `ws_connections_active` — if it is 0, clients are not connected to the gateway.
