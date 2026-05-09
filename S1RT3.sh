#!/bin/bash
set -e

echo "🚀 Starting Sirt3B3s1n Build Sequence..."

#####################################################
# Load environment FIRST (critical fix)
#####################################################

if [ -f ".env" ]; then
    echo "📥 Loading existing .env"
    set -a
    source .env
    set +a
fi

#####################################################
# Build section
#####################################################

BUILD_DIR="./B3S1N"

mkdir -p "$BUILD_DIR"
rm -f "$BUILD_DIR"/*

echo "📦 Compiling services..."

go build -o "$BUILD_DIR/batch-writer"      ./batch-writer
go build -o "$BUILD_DIR/mqtt-consumer"     ./mqtt-consumer
go build -o "$BUILD_DIR/websocket-gateway" ./websocket-gateway
go build -o "$BUILD_DIR/sirt3b3s1n"        ./main.go

echo "🧠 Build complete"

export ORCHESTRATOR_BINARY_DIR="$(pwd)/B3S1N"
export ORCHESTRATOR_USE_GO_RUN=false

#####################################################
# Redis Cluster config
#####################################################

CLUSTER_DIR="$(pwd)/cluster"
REDIS_SCRIPT="$CLUSTER_DIR/create-cluster.sh"
SEED_PORT=30001
REDIS_DATA_DIR="$CLUSTER_DIR/data"

mkdir -p "$REDIS_DATA_DIR"

: "${REDIS_CLUSTER_SEED:=127.0.0.1:30001}"
: "${REDIS_CLUSTER_ENABLED:=true}"

#####################################################
# ClickHouse config
#
# Supports:
#   CLICKHOUSE_ADDR=localhost:9000
#   BATCH_WRITER_CLICKHOUSE_HOST=localhost:9000
#
# websocket-gateway REQUIRES:
#   WEBSOCKET_GATEWAY_CLICKHOUSE_HOST
# WITHOUT port.
#####################################################

: "${CLICKHOUSE_ADDR:=localhost:9000}"

# Backward compatibility
if [[ -n "${BATCH_WRITER_CLICKHOUSE_HOST:-}" ]]; then
    CLICKHOUSE_ADDR="$BATCH_WRITER_CLICKHOUSE_HOST"
fi

# Split host:port
CLICKHOUSE_HOST_PARSED="${CLICKHOUSE_ADDR%%:*}"
CLICKHOUSE_PORT_PARSED="${CLICKHOUSE_ADDR##*:}"

# Fallback if no explicit port
if [[ "$CLICKHOUSE_HOST_PARSED" == "$CLICKHOUSE_PORT_PARSED" ]]; then
    CLICKHOUSE_PORT_PARSED="9000"
fi

: "${CLICKHOUSE_HOST:=$CLICKHOUSE_HOST_PARSED}"
: "${CLICKHOUSE_PORT:=$CLICKHOUSE_PORT_PARSED}"

: "${CLICKHOUSE_USERNAME:=${BATCH_WRITER_CLICKHOUSE_USERNAME:-default}}"
: "${CLICKHOUSE_PASSWORD:=${BATCH_WRITER_CLICKHOUSE_PASSWORD:-}}"

#####################################################
# websocket-gateway compatibility exports
#####################################################

export WEBSOCKET_GATEWAY_CLICKHOUSE_HOST="$CLICKHOUSE_HOST"
export WEBSOCKET_GATEWAY_CLICKHOUSE_USERNAME="$CLICKHOUSE_USERNAME"
export WEBSOCKET_GATEWAY_CLICKHOUSE_PASSWORD="$CLICKHOUSE_PASSWORD"

#####################################################
# batch-writer compatibility exports
#####################################################

export BATCH_WRITER_CLICKHOUSE_HOST="${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT}"
export BATCH_WRITER_CLICKHOUSE_USERNAME="$CLICKHOUSE_USERNAME"
export BATCH_WRITER_CLICKHOUSE_PASSWORD="$CLICKHOUSE_PASSWORD"

#####################################################
# MQTT / rmqtt cluster config
#####################################################

: "${MQTT_HOST:=localhost}"
: "${MQTT_PORT:=1883}"
: "${MQTT_BROKER:=${MQTT_HOST}:${MQTT_PORT}}"
: "${MQTT_CLUSTER_NODES:=${MQTT_HOST}:${MQTT_PORT}}"
: "${MQTT_USERNAME:=}"
: "${MQTT_PASSWORD:=}"
: "${MQTT_TLS:=false}"

export MQTT_BROKER
export MQTT_USERNAME
export MQTT_PASSWORD
export MQTT_TLS
export MQTT_CLUSTER_NODES

#####################################################
# Helpers
#####################################################

is_redis_up() {
    redis-cli -p "$SEED_PORT" ping >/dev/null 2>&1
}

is_cluster_ok() {
    redis-cli -p "$SEED_PORT" cluster info 2>/dev/null | grep -q "cluster_state:ok"
}

is_clickhouse_ok() {
    echo "🔎 Testing ClickHouse at ${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT} (user=${CLICKHOUSE_USERNAME})"

    clickhouse-client \
        --host="$CLICKHOUSE_HOST" \
        --port="$CLICKHOUSE_PORT" \
        --user="$CLICKHOUSE_USERNAME" \
        --password="$CLICKHOUSE_PASSWORD" \
        --query "SELECT 1" \
        >/dev/null 2>&1
}

#####################################################
# MQTT health probe
#####################################################

is_mqtt_ok() {
    local host="${MQTT_HOST}"
    local port="${MQTT_PORT}"

    echo "🔎 Testing MQTT broker at $host:$port (user=${MQTT_USERNAME:-<none>})"

    if command -v mosquitto_pub >/dev/null 2>&1; then
        local auth_args=()

        [[ -n "$MQTT_USERNAME" ]] && auth_args+=(-u "$MQTT_USERNAME")
        [[ -n "$MQTT_PASSWORD" ]] && auth_args+=(-P "$MQTT_PASSWORD")

        mosquitto_pub \
            -h "$host" \
            -p "$port" \
            "${auth_args[@]}" \
            -t "sirt3/health" \
            -m "ping" \
            --timeout 2 \
            -q 0 \
            >/dev/null 2>&1
    else
        (echo >/dev/tcp/"$host"/"$port") >/dev/null 2>&1
    fi
}

probe_mqtt_cluster() {
    echo "🔍 Probing rmqtt cluster nodes: $MQTT_CLUSTER_NODES"

    local ok=0
    local fail=0

    IFS=',' read -ra NODES <<< "$MQTT_CLUSTER_NODES"

    for node in "${NODES[@]}"; do
        local h
        local p

        h="${node%%:*}"
        p="${node##*:}"

        if (echo >/dev/tcp/"$h"/"$p") >/dev/null 2>&1; then
            echo "  ✅ $node"
            (( ok++ )) || true
        else
            echo "  ❌ $node — unreachable"
            (( fail++ )) || true
        fi
    done

    echo "📊 Cluster probe: $ok up / $fail down"

    [[ "$ok" -gt 0 ]]
}

start_cluster() {
    echo "🔧 Starting Redis cluster..."

    pushd "$CLUSTER_DIR" >/dev/null

    bash create-cluster.sh clean
    bash create-cluster.sh start

    popd >/dev/null
}

create_cluster() {
    echo "🧩 Creating Redis cluster..."

    pushd "$CLUSTER_DIR" >/dev/null

    bash create-cluster.sh create

    popd >/dev/null
}

#####################################################
# Env update helpers
#####################################################

upsert_env() {
    local key="$1"
    local val="$2"
    local file="$3"

    if grep -q "^${key}=" "$file"; then
        sed -i "s|^${key}=.*|${key}=${val}|" "$file"
    else
        echo "${key}=${val}" >> "$file"
    fi
}

update_env() {
    ENV_FILE=".env"

    echo "🧾 Updating environment in $ENV_FILE"

    touch "$ENV_FILE"

    #################################################
    # Redis
    #################################################

    upsert_env "REDIS_CLUSTER_SEED"    "$REDIS_CLUSTER_SEED"    "$ENV_FILE"
    upsert_env "REDIS_CLUSTER_ENABLED" "$REDIS_CLUSTER_ENABLED" "$ENV_FILE"

    #################################################
    # ClickHouse
    #################################################

    if is_clickhouse_ok; then
        echo "✅ ClickHouse OK — persisting config"

        # Generic/internal
        upsert_env "CLICKHOUSE_HOST" \
            "$CLICKHOUSE_HOST" \
            "$ENV_FILE"

        upsert_env "CLICKHOUSE_PORT" \
            "$CLICKHOUSE_PORT" \
            "$ENV_FILE"

        upsert_env "CLICKHOUSE_USERNAME" \
            "$CLICKHOUSE_USERNAME" \
            "$ENV_FILE"

        upsert_env "CLICKHOUSE_PASSWORD" \
            "$CLICKHOUSE_PASSWORD" \
            "$ENV_FILE"

        # batch-writer compatibility
        upsert_env "BATCH_WRITER_CLICKHOUSE_HOST" \
            "${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT}" \
            "$ENV_FILE"

        upsert_env "BATCH_WRITER_CLICKHOUSE_USERNAME" \
            "$CLICKHOUSE_USERNAME" \
            "$ENV_FILE"

        upsert_env "BATCH_WRITER_CLICKHOUSE_PASSWORD" \
            "$CLICKHOUSE_PASSWORD" \
            "$ENV_FILE"

        # websocket-gateway compatibility
        upsert_env "WEBSOCKET_GATEWAY_CLICKHOUSE_HOST" \
            "$CLICKHOUSE_HOST" \
            "$ENV_FILE"

        upsert_env "WEBSOCKET_GATEWAY_CLICKHOUSE_USERNAME" \
            "$CLICKHOUSE_USERNAME" \
            "$ENV_FILE"

        upsert_env "WEBSOCKET_GATEWAY_CLICKHOUSE_PASSWORD" \
            "$CLICKHOUSE_PASSWORD" \
            "$ENV_FILE"

    else
        echo "⚠️  ClickHouse unreachable — not updating .env"
    fi

    #################################################
    # MQTT / rmqtt cluster
    #################################################

    if is_mqtt_ok; then
        echo "✅ MQTT broker OK — persisting config"

        upsert_env "MQTT_HOST"          "$MQTT_HOST"          "$ENV_FILE"
        upsert_env "MQTT_PORT"          "$MQTT_PORT"          "$ENV_FILE"
        upsert_env "MQTT_BROKER"        "$MQTT_BROKER"        "$ENV_FILE"
        upsert_env "MQTT_CLUSTER_NODES" "$MQTT_CLUSTER_NODES" "$ENV_FILE"
        upsert_env "MQTT_USERNAME"      "$MQTT_USERNAME"      "$ENV_FILE"
        upsert_env "MQTT_PASSWORD"      "$MQTT_PASSWORD"      "$ENV_FILE"
        upsert_env "MQTT_TLS"           "$MQTT_TLS"           "$ENV_FILE"

        probe_mqtt_cluster || true
    else
        echo "⚠️  MQTT broker unreachable at ${MQTT_HOST}:${MQTT_PORT}"
        echo "    mqtt-consumer will retry after startup."
    fi
}

#####################################################
# Bootstrap logic
#####################################################

echo "🔍 Checking Redis cluster state..."

if is_redis_up; then
    echo "✅ Redis nodes already running"
else
    echo "⚠️  Redis not running — starting cluster"

    start_cluster

    sleep 3
fi

if is_cluster_ok; then
    echo "✅ Cluster already initialized"
else
    echo "⚠️  Cluster not initialized — creating"

    create_cluster
fi

echo "🔍 Checking MQTT broker state..."

if is_mqtt_ok; then
    echo "✅ MQTT broker reachable"
else
    echo "⚠️  MQTT broker not reachable at ${MQTT_HOST}:${MQTT_PORT}"
    echo "    mqtt-consumer will retry on startup — proceeding anyway."
fi

update_env

#####################################################
# Launch orchestrator
#####################################################

echo "🔥 Launching Orchestrator..."

exec "$BUILD_DIR/sirt3b3s1n"

