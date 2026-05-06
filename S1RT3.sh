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
go build -o $BUILD_DIR/batch-writer ./batch-writer
go build -o $BUILD_DIR/mqtt-consumer ./mqtt-consumer
go build -o $BUILD_DIR/websocket-gateway ./websocket-gateway
go build -o $BUILD_DIR/sirt3b3s1n ./main.go

echo "🧠 Build complete"

export ORCHESTRATOR_BINARY_DIR=$(pwd)/B3S1N
export ORCHESTRATOR_USE_GO_RUN=false

#####################################################
# Redis Cluster config (defaults only if missing)
#####################################################

CLUSTER_DIR="$(pwd)/cluster"
REDIS_SCRIPT="$CLUSTER_DIR/create-cluster.sh"
SEED_PORT=30001

REDIS_DATA_DIR="$CLUSTER_DIR/data"

mkdir -p "$REDIS_DATA_DIR"

: "${REDIS_CLUSTER_SEED:=127.0.0.1:30001}"
: "${REDIS_CLUSTER_ENABLED:=true}"

#####################################################
# ClickHouse config (defaults only if missing)
#####################################################

: "${CLICKHOUSE_HOST:=localhost}"
: "${CLICKHOUSE_PORT:=9000}"
: "${CLICKHOUSE_USERNAME:=default}"
: "${CLICKHOUSE_PASSWORD:=}"

#####################################################
# Helpers
#####################################################

is_redis_up() {
    redis-cli -p $SEED_PORT ping >/dev/null 2>&1
}

is_cluster_ok() {
    redis-cli -p $SEED_PORT cluster info 2>/dev/null | grep -q "cluster_state:ok"
}

is_clickhouse_ok() {
    echo "🔎 Testing ClickHouse at $CLICKHOUSE_HOST:$CLICKHOUSE_PORT (user=$CLICKHOUSE_USERNAME)"

    clickhouse-client \
        --host="$CLICKHOUSE_HOST" \
        --port="$CLICKHOUSE_PORT" \
        --user="$CLICKHOUSE_USERNAME" \
        --password="$CLICKHOUSE_PASSWORD" \
        --query "SELECT 1" >/dev/null 2>&1
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
# Env update (only persists VALIDATED values)
#####################################################

update_env() {
    ENV_FILE=".env"

    echo "🧾 Updating environment in $ENV_FILE"
    touch "$ENV_FILE"

    #############################################
    # Redis
    #############################################

    if grep -q "^REDIS_CLUSTER_SEED=" "$ENV_FILE"; then
        sed -i "s|^REDIS_CLUSTER_SEED=.*|REDIS_CLUSTER_SEED=$REDIS_CLUSTER_SEED|" "$ENV_FILE"
    else
        echo "REDIS_CLUSTER_SEED=$REDIS_CLUSTER_SEED" >> "$ENV_FILE"
    fi

    if grep -q "^REDIS_CLUSTER_ENABLED=" "$ENV_FILE"; then
        sed -i "s|^REDIS_CLUSTER_ENABLED=.*|REDIS_CLUSTER_ENABLED=$REDIS_CLUSTER_ENABLED|" "$ENV_FILE"
    else
        echo "REDIS_CLUSTER_ENABLED=$REDIS_CLUSTER_ENABLED" >> "$ENV_FILE"
    fi

    #############################################
    # ClickHouse (only if reachable)
    #############################################

    if is_clickhouse_ok; then
        echo "✅ ClickHouse OK — persisting config"

        grep -q "^CLICKHOUSE_HOST=" "$ENV_FILE" \
            && sed -i "s|^CLICKHOUSE_HOST=.*|CLICKHOUSE_HOST=$CLICKHOUSE_HOST|" "$ENV_FILE" \
            || echo "CLICKHOUSE_HOST=$CLICKHOUSE_HOST" >> "$ENV_FILE"

        grep -q "^CLICKHOUSE_PORT=" "$ENV_FILE" \
            && sed -i "s|^CLICKHOUSE_PORT=.*|CLICKHOUSE_PORT=$CLICKHOUSE_PORT|" "$ENV_FILE" \
            || echo "CLICKHOUSE_PORT=$CLICKHOUSE_PORT" >> "$ENV_FILE"

        grep -q "^CLICKHOUSE_USERNAME=" "$ENV_FILE" \
            && sed -i "s|^CLICKHOUSE_USERNAME=.*|CLICKHOUSE_USERNAME=$CLICKHOUSE_USERNAME|" "$ENV_FILE" \
            || echo "CLICKHOUSE_USERNAME=$CLICKHOUSE_USERNAME" >> "$ENV_FILE"

        grep -q "^CLICKHOUSE_PASSWORD=" "$ENV_FILE" \
            && sed -i "s|^CLICKHOUSE_PASSWORD=.*|CLICKHOUSE_PASSWORD=$CLICKHOUSE_PASSWORD|" "$ENV_FILE" \
            || echo "CLICKHOUSE_PASSWORD=$CLICKHOUSE_PASSWORD" >> "$ENV_FILE"
    else
        echo "⚠️ ClickHouse unreachable — not updating .env"
    fi
}

#####################################################
# Bootstrap logic
#####################################################

echo "🔍 Checking Redis cluster state..."

if is_redis_up; then
    echo "✅ Redis nodes already running"
else
    echo "⚠️ Redis not running — starting cluster"
    start_cluster
    sleep 3
fi

if is_cluster_ok; then
    echo "✅ Cluster already initialized"
else
    echo "⚠️ Cluster not initialized — creating"
    create_cluster
fi

update_env

#####################################################
# Launch orchestrator
#####################################################

echo "🔥 Launching Orchestrator..."
$BUILD_DIR/sirt3b3s1n
