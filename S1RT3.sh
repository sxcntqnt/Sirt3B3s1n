#!/bin/bash
set -e
 

echo "🚀 Starting Sirt3B3s1n Build Sequence..."
 

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
 

export BINARY_DIR=$(pwd)/B3S1N
export USE_GO_RUN=false
 

#####################################################
# Redis Cluster config
#####################################################
 

CLUSTER_DIR="$(pwd)/cluster"
REDIS_SCRIPT="$CLUSTER_DIR/create-cluster.sh"
SEED_PORT=30001
 

REDIS_DATA_DIR="$CLUSTER_DIR/data"
 

mkdir -p "$REDIS_DATA_DIR"
 

#####################################################
# Helpers
#####################################################
 

is_redis_up() {
    redis-cli -p $SEED_PORT ping >/dev/null 2>&1
}
 

is_cluster_ok() {
    redis-cli -p $SEED_PORT cluster info 2>/dev/null | grep -q "cluster_state:ok"
}
 

start_cluster() {
    echo "🔧 Starting Redis cluster..."
 

    # IMPORTANT: force execution directory so logs don't pollute root
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
 

get_seed_node() {
    echo "127.0.0.1:30001"
}
 

#####################################################
# Env update (ONLY seed, no topology leakage)
#####################################################
 

update_env() {
    ENV_FILE=".env"
    SEED=$(get_seed_node)
 

    echo "🧾 Updating Redis entrypoint in $ENV_FILE"
 

    touch "$ENV_FILE"
 

    if grep -q "^REDIS_CLUSTER_SEED=" "$ENV_FILE"; then
        sed -i "s|^REDIS_CLUSTER_SEED=.*|REDIS_CLUSTER_SEED=$SEED|" "$ENV_FILE"
    else
        echo "REDIS_CLUSTER_SEED=$SEED" >> "$ENV_FILE"
    fi
 

    if grep -q "^REDIS_CLUSTER_ENABLED=" "$ENV_FILE"; then
        sed -i "s|^REDIS_CLUSTER_ENABLED=.*|REDIS_CLUSTER_ENABLED=true|" "$ENV_FILE"
    else
        echo "REDIS_CLUSTER_ENABLED=true" >> "$ENV_FILE"
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
