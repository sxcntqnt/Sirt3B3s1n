#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Define the target directory
BUILD_DIR="./B3S1N"

echo "🚀 Starting Sirt3B3s1n Build Sequence..."

# 1. Clean/Create build directory
mkdir -p $BUILD_DIR
rm -f $BUILD_DIR/*

# 2. Compile Child Services
echo "📦 Compiling child services..."
go build -o $BUILD_DIR/batch-writer ./batch-writer
go build -o $BUILD_DIR/mqtt-consumer ./mqtt-consumer
go build -o $BUILD_DIR/websocket-gateway ./websocket-gateway

# 3. Compile the Orchestrator itself
echo "🧠 Compiling Sirt3B3s1n orchestrator..."
go build -o $BUILD_DIR/sirt3b3s1n ./main.go

echo "✅ Build complete. Binaries located in $BUILD_DIR"
echo "------------------------------------------------"

# 4. Execution setup
# We point BINARY_DIR to the absolute path of our build folder
export BINARY_DIR=$(pwd)/B3S1N
export USE_GO_RUN=false

echo "🔥 Launching Orchestrator..."
$BUILD_DIR/sirt3b3s1n
