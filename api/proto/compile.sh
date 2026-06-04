#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR"
while [ ! -f "$PROJECT_ROOT/go.mod" ]; do
    PROJECT_ROOT="$(dirname "$PROJECT_ROOT")"
    if [ "$PROJECT_ROOT" = "/" ]; then
        echo "Error: go.mod not found." >&2
        exit 1
    fi
done

PROTO_DIR="$PROJECT_ROOT/api/proto"
MODULE="$(head -1 "$PROJECT_ROOT/go.mod" | awk '{print $2}')"

protoc \
    -I "$PROTO_DIR" \
    --go_out="$PROJECT_ROOT" \
    --go_opt="module=$MODULE" \
    --go-grpc_out="$PROJECT_ROOT" \
    --go-grpc_opt="module=$MODULE" \
    "$PROTO_DIR/kv.proto" \
    "$PROTO_DIR/raft.proto" \
    "$PROTO_DIR/rpc.proto"

echo "Protobuf compiled completely."
