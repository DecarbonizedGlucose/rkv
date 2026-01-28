#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

# generate raftrpc pb files
protoc \
    --proto_path=. \
    --go_out=. \
    --go_opt=paths=source_relative \
    --go-grpc_out=. \
    --go-grpc_opt=paths=source_relative \
    api/raftrpc/consensus.proto \
    api/raftrpc/persistence.proto

# generate raft applier pb files
protoc \
    --proto_path=. \
    --go_out=. \
    --go_opt=paths=source_relative \
    api/raftapplier/raftapplier.proto

# generate kvrpc pb files
protoc \
    --proto_path=. \
    --go_out=. \
    --go_opt=paths=source_relative \
    --go-grpc_out=. \
    --go-grpc_opt=paths=source_relative \
    api/kvrpc/kv.proto
