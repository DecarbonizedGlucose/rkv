#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

protoc \
    --proto_path=. \
    --go_out=. \
    --go_opt=paths=source_relative \
    internal/raft/proto/persist.proto