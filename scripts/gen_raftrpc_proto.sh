#!/bin/bash

PROJECT_ROOT="$PWD/.."
cd $PROJECT_ROOT

# generate raftrpc pb files
protoc \
    --proto_path=. \
    --go_out=. \
    --go_opt=paths=source_relative \
    --go-grpc_out=. \
    --go-grpc_opt=paths=source_relative \
    api/raftrpc/consensus.proto \
    api/raftrpc/snapshot.proto

# generate rsm pb files
protoc \
    --proto_path=. \
    --go_out=. \
    --go_opt=paths=source_relative \
    api/rsm/rsm.proto

# generate kvrpc pb files
protoc \
    --proto_path=. \
    --go_out=. \
    --go_opt=paths=source_relative \
    --go-grpc_out=. \
    --go-grpc_opt=paths=source_relative \
    api/kvrpc/kv.proto
