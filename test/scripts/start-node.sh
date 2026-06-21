#!/usr/bin/env bash
set -e

ID=${1:?usage: $0 <node-id>}
shift

# 其他节点作为参数传入，如: start-node.sh 1 2=127.0.0.1:12002 3=127.0.0.1:12003
# 不传则默认单节点
PORT_BASE=$((12000 + ID))
PEERS="1=127.0.0.1:12001"
for p in "$@"; do
    PEERS="$PEERS,$p"
done

ARGS=(
    -id="$ID"
    -peers="$PEERS"
    -data-dir="/tmp/rkv-data-${ID}"
    -raft-dir="/tmp/rkv-raft-${ID}"
    -raft-addr="127.0.0.1:${PORT_BASE}"
    -grpc-addr="127.0.0.1:$((PORT_BASE + 1000))"
)

mkdir -p "/tmp/rkv-data-${ID}" "/tmp/rkv-raft-${ID}"

if [ -n "${RKV_BIN:-}" ]; then
    exec "$RKV_BIN" "${ARGS[@]}"
else
    exec go run ./cmd/rkv "${ARGS[@]}"
fi
