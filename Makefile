GO := go

.PHONY: proto build check test-unit test-node test-cluster clean

proto:
	bash api/proto/compile.sh

build:
	$(GO) build -o bin/rkv ./cmd/rkv

check:
	gofmt -w $$(find api cmd pkg test -type f -name '*.go')
	$(GO) vet ./...
	$(MAKE) test-unit
	$(MAKE) test-node
	$(MAKE) test-cluster

test-unit:
	$(GO) test -v -race -timeout 60s ./pkg/...

test-node: build
	rm -rf /tmp/rkv-data-1 /tmp/rkv-raft-1
	@set -e; \
	RKV_BIN=./bin/rkv bash test/scripts/start-node.sh 1 >/dev/null 2>&1 & \
	PID=$$!; \
	cleanup() { \
		kill -9 $$PID 2>/dev/null || true; \
		wait $$PID 2>/dev/null || true; \
		rm -rf /tmp/rkv-data-1 /tmp/rkv-raft-1; \
	}; \
	trap cleanup EXIT INT TERM; \
	READY=0; \
	for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20; do \
		if grpcurl -plaintext -max-time 1 127.0.0.1:13001 list rpcpb.KVService >/dev/null 2>&1; then \
			READY=1; \
			break; \
		fi; \
		sleep 0.3; \
	done; \
	if [ $$READY -ne 1 ]; then \
		echo "node did not become ready" >&2; \
		exit 1; \
	fi; \
	$(GO) test -v -count=1 -timeout 30s ./test/integration/

test-cluster: build
	$(GO) test -v -count=1 -timeout 120s ./test/cluster/

clean:
	rm -rf bin/
