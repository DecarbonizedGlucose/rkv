GO := go

.PHONY: proto build test-unit test-node clean

proto:
	bash api/proto/compile.sh

build:
	$(GO) build -o bin/rkv ./cmd/rkv

test-unit:
	$(GO) test -v -race -timeout 60s ./pkg/...

test-node: build
	rm -rf /tmp/rkv-*
	RKV_BIN=./bin/rkv bash test/scripts/start-node.sh 1 >/dev/null 2>&1 &
	PID=$$!; \
	for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20; do \
		grpcurl -plaintext -max-time 1 127.0.0.1:13001 list rpcpb.KVService >/dev/null 2>&1 && break; \
		sleep 0.3; \
	done; \
	$(GO) test -v -count=1 -timeout 30s ./test/integration/; \
	kill -9 $$PID 2>/dev/null; \
	rm -rf /tmp/rkv-*

clean:
	rm -rf bin/