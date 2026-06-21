package testutil

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/DecarbonizedGlucose/rkv/api/proto/pkg/kvpb"
	"github.com/DecarbonizedGlucose/rkv/api/proto/pkg/rpcpb"
	"github.com/DecarbonizedGlucose/rkv/pkg/client"
)

// Cluster holds a set of running rkv nodes started as subprocesses.
type Cluster struct {
	// Peers maps nodeID to gRPC address; pass directly to client.DefaultOptions().WithPeers.
	Peers map[uint64]string
	nodes []clusterNode
}

type clusterNode struct {
	id      uint64
	cmd     *exec.Cmd
	dataDir string
	raftDir string
}

// StartCluster starts n rkv nodes as subprocesses and blocks until all are ready.
// Cleanup (kill + temp dir removal) is registered via t.Cleanup.
//
// Requires ./bin/rkv to exist. Run `make build` first, or set RKV_BIN env var.
func StartCluster(t *testing.T, n int) *Cluster {
	t.Helper()

	bin := rkvBinPath(t)

	// Assign ports: raft on 12001..12n, gRPC on 13001..13n
	type addr struct{ raft, grpc string }
	addrs := make([]addr, n)
	peersArg := ""
	for i := 0; i < n; i++ {
		id := uint64(i + 1)
		addrs[i] = addr{
			raft: fmt.Sprintf("127.0.0.1:%d", 12000+id),
			grpc: fmt.Sprintf("127.0.0.1:%d", 13000+id),
		}
		if i > 0 {
			peersArg += ","
		}
		peersArg += fmt.Sprintf("%d=%s", id, addrs[i].raft)
	}

	peers := make(map[uint64]string, n)
	nodes := make([]clusterNode, n)

	for i := 0; i < n; i++ {
		id := uint64(i + 1)
		dataDir := t.TempDir()
		raftDir := t.TempDir()

		cmd := exec.Command(bin,
			fmt.Sprintf("-id=%d", id),
			fmt.Sprintf("-peers=%s", peersArg),
			fmt.Sprintf("-data-dir=%s", dataDir),
			fmt.Sprintf("-raft-dir=%s", raftDir),
			fmt.Sprintf("-raft-addr=%s", addrs[i].raft),
			fmt.Sprintf("-grpc-addr=%s", addrs[i].grpc),
		)
		// Suppress node output unless test is verbose
		if testing.Verbose() {
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
		}
		if err := cmd.Start(); err != nil {
			t.Fatalf("start node %d: %v", id, err)
		}
		nodes[i] = clusterNode{id: id, cmd: cmd, dataDir: dataDir, raftDir: raftDir}
		peers[id] = addrs[i].grpc
	}

	c := &Cluster{Peers: peers, nodes: nodes}
	t.Cleanup(c.Cleanup)

	// Wait for all nodes to accept gRPC connections
	probeCtx, probeCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer probeCancel()
	for i, nd := range nodes {
		if !probeReady(probeCtx, addrs[i].grpc) {
			t.Fatalf("node %d (%s) not ready within 15s", nd.id, addrs[i].grpc)
		}
	}

	// Wait for Raft leader election to complete
	leaderCtx, leaderCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer leaderCancel()
	if !waitForLeader(t, leaderCtx, peers) {
		t.Fatal("no leader elected within 15s")
	}

	return c
}

// Kill terminates the node with the given ID. Useful for leader failover tests.
func (c *Cluster) Kill(id uint64) {
	for _, nd := range c.nodes {
		if nd.id == id && nd.cmd.Process != nil {
			nd.cmd.Process.Kill()
			nd.cmd.Wait() //nolint:errcheck
		}
	}
}

// Cleanup kills all nodes. Called automatically via t.Cleanup.
func (c *Cluster) Cleanup() {
	for _, nd := range c.nodes {
		if nd.cmd.Process != nil {
			nd.cmd.Process.Kill()
			nd.cmd.Wait() //nolint:errcheck
		}
	}
}

// rkvBinPath returns the path to the rkv binary.
// Checks RKV_BIN env var first, then walks up to find the module root.
func rkvBinPath(t *testing.T) string {
	t.Helper()
	if bin := os.Getenv("RKV_BIN"); bin != "" {
		return bin
	}
	// Walk up from cwd to find go.mod (module root), then look for bin/rkv
	dir, _ := os.Getwd()
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			bin := filepath.Join(dir, "bin", "rkv")
			if _, err := os.Stat(bin); err != nil {
				t.Fatalf("rkv binary not found at %s; run `make build` first", bin)
			}
			return bin
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("could not find module root (go.mod not found)")
		}
		dir = parent
	}
}

// probeReady polls addr until the gRPC server is responding (any app-level
// response, including "not leader", counts as ready).
func probeReady(ctx context.Context, addr string) bool {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return false
	}
	defer conn.Close()
	c := rpcpb.NewKVServiceClient(conn)
	for {
		_, err := c.Get(ctx, &kvpb.GetRequest{Key: []byte("__probe__")})
		if err == nil {
			return true
		}
		// Any application-level error (e.g. FailedPrecondition="not leader") means
		// the gRPC server is up; only Unavailable means the TCP connection isn't ready.
		if st, ok := status.FromError(err); ok && st.Code() != codes.Unavailable {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(300 * time.Millisecond):
		}
	}
}

// waitForLeader polls each peer until one accepts a write (i.e. is the leader).
func waitForLeader(t *testing.T, ctx context.Context, peers map[uint64]string) bool {
	t.Helper()
	for {
		for _, addr := range peers {
			cli, err := client.NewClient(client.DefaultOptions().WithEndpoint(addr))
			if err != nil {
				continue
			}
			_, err = cli.Put(ctx, []byte("__leader-probe__"), []byte("1"), 0)
			cli.Close()
			if err == nil {
				return true
			}
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(300 * time.Millisecond):
		}
	}
}
