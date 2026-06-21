package cluster

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/DecarbonizedGlucose/rkv/pkg/client"
	"github.com/DecarbonizedGlucose/rkv/test/testutil"
)

// newClient 创建连接整个集群的 SDK 客户端（自动 Leader 重定向）。
func newClient(t *testing.T, peers map[uint64]string) *client.Client {
	t.Helper()
	cli, err := client.NewClient(client.DefaultOptions().WithPeers(peers))
	require.NoError(t, err)
	t.Cleanup(func() { cli.Close() })
	return cli
}

// TestThreeNodeClusterElection 验证三节点集群能选出 Leader 并接受写入。
func TestThreeNodeClusterElection(t *testing.T) {
	cluster := testutil.StartCluster(t, 3)
	cli := newClient(t, cluster.Peers)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// SDK 自动重定向到 Leader；能写成功即证明选举完成。
	resp, err := cli.Put(ctx, []byte("election-probe"), []byte("1"), 0)
	require.NoError(t, err, "cluster should elect a leader and accept writes")
	assert.NotZero(t, resp.Header.MemberId)
	assert.Greater(t, resp.Header.RaftTerm, uint64(0))
}

// TestLeaderFailover 验证 Leader 宕机后集群在 5s 内选出新 Leader 并恢复写入。
func TestLeaderFailover(t *testing.T) {
	cluster := testutil.StartCluster(t, 3)
	cli := newClient(t, cluster.Peers)

	ctx := context.Background()

	// 触发一次写入，从响应头得知当前 Leader 的 nodeID。
	resp, err := cli.Put(ctx, []byte("failover-key"), []byte("before"), 0)
	require.NoError(t, err)
	leaderID := resp.Header.MemberId
	t.Logf("current leader: node %d", leaderID)

	// Kill Leader。
	cluster.Kill(leaderID)

	// 5s 内应选出新 Leader 并接受写入。
	require.Eventually(t, func() bool {
		_, err := cli.Put(ctx, []byte("failover-key"), []byte("after"), 0)
		return err == nil
	}, 5*time.Second, 300*time.Millisecond, "new leader should be elected within 5s")
}

// TestLogReplication 验证写入 Leader 的数据在 Leader 宕机后仍可通过新 Leader 读取。
func TestLogReplication(t *testing.T) {
	cluster := testutil.StartCluster(t, 3)
	cli := newClient(t, cluster.Peers)

	ctx := context.Background()

	// 写入 10 条数据，记录处理这些写入的 Leader。
	const n = 10
	keys := make([][]byte, n)
	var leaderID uint64
	for i := range n {
		keys[i] = []byte(fmt.Sprintf("rep-key-%02d", i))
		resp, err := cli.Put(ctx, keys[i], []byte(fmt.Sprintf("val-%d", i)), 0)
		require.NoError(t, err)
		leaderID = resp.Header.MemberId
	}
	t.Logf("wrote %d keys via leader node %d", n, leaderID)

	// Kill Leader，强制客户端切换到已复制数据的 Follower。
	cluster.Kill(leaderID)

	// 等待新 Leader 就绪。
	require.Eventually(t, func() bool {
		_, err := cli.Get(ctx, keys[0])
		return err == nil
	}, 5*time.Second, 300*time.Millisecond, "new leader should be ready within 5s")

	// 验证所有数据均已复制。
	for i, k := range keys {
		resp, err := cli.Get(ctx, k)
		require.NoError(t, err)
		require.NotNil(t, resp.Kv, "key %s should survive leader failover", k)
		assert.Equal(t, []byte(fmt.Sprintf("val-%d", i)), resp.Kv.Value)
	}
}
