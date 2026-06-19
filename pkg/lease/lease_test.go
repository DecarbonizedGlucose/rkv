package lease

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/DecarbonizedGlucose/rkv/api/proto/pkg/kvpb"
)

// capturedCmd collects commands passed to a mock proposeFn.
type capturedCmd struct {
	mu   sync.Mutex
	cmds []*kvpb.Command
}

func (c *capturedCmd) add(cmd *kvpb.Command) {
	c.mu.Lock()
	c.cmds = append(c.cmds, cmd)
	c.mu.Unlock()
}

func (c *capturedCmd) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.cmds)
}

func (c *capturedCmd) last() *kvpb.Command {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.cmds) == 0 {
		return nil
	}
	return c.cmds[len(c.cmds)-1]
}

func newTestLM(t *testing.T) (*LeaseManager, *capturedCmd, func()) {
	t.Helper()

	cfg := DefaultTimeWheelConfig()
	cfg.TickDuration = 10 * time.Millisecond // fast ticks for tests
	tw := NewTimeWheel(cfg)

	capt := &capturedCmd{}
	proposeFn := func(cmd *kvpb.Command) error {
		capt.add(cmd)
		return nil
	}

	lm := NewLeaseManager(tw, nil, proposeFn)

	cleanup := func() {
		lm.tw.Stop()
	}

	return lm, capt, cleanup
}

// ========================================
// Grant / Revoke
// ========================================

func TestGrant(t *testing.T) {
	lm, _, cleanup := newTestLM(t)
	defer cleanup()

	lm.Grant(1, 30)

	lm.mu.Lock()
	l, ok := lm.leases[1]
	lm.mu.Unlock()

	require.True(t, ok)
	assert.Equal(t, int64(1), l.ID)
	assert.Equal(t, int64(30), l.TTL)
	assert.NotZero(t, l.timerID, "timer should be scheduled")
}

func TestRevokeReturnsKeys(t *testing.T) {
	lm, _, cleanup := newTestLM(t)
	defer cleanup()

	lm.Grant(1, 30)
	lm.Attach(1, []byte("k1"))
	lm.Attach(1, []byte("k2"))

	keys := lm.Revoke(1)

	assert.Len(t, keys, 2)
	assert.Equal(t, []byte("k1"), keys[0])
	assert.Equal(t, []byte("k2"), keys[1])

	// lease removed from map
	lm.mu.Lock()
	_, ok := lm.leases[1]
	lm.mu.Unlock()
	assert.False(t, ok)
}

func TestRevokeUnknownLease(t *testing.T) {
	lm, _, cleanup := newTestLM(t)
	defer cleanup()

	keys := lm.Revoke(999)
	assert.Nil(t, keys, "unknown lease should return nil")
}

// ========================================
// Attach / Detach
// ========================================

func TestAttach(t *testing.T) {
	lm, _, cleanup := newTestLM(t)
	defer cleanup()

	lm.Grant(1, 30)
	lm.Attach(1, []byte("k1"))
	lm.Attach(1, []byte("k2"))
	lm.Attach(1, []byte("k1")) // duplicate — allowed at this layer (storage wins or loses based on ordering)

	lm.mu.Lock()
	l := lm.leases[1]
	lm.mu.Unlock()

	assert.Len(t, l.Keys, 3)
}

func TestAttachUnknownLeaseNoPanic(t *testing.T) {
	lm, _, cleanup := newTestLM(t)
	defer cleanup()

	// attaching to a lease that doesn't exist must not panic
	lm.Attach(999, []byte("orphan"))
}

func TestDetach(t *testing.T) {
	lm, _, cleanup := newTestLM(t)
	defer cleanup()

	lm.Grant(1, 30)
	lm.Attach(1, []byte("k1"))
	lm.Attach(1, []byte("k2"))
	lm.Attach(1, []byte("k3"))

	lm.Detach(1, []byte("k2"))

	lm.mu.Lock()
	l := lm.leases[1]
	lm.mu.Unlock()

	assert.Len(t, l.Keys, 2)
	assert.Equal(t, []byte("k1"), l.Keys[0])
	assert.Equal(t, []byte("k3"), l.Keys[1])
}

func TestDetachLastKey(t *testing.T) {
	lm, _, cleanup := newTestLM(t)
	defer cleanup()

	lm.Grant(1, 30)
	lm.Attach(1, []byte("only"))

	lm.Detach(1, []byte("only"))

	lm.mu.Lock()
	l := lm.leases[1]
	lm.mu.Unlock()

	assert.Empty(t, l.Keys, "lease should have no keys after detaching last one")
}

func TestDetachUnknownLeaseNoPanic(t *testing.T) {
	lm, _, cleanup := newTestLM(t)
	defer cleanup()

	lm.Detach(999, []byte("orphan"))
}

// ========================================
// LeaseGrant (gRPC handler)
// ========================================

func TestLeaseGrantAutoID(t *testing.T) {
	lm, capt, cleanup := newTestLM(t)
	defer cleanup()

	id, err := lm.LeaseGrant(0, 30)

	require.NoError(t, err)
	assert.Equal(t, int64(1), id)

	// verify proposeFn received LeaseGrant command
	cmd := capt.last()
	require.NotNil(t, cmd)
	grantCmd := cmd.GetLeaseGrant()
	require.NotNil(t, grantCmd)
	assert.Equal(t, int64(1), grantCmd.Id)
	assert.Equal(t, int64(30), grantCmd.Ttl)
}

func TestLeaseGrantCustomID(t *testing.T) {
	lm, capt, cleanup := newTestLM(t)
	defer cleanup()

	id, err := lm.LeaseGrant(42, 60)

	require.NoError(t, err)
	assert.Equal(t, int64(42), id)

	cmd := capt.last()
	require.NotNil(t, cmd)
	assert.Equal(t, int64(42), cmd.GetLeaseGrant().Id)
}

func TestLeaseGrantIDIncrement(t *testing.T) {
	lm, _, cleanup := newTestLM(t)
	defer cleanup()

	id1, _ := lm.LeaseGrant(0, 30)
	id2, _ := lm.LeaseGrant(0, 30)
	assert.Equal(t, int64(1), id1)
	assert.Equal(t, int64(2), id2)
}

// ========================================
// LeaseRevoke (gRPC handler)
// ========================================

func TestLeaseRevoke(t *testing.T) {
	lm, capt, cleanup := newTestLM(t)
	defer cleanup()

	err := lm.LeaseRevoke(5)
	require.NoError(t, err)

	cmd := capt.last()
	require.NotNil(t, cmd)
	revokeCmd := cmd.GetLeaseRevoke()
	require.NotNil(t, revokeCmd)
	assert.Equal(t, int64(5), revokeCmd.Id)
}

// ========================================
// onExpire
// ========================================

func TestOnExpireProposesLeaseRevoke(t *testing.T) {
	lm, capt, cleanup := newTestLM(t)
	defer cleanup()

	// Grant to create the lease (needed for onExpire to find it)
	lm.Grant(3, 1)

	lm.onExpire(3)

	cmd := capt.last()
	require.NotNil(t, cmd)
	assert.NotNil(t, cmd.GetLeaseRevoke(), "onExpire must propose LeaseRevoke")
	assert.Equal(t, int64(3), cmd.GetLeaseRevoke().Id)
}

func TestOnExpireUnknownLeaseNoProposal(t *testing.T) {
	lm, capt, cleanup := newTestLM(t)
	defer cleanup()

	before := capt.len()
	lm.onExpire(999)
	assert.Equal(t, before, capt.len(), "unknown lease expiry must not propose")
}

// ========================================
// KeepAlive (gRPC handler — local, needs leader)
// ========================================

func TestKeepAliveLeaseNotFound(t *testing.T) {
	lm, _, cleanup := newTestLM(t)
	defer cleanup()

	// node is nil → IsLeader() panics.
	// To test the lease-not-found path we need a node that IsLeader()==true,
	// then we could reach the map lookup. Without a real node we can only test
	// that a nil node panics (which it does).
	//
	// For integration: KeepAlive first checks IsLeader; if leader and lease
	// missing, returns ErrLeaseNotFound.
	//
	// This is an integration-test concern.
	_ = lm
}

// ========================================
// TimeWheel integration
// ========================================

func TestGrantTimerScheduled(t *testing.T) {
	lm, _, cleanup := newTestLM(t)
	defer cleanup()

	lm.Grant(10, 30)

	lm.mu.Lock()
	l := lm.leases[10]
	lm.mu.Unlock()

	assert.NotZero(t, l.timerID, "Grant must schedule a TimeWheel timer")
}

func TestOnExpireKeepsLeaseUntilRevoked(t *testing.T) {
	lm, _, cleanup := newTestLM(t)
	defer cleanup()

	lm.Grant(7, 30)
	lm.onExpire(7)

	// onExpire only proposes LeaseRevoke — the lease stays
	// until StateMachine applies the command and calls Revoke().
	lm.mu.Lock()
	_, ok := lm.leases[7]
	lm.mu.Unlock()

	assert.True(t, ok, "onExpire must NOT remove lease; removal is done by Revoke() called from StateMachine")
}

// ========================================
// LeaseManager full lifecycle
// ========================================

func TestLeaseLifecycle(t *testing.T) {
	lm, capt, cleanup := newTestLM(t)
	defer cleanup()

	// 1. Grant via Raft → apply Grant
	lm.Grant(1, 30)

	// 2. Attach keys via applyPut
	lm.Attach(1, []byte("k1"))
	lm.Attach(1, []byte("k2"))

	// 3. Revoke via Raft → apply Revoke returns keys
	keys := lm.Revoke(1)
	assert.Len(t, keys, 2)

	// 4. LeaseManager.Revoke does NOT propose deletes itself
	// (that's StateMachine's job via applyLeaseRevoke)
	assert.Equal(t, 0, capt.len(),
		"Revoke(apply-side) should not proposeFn; deletion handled by StateMachine")
}

// ========================================
// LeaseGrant → LeaseApply → LeaseRevoke via proposFn
// ========================================

func TestLeaseGrantRevokeViaProposeFn(t *testing.T) {
	lm, capt, cleanup := newTestLM(t)
	defer cleanup()

	// Simulate: client calls LeaseGrant → proposeFn → Raft → apply Grant
	_, err := lm.LeaseGrant(0, 30)
	require.NoError(t, err)
	// Simulate apply
	grantCmd := capt.last().GetLeaseGrant()
	lm.Grant(grantCmd.Id, grantCmd.Ttl)

	// Verify lease exists
	lm.mu.Lock()
	_, ok := lm.leases[grantCmd.Id]
	lm.mu.Unlock()
	assert.True(t, ok)

	// Simulate: client calls LeaseRevoke → proposeFn → Raft → apply Revoke
	err = lm.LeaseRevoke(grantCmd.Id)
	require.NoError(t, err)
	keys := lm.Revoke(grantCmd.Id)
	assert.NotNil(t, keys)
	assert.Empty(t, keys) // no keys attached yet
}

// ========================================
// KeepAlive: Leader gate
// ========================================

func TestKeepAliveNotLeader(t *testing.T) {
	lm, _, cleanup := newTestLM(t)
	defer cleanup()

	// Default isLeaderFn returns false when node is nil
	lm.Grant(5, 10)

	_, err := lm.KeepAlive(5)
	assert.ErrorIs(t, err, ErrNotLeader, "KeepAlive must reject non-leader")
}

func TestKeepAliveAsLeader(t *testing.T) {
	cfg := DefaultTimeWheelConfig()
	cfg.TickDuration = 10 * time.Millisecond
	tw := NewTimeWheel(cfg)

	capt := &capturedCmd{}
	lm := NewLeaseManager(tw, nil, func(cmd *kvpb.Command) error {
		capt.add(cmd)
		return nil
	})
	defer tw.Stop()

	// Override isLeaderFn → always leader
	lm.isLeaderFn = func() bool { return true }

	// Grant lease
	lm.Grant(5, 1)

	// KeepAlive via public API — must succeed
	ttl, err := lm.KeepAlive(5)
	require.NoError(t, err)
	assert.Equal(t, int64(1), ttl)

	// Verify timer was rescheduled (renew called internally)
	lm.mu.Lock()
	l := lm.leases[5]
	lm.mu.Unlock()
	assert.NotZero(t, l.timerID, "KeepAlive must re-schedule the timer")

	// Original TTL=1s → KeepAlive resets → must still be alive after 600ms
	time.Sleep(600 * time.Millisecond)
	assert.Equal(t, 0, capt.len(), "KeepAlive renewed timer: must NOT have expired at 600ms")

	// Wait past extended deadline (~1.6s from start)
	time.Sleep(800 * time.Millisecond)
	assert.Equal(t, 1, capt.len(), "renewed timer should expire after extended TTL")
	assert.NotNil(t, capt.last().GetLeaseRevoke())
}

// ========================================
// KeepAlive: lease not found
// ========================================

func TestKeepAliveLeaseNotFoundAsLeader(t *testing.T) {
	lm, _, cleanup := newTestLM(t)
	defer cleanup()

	lm.isLeaderFn = func() bool { return true }

	_, err := lm.KeepAlive(999)
	assert.ErrorIs(t, err, ErrLeaseNotFound)
}

// ========================================
// renew: internal renewal (no leader check)
// ========================================

func TestRenewContract(t *testing.T) {
	lm, capt, cleanup := newTestLM(t)
	defer cleanup()

	lm.Grant(5, 10)

	ttl, err := lm.renew(5)
	require.NoError(t, err)
	assert.Equal(t, int64(10), ttl)

	_, err = lm.renew(999)
	assert.ErrorIs(t, err, ErrLeaseNotFound)

	lm.mu.Lock()
	l := lm.leases[5]
	lm.mu.Unlock()
	assert.NotZero(t, l.timerID, "renew must re-schedule the timer")
	assert.Equal(t, int64(10), l.TTL, "TTL must not change after renew")
	assert.Equal(t, 0, capt.len(), "renew is local, must not propose")
}
