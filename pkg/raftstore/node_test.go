package raftstore_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/DecarbonizedGlucose/rkv/api/proto/pkg/raftpb"
	"github.com/DecarbonizedGlucose/rkv/pkg/raft"
	"github.com/DecarbonizedGlucose/rkv/pkg/raft_transport"
	"github.com/DecarbonizedGlucose/rkv/pkg/raftstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ========================================
// Mock Transport
// ========================================

type mockTransport struct {
	recvCh chan *raftpb.RaftMessage
}

func newMockTransport() *mockTransport {
	return &mockTransport{recvCh: make(chan *raftpb.RaftMessage, 16)}
}

func (m *mockTransport) Send(msg *raftpb.RaftMessage) error { return nil }
func (m *mockTransport) Recv() <-chan *raftpb.RaftMessage    { return m.recvCh }
func (m *mockTransport) Start() error                        { return nil }
func (m *mockTransport) Stop() error                         { return nil }

var _ raft_transport.Transport = (*mockTransport)(nil)

// ========================================
// Mock StateMachine
// ========================================

type mockStateMachine struct {
	mu      sync.Mutex
	applied [][]byte
	pid     uint64 // 递增 ProposalID
}

func (sm *mockStateMachine) Apply(entries []*raftpb.Entry) (results []raftstore.ApplyResult, err error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	for _, e := range entries {
		sm.applied = append(sm.applied, e.Data)
	}
	results = make([]raftstore.ApplyResult, 0, len(entries))
	for _, e := range entries {
		pid := sm.pid
		sm.pid++
		results = append(results, raftstore.ApplyResult{ProposalID: pid, Data: e.Data})
	}
	return results, nil
}

func (sm *mockStateMachine) SnapshotData() ([]byte, error)           { return nil, nil }
func (sm *mockStateMachine) ApplySnapshot(*raftpb.Snapshot) error    { return nil }

func (sm *mockStateMachine) appliedData() [][]byte {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	out := make([][]byte, len(sm.applied))
	copy(out, sm.applied)
	return out
}

// ========================================
// Helpers
// ========================================

// newTestNode 创建一个单节点用于测试，Transport 为 mock。
func newTestNode(t *testing.T, id uint64, peers []uint64) (*raftstore.Node, *mockTransport, *mockStateMachine, func()) {
	t.Helper()

	storage := raft.NewMemoryStorage()
	// 预置 ConfState，避免 NewNode 的 bootstrap 路径尝试 CreateSnapshot(0, ...)
	storage.SetConfState(&raftpb.ConfState{Nodes: peers})
	tr := newMockTransport()
	sm := &mockStateMachine{}

	cfg := &raftstore.Config{
		RaftConfig: &raft.Config{
			ID:               id,
			Peers:            peers,
			ElectionTimeout:  10,
			HeartbeatTimeout: 3,
			Storage:          storage,
		},
		Storage:      newWrappedStorage(storage),
		Transport:    tr,
		StateMachine: sm,
		TickInterval: 10 * time.Millisecond,
	}

	n, err := raftstore.NewNode(cfg)
	require.NoError(t, err)

	cleanup := func() {
		n.Stop()
	}
	return n, tr, sm, cleanup
}

// ========================================
// Tests
// ========================================

func TestNodeStartAndBecomeLeader(t *testing.T) {
	n, _, _, cleanup := newTestNode(t, 1, []uint64{1})
	defer cleanup()

	require.Eventually(t, func() bool {
		return n.LeaderID() == 1
	}, 2*time.Second, 50*time.Millisecond, "single node should become Leader")

	result, err := n.Propose([]byte{0}, 0)
	require.NoError(t, err)
	assert.Equal(t, []byte{0}, result)
}

func TestNodeProposeAndApply(t *testing.T) {
	n, _, sm, cleanup := newTestNode(t, 1, []uint64{1})
	defer cleanup()

	require.Eventually(t, func() bool {
		return n.LeaderID() == 1
	}, 2*time.Second, 50*time.Millisecond)

	result, err := n.Propose([]byte("hello"), 0)
	require.NoError(t, err)
	assert.Equal(t, []byte("hello"), result)

	require.Eventually(t, func() bool {
		for _, d := range sm.appliedData() {
			if string(d) == "hello" {
				return true
			}
		}
		return false
	}, 2*time.Second, 50*time.Millisecond, "proposed data should be applied to state machine")
}

func TestNodeProposeNotLeader(t *testing.T) {
	n, _, _, cleanup := newTestNode(t, 1, []uint64{1, 2, 3})
	defer cleanup()

	require.Eventually(t, func() bool {
		_, err := n.Propose([]byte("x"), 0)
		return errors.Is(err, raft.ErrNotLeader)
	}, 2*time.Second, 50*time.Millisecond, "non-leader should reject propose with ErrNotLeader")
}

func TestNodeStop(t *testing.T) {
	n, _, _, cleanup := newTestNode(t, 1, []uint64{1})
	cleanup()

	assert.NotPanics(t, func() {
		n.Stop()
	})

	_, err := n.Propose([]byte("x"), 0)
	assert.ErrorIs(t, err, raftstore.ErrStopped)
}

func TestNodeLeaderID(t *testing.T) {
	n, _, _, cleanup := newTestNode(t, 1, []uint64{1, 2, 3})
	defer cleanup()

	assert.Equal(t, uint64(0), n.LeaderID())
}

func TestNodeMultipleProposes(t *testing.T) {
	n, _, sm, cleanup := newTestNode(t, 1, []uint64{1})
	defer cleanup()

	require.Eventually(t, func() bool {
		return n.LeaderID() == 1
	}, 2*time.Second, 50*time.Millisecond)

	for i := 0; i < 5; i++ {
		result, err := n.Propose([]byte{byte(i)}, uint64(i))
		require.NoError(t, err)
		assert.Equal(t, []byte{byte(i)}, result)
	}

	require.Eventually(t, func() bool {
		return len(sm.appliedData()) == 5
	}, 3*time.Second, 50*time.Millisecond, "all 5 entries should be applied")
}

// ========================================
// wrappedStorage adapts raft.MemoryStorage to raftstore.Storage
// ========================================

type wrappedStorage struct {
	*raft.MemoryStorage
}

func newWrappedStorage(ms *raft.MemoryStorage) raftstore.Storage {
	return &wrappedStorage{MemoryStorage: ms}
}

func (w *wrappedStorage) SaveHardState(hs *raftpb.HardState) error {
	w.MemoryStorage.SetHardState(hs)
	return nil
}

func (w *wrappedStorage) ApplySnapshot(snap *raftpb.Snapshot) error {
	return w.MemoryStorage.ApplySnapshot(snap)
}

func (w *wrappedStorage) CreateSnapshot(idx uint64, cs *raftpb.ConfState, data []byte) error {
	_, err := w.MemoryStorage.CreateSnapshot(idx, cs, data)
	return err
}

func (w *wrappedStorage) Append(entries []*raftpb.Entry) error {
	return w.MemoryStorage.Append(entries)
}

func (w *wrappedStorage) Compact(idx uint64) error {
	return w.MemoryStorage.Compact(idx)
}
