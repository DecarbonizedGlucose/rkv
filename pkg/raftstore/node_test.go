package raftstore_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/DecarbonizedGlucose/rkv/api/proto/pkg/raftpb"
	"github.com/DecarbonizedGlucose/rkv/pkg/raft"
	"github.com/DecarbonizedGlucose/rkv/pkg/raft_transport"
	"github.com/DecarbonizedGlucose/rkv/pkg/raftstore"
)

// ========================================
// Mock Transport
// ========================================

type mockTransport struct {
	recvCh chan *raftpb.RaftMessage
	sentCh chan *raftpb.RaftMessage
}

func newMockTransport() *mockTransport {
	return &mockTransport{
		recvCh: make(chan *raftpb.RaftMessage, 16),
		sentCh: make(chan *raftpb.RaftMessage, 64),
	}
}

func (m *mockTransport) Send(msg *raftpb.RaftMessage) error {
	select {
	case m.sentCh <- msg:
	default:
	}
	return nil
}
func (m *mockTransport) Recv() <-chan *raftpb.RaftMessage { return m.recvCh }
func (m *mockTransport) Start() error                     { return nil }
func (m *mockTransport) Stop() error                      { return nil }

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
	results = make([]raftstore.ApplyResult, 0, len(entries))
	for _, e := range entries {
		if len(e.Data) == 0 {
			continue // no-op entry (new leader commits pending entries from prev term)
		}
		sm.applied = append(sm.applied, e.Data)
		pid := sm.pid
		sm.pid++
		results = append(results, raftstore.ApplyResult{ProposalID: pid, Data: e.Data})
	}
	return results, nil
}

func (sm *mockStateMachine) SnapshotData() ([]byte, error)        { return nil, nil }
func (sm *mockStateMachine) ApplySnapshot(*raftpb.Snapshot) error { return nil }

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
	return newTestNodeWithTick(t, id, peers, 10*time.Millisecond)
}

func newTestNodeWithTick(t *testing.T, id uint64, peers []uint64, tick time.Duration) (*raftstore.Node, *mockTransport, *mockStateMachine, func()) {
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
		TickInterval: tick,
	}

	n, err := raftstore.NewNode(cfg)
	require.NoError(t, err)

	cleanup := func() {
		n.Stop()
	}
	return n, tr, sm, cleanup
}

func waitSentMessage(t *testing.T, tr *mockTransport, timeout time.Duration, match func(*raftpb.RaftMessage) bool) *raftpb.RaftMessage {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case msg := <-tr.sentCh:
			if match(msg) {
				return msg
			}
		case <-timer.C:
			t.Fatal("timed out waiting for Raft message")
			return nil
		}
	}
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

func TestNodeAcquireReadPermit(t *testing.T) {
	n, _, _, cleanup := newTestNode(t, 1, []uint64{1})
	defer cleanup()

	require.Eventually(t, n.IsLeader, 2*time.Second, 10*time.Millisecond)
	permit, err := n.AcquireReadPermit(t.Context())
	require.NoError(t, err)
	assert.Equal(t, n.Term(), permit.Term)
	assert.True(t, n.ValidateReadPermit(permit))
}

func TestNodeReadIndexWaitsUntilApplied(t *testing.T) {
	n, tr, _, cleanup := newTestNode(t, 2, []uint64{1, 2, 3})
	defer cleanup()

	// 日志 2 已接收，但 Leader 只公布提交到 1。
	tr.recvCh <- &raftpb.RaftMessage{
		From: 1, To: 2, Term: 1, Type: raftpb.MessageType_APPEND_REQ,
		Body: &raftpb.RaftMessage_AppendReq{AppendReq: &raftpb.AppendEntriesRequest{
			Term: 1, LeaderId: 1, Entries: []*raftpb.Entry{
				{Index: 1, Term: 1},
				{Index: 2, Term: 1, Data: []byte("value")},
			}, LeaderCommit: 1,
		}},
	}
	require.Eventually(t, func() bool { return n.LeaderID() == 1 }, time.Second, 10*time.Millisecond)

	resultCh := make(chan struct {
		index uint64
		err   error
	}, 1)
	go func() {
		index, err := n.ReadIndex(t.Context())
		resultCh <- struct {
			index uint64
			err   error
		}{index: index, err: err}
	}()

	var requestID uint64
	require.Eventually(t, func() bool {
		select {
		case msg := <-tr.sentCh:
			if msg.Type == raftpb.MessageType_READ_INDEX_REQ {
				requestID = msg.GetReadIndexReq().RequestId
				return true
			}
		default:
		}
		return false
	}, time.Second, 10*time.Millisecond)

	tr.recvCh <- &raftpb.RaftMessage{
		From: 1, To: 2, Term: 1, Type: raftpb.MessageType_READ_INDEX_RESP,
		Body: &raftpb.RaftMessage_ReadIndexResp{ReadIndexResp: &raftpb.ReadIndexResponse{
			RequestId: requestID, ReadIndex: 2, Success: true,
		}},
	}
	select {
	case <-resultCh:
		t.Fatal("ReadIndex returned before index 2 was applied")
	case <-time.After(30 * time.Millisecond):
	}

	// Leader 公布 commit=2；Node apply 完 index 2 后才释放等待者。
	tr.recvCh <- &raftpb.RaftMessage{
		From: 1, To: 2, Term: 1, Type: raftpb.MessageType_APPEND_REQ,
		Body: &raftpb.RaftMessage_AppendReq{AppendReq: &raftpb.AppendEntriesRequest{
			Term: 1, LeaderId: 1, PrevLogIndex: 2, PrevLogTerm: 1, LeaderCommit: 2,
		}},
	}
	select {
	case result := <-resultCh:
		require.NoError(t, result.err)
		assert.Equal(t, uint64(2), result.index)
	case <-time.After(time.Second):
		t.Fatal("ReadIndex did not return after index 2 was applied")
	}
}

func TestLeaderReadPermitIsNotStarvedByFollowerReadIndex(t *testing.T) {
	n, tr, _, cleanup := newTestNodeWithTick(t, 1, []uint64{1, 2, 3}, 100*time.Millisecond)
	defer cleanup()

	vote := waitSentMessage(t, tr, 3*time.Second, func(msg *raftpb.RaftMessage) bool {
		return msg.Type == raftpb.MessageType_REQUEST_VOTE_REQ && msg.To == 2
	})
	tr.recvCh <- &raftpb.RaftMessage{
		From: 2, To: 1, Term: vote.Term, Type: raftpb.MessageType_REQUEST_VOTE_RESP,
		Body: &raftpb.RaftMessage_VoteResp{VoteResp: &raftpb.RequestVoteResponse{
			Term: vote.Term, VoteGranted: true, VoterId: 2,
		}},
	}
	require.Eventually(t, n.IsLeader, time.Second, 10*time.Millisecond)

	// 先复制并提交新 Leader 的当前 term no-op。
	noOp := waitSentMessage(t, tr, time.Second, func(msg *raftpb.RaftMessage) bool {
		return msg.Type == raftpb.MessageType_APPEND_REQ && msg.To == 2 && len(msg.GetAppendReq().Entries) > 0
	})
	tr.recvCh <- appendSuccess(noOp, 2, 1)

	// round 1 由 Follower ReadIndex 占用。
	tr.recvCh <- readIndexRequestMessage(2, 1, vote.Term, 1)
	round1 := waitSentMessage(t, tr, time.Second, func(msg *raftpb.RaftMessage) bool {
		return msg.Type == raftpb.MessageType_APPEND_REQ && msg.To == 2 && msg.GetAppendReq().QuorumRound != 0
	})

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	permitCh := make(chan struct {
		permit raftstore.ReadPermit
		err    error
	}, 1)
	go func() {
		permit, err := n.AcquireReadPermit(ctx)
		permitCh <- struct {
			permit raftstore.ReadPermit
			err    error
		}{permit: permit, err: err}
	}()
	time.Sleep(20 * time.Millisecond) // 等 Node.run 将本地 Lease consumer 登记进下一批

	// 持续流量进入下一批；Leader Lease consumer 也已经登记在同一批。
	tr.recvCh <- readIndexRequestMessage(2, 1, vote.Term, 2)
	tr.recvCh <- appendSuccess(round1, 2, 1)
	round2 := waitSentMessage(t, tr, time.Second, func(msg *raftpb.RaftMessage) bool {
		return msg.Type == raftpb.MessageType_APPEND_REQ && msg.To == 2 &&
			msg.GetAppendReq().QuorumRound != 0 && msg.GetAppendReq().QuorumRound != round1.GetAppendReq().QuorumRound
	})
	tr.recvCh <- readIndexRequestMessage(2, 1, vote.Term, 3)
	tr.recvCh <- appendSuccess(round2, 2, 1)

	select {
	case result := <-permitCh:
		require.NoError(t, result.err)
		assert.True(t, n.ValidateReadPermit(result.permit))
	case <-time.After(time.Second):
		t.Fatal("Leader read permit was starved by Follower ReadIndex traffic")
	}
}

func appendSuccess(request *raftpb.RaftMessage, from, to uint64) *raftpb.RaftMessage {
	req := request.GetAppendReq()
	return &raftpb.RaftMessage{
		From: from, To: to, Term: request.Term, Type: raftpb.MessageType_APPEND_RESP,
		Body: &raftpb.RaftMessage_AppendResp{AppendResp: &raftpb.AppendEntriesResponse{
			Term: req.Term, Success: true, LastLogIndex: req.PrevLogIndex + uint64(len(req.Entries)),
			QuorumRound: req.QuorumRound,
		}},
	}
}

func readIndexRequestMessage(from, to, term, requestID uint64) *raftpb.RaftMessage {
	return &raftpb.RaftMessage{
		From: from, To: to, Term: term, Type: raftpb.MessageType_READ_INDEX_REQ,
		Body: &raftpb.RaftMessage_ReadIndexReq{ReadIndexReq: &raftpb.ReadIndexRequest{RequestId: requestID}},
	}
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

func (w *wrappedStorage) Close() error {
	return nil
}
