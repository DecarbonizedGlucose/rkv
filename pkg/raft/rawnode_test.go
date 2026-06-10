package raft

import (
	"testing"

	"github.com/DecarbonizedGlucose/rkv/api/proto/pkg/raftpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ========================================
// Test helpers (RawNode)
// ========================================

func newTestRawNode(id uint64, peers []uint64) *RawNode {
	rn, err := NewRawNode(newTestConfig(id, peers))
	if err != nil {
		panic(err)
	}
	return rn
}

// ========================================
// Campaign
// ========================================

func TestRawNodeCampaign(t *testing.T) {
	rn := newTestRawNode(1, []uint64{1})

	rn.Campaign()
	assert.True(t, rn.HasReady())

	rd := rn.Ready()
	require.NotNil(t, rd.SoftState)
	assert.True(t, rd.RaftState == stateLeader, "expected Leader, got %s", rd.RaftState.str())

	rn.Advance(&rd)
	assert.False(t, rn.HasReady())
}

func TestRawNodeCampaignMultiNode(t *testing.T) {
	rn := newTestRawNode(1, []uint64{1, 2, 3})

	rn.Campaign()
	rd := rn.Ready()
	require.NotNil(t, rd.SoftState)
	assert.True(t, rd.RaftState == stateCandidate, "expected Candidate, got %s", rd.RaftState.str())
	assert.NotEmpty(t, rd.Messages)

	rn.Advance(&rd)
	assert.False(t, rn.HasReady())
}

// ========================================
// Propose
// ========================================

func TestRawNodePropose(t *testing.T) {
	rn := newTestRawNode(1, []uint64{1})

	// 先当选 leader
	rn.Campaign()
	rd := rn.Ready()
	rn.Advance(&rd)

	// Propose
	require.NoError(t, rn.Propose([]byte("test_data")))
	assert.True(t, rn.HasReady())

	rd = rn.Ready()
	assert.NotNil(t, rd.Entries)
	require.Len(t, rd.Entries, 1)
	assert.Equal(t, []byte("test_data"), rd.Entries[0].Data)

	rn.Advance(&rd)
}

func TestRawNodeProposeNotLeader(t *testing.T) {
	rn := newTestRawNode(1, []uint64{1, 2, 3})

	// 未经过选举
	err := rn.Propose([]byte("test"))
	assert.ErrorIs(t, err, ErrNotLeader)
}

// ========================================
// Single-node commit flow
// ========================================

func TestRawNodeSingleNodeCommit(t *testing.T) {
	rn := newTestRawNode(1, []uint64{1})

	rn.Campaign()
	rd := rn.Ready()
	assert.True(t, rd.RaftState == stateLeader)
	rn.Advance(&rd)

	rn.Propose([]byte("foo"))
	assert.True(t, rn.HasReady())
	rd = rn.Ready()

	require.NotEmpty(t, rd.CommittedEntries, "single-node commits immediately on propose")
	assert.Equal(t, []byte("foo"), rd.CommittedEntries[0].Data)

	rn.Advance(&rd)
	assert.False(t, rn.HasReady())
}

// ========================================
// HasReady
// ========================================

func TestRawNodeHasReadyAfterCampaign(t *testing.T) {
	rn := newTestRawNode(1, []uint64{1})

	assert.False(t, rn.HasReady())
	rn.Campaign()
	assert.True(t, rn.HasReady())

	rd := rn.Ready()
	rn.Advance(&rd)
	assert.False(t, rn.HasReady())
}

// ========================================
// Step
// ========================================

func TestRawNodeStep(t *testing.T) {
	rn := newTestRawNode(1, []uint64{1, 2})

	// 收到高 term 的 AppendEntries 应更新 term
	msg := &raftpb.RaftMessage{
		From: 2,
		To:   1,
		Type: raftpb.MessageType_APPEND_REQ,
		Term: 5,
		Body: &raftpb.RaftMessage_AppendReq{AppendReq: &raftpb.AppendEntriesRequest{
			Term:     5,
			LeaderId: 2,
		}},
	}

	require.NoError(t, rn.Step(msg))
	assert.Equal(t, uint64(5), rn.Raft.hardState.Term)
}

func TestRawNodeStepLocalMsg(t *testing.T) {
	rn := newTestRawNode(1, []uint64{1})

	// 本地消息应被 Step 拒绝
	err := rn.Step(&raftpb.RaftMessage{Type: raftpb.MessageType_HUP})
	assert.ErrorIs(t, err, ErrStepLocalMsg)
}

// ========================================
// Snapshot in Ready
// ========================================

func TestRawNodeSnapshotInReady(t *testing.T) {
	rn := newTestRawNode(1, []uint64{1, 2})

	// 先当选 leader
	rn.Campaign()
	rd := rn.Ready()
	rn.Advance(&rd)

	// 在 storage 中创建快照并设置 pendingSnapshot
	snap := &raftpb.Snapshot{
		Data: []byte("snapdata"),
		Metadata: &raftpb.SnapshotMetadata{
			LastIncludedIndex: 10,
			LastIncludedTerm:  5,
			ConfState:         &raftpb.ConfState{Nodes: []uint64{1, 2}},
		},
	}
	rn.Raft.raftLog.pendingSnapshot = snap
	assert.True(t, rn.HasReady())

	rd = rn.Ready()
	require.NotNil(t, rd.Snapshot)
	assert.Equal(t, snap, rd.Snapshot)

	rn.Advance(&rd)
	assert.True(t, IsEmptySnapshot(rn.Raft.raftLog.pendingSnapshot))
}

// ========================================
// GetProgress
// ========================================

func TestRawNodeGetProgress(t *testing.T) {
	rn := newTestRawNode(1, []uint64{1})

	// 非 Leader 时返回空
	prs := rn.GetProgress()
	assert.Empty(t, prs)

	// 单节点立即当选
	rn.Campaign()
	rd := rn.Ready()
	rn.Advance(&rd)

	prs = rn.GetProgress()
	assert.Len(t, prs, 1)
	assert.Contains(t, prs, uint64(1))
}
