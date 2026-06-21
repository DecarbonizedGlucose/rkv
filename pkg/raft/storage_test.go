package raft

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/DecarbonizedGlucose/rkv/api/proto/pkg/raftpb"
)

// ========================================
// Initial State
// ========================================

func TestMemoryStorageInitialState(t *testing.T) {
	s := NewMemoryStorage()

	hs, cs, err := s.InitialState()
	require.NoError(t, err)
	assert.NotNil(t, hs)
	assert.Equal(t, uint64(0), hs.Term)
	assert.Equal(t, uint64(0), hs.Vote)
	assert.NotNil(t, cs)
	assert.Nil(t, cs.Nodes)
}

// ========================================
// HardState / ConfState
// ========================================

func TestMemoryStorageSetHardState(t *testing.T) {
	s := NewMemoryStorage()
	s.SetHardState(&raftpb.HardState{Term: 5, Vote: 2})

	hs, _, err := s.InitialState()
	require.NoError(t, err)
	assert.Equal(t, uint64(5), hs.Term)
	assert.Equal(t, uint64(2), hs.Vote)
}

func TestMemoryStorageSetConfState(t *testing.T) {
	s := NewMemoryStorage()
	s.SetConfState(&raftpb.ConfState{Nodes: []uint64{1, 2, 3}})

	_, cs, err := s.InitialState()
	require.NoError(t, err)
	assert.Equal(t, []uint64{1, 2, 3}, cs.Nodes)
}

// ========================================
// Append
// ========================================

func TestMemoryStorageAppend(t *testing.T) {
	s := NewMemoryStorage()

	err := s.Append([]*raftpb.Entry{makeEntry(1, 1)})
	require.NoError(t, err)

	last, err := s.LastIndex()
	require.NoError(t, err)
	assert.Equal(t, uint64(1), last)
}

func TestMemoryStorageAppendMultiBatch(t *testing.T) {
	s := NewMemoryStorage()

	require.NoError(t, s.Append([]*raftpb.Entry{makeEntry(1, 1), makeEntry(2, 1)}))
	require.NoError(t, s.Append([]*raftpb.Entry{makeEntry(3, 2)}))

	last, err := s.LastIndex()
	require.NoError(t, err)
	assert.Equal(t, uint64(3), last)
}

func TestMemoryStorageAppendEmpty(t *testing.T) {
	s := NewMemoryStorage()
	require.NoError(t, s.Append(nil))
	require.NoError(t, s.Append([]*raftpb.Entry{}))

	last, err := s.LastIndex()
	require.NoError(t, err)
	assert.Equal(t, uint64(0), last)
}

func TestMemoryStorageAppendOverlap(t *testing.T) {
	s := NewMemoryStorage()

	require.NoError(t, s.Append([]*raftpb.Entry{makeEntry(1, 1), makeEntry(2, 1)}))
	// 第二批起始索引重叠，应截断旧条目后追加
	require.NoError(t, s.Append([]*raftpb.Entry{makeEntry(2, 3), makeEntry(3, 3)}))

	// entry 1 的 term 还是 1，entry 2 的 term 已被覆盖为 3
	term, err := s.TermOfLog(2)
	require.NoError(t, err)
	assert.Equal(t, uint64(3), term)
}

// ========================================
// Entries
// ========================================

func TestMemoryStorageEntries(t *testing.T) {
	s := NewMemoryStorage()
	require.NoError(t, s.Append([]*raftpb.Entry{makeEntry(1, 1), makeEntry(2, 2), makeEntry(3, 3)}))

	ents, err := s.Entries(1, 4)
	require.NoError(t, err)
	assert.Len(t, ents, 3)
}

func TestMemoryStorageEntriesCompacted(t *testing.T) {
	s := NewMemoryStorage()

	_, err := s.Entries(0, 1)
	assert.ErrorIs(t, err, ErrCompacted)
}

func TestMemoryStorageEntriesOutOfBound(t *testing.T) {
	s := NewMemoryStorage()

	_, err := s.Entries(1, 100)
	assert.ErrorIs(t, err, ErrOutOfBound)
}

func TestMemoryStorageEntriesUnavailable(t *testing.T) {
	s := NewMemoryStorage()
	// 只有哨兵索引 0，请求 [1,1) 返回 ErrUnavailable
	_, err := s.Entries(1, 1)
	assert.ErrorIs(t, err, ErrUnavailable)
}

func TestMemoryStorageEntriesEmptyRange(t *testing.T) {
	s := NewMemoryStorage()
	require.NoError(t, s.Append([]*raftpb.Entry{makeEntry(1, 1)}))

	ents, err := s.Entries(1, 1)
	require.NoError(t, err)
	assert.Empty(t, ents)
}

// ========================================
// TermOfLog
// ========================================

func TestMemoryStorageTermOfLog(t *testing.T) {
	s := NewMemoryStorage()
	require.NoError(t, s.Append([]*raftpb.Entry{makeEntry(1, 5)}))

	term, err := s.TermOfLog(1)
	require.NoError(t, err)
	assert.Equal(t, uint64(5), term)
}

func TestMemoryStorageTermOfLogCompacted(t *testing.T) {
	s := NewMemoryStorage()
	require.NoError(t, s.Append([]*raftpb.Entry{makeEntry(1, 5)}))
	require.NoError(t, s.Compact(1))

	// compact 后哨兵移到 1，idx=0 已被压缩
	_, err := s.TermOfLog(0)
	assert.ErrorIs(t, err, ErrCompacted)
}

func TestMemoryStorageTermOfLogUnavailable(t *testing.T) {
	s := NewMemoryStorage()

	_, err := s.TermOfLog(1)
	assert.ErrorIs(t, err, ErrUnavailable)
}

// ========================================
// FirstIndex / LastIndex
// ========================================

func TestMemoryStorageFirstLastIndex(t *testing.T) {
	s := NewMemoryStorage()
	require.NoError(t, s.Append([]*raftpb.Entry{makeEntry(1, 1), makeEntry(2, 1), makeEntry(3, 1)}))

	first, err := s.FirstIndex()
	require.NoError(t, err)
	assert.Equal(t, uint64(1), first)

	last, err := s.LastIndex()
	require.NoError(t, err)
	assert.Equal(t, uint64(3), last)
}

func TestMemoryStorageFirstLastIndexEmpty(t *testing.T) {
	s := NewMemoryStorage()

	first, _ := s.FirstIndex()
	last, _ := s.LastIndex()
	assert.Equal(t, uint64(1), first)
	assert.Equal(t, uint64(0), last)
}

// ========================================
// CreateSnapshot
// ========================================

func TestMemoryStorageCreateSnapshot(t *testing.T) {
	s := NewMemoryStorage()
	require.NoError(t, s.Append([]*raftpb.Entry{makeEntry(1, 1), makeEntry(2, 2), makeEntry(3, 3)}))

	data := []byte("snapshot_data")
	cs := &raftpb.ConfState{Nodes: []uint64{1, 2}}
	snap, err := s.CreateSnapshot(2, cs, data)
	require.NoError(t, err)
	require.NotNil(t, snap)

	assert.Equal(t, uint64(2), snap.Metadata.LastIncludedIndex)
	assert.Equal(t, uint64(2), snap.Metadata.LastIncludedTerm)
	assert.Equal(t, data, snap.Data)
	assert.Equal(t, cs, snap.Metadata.ConfState)

	// Snapshot() 返回同一份
	stored, err := s.Snapshot()
	require.NoError(t, err)
	assert.Equal(t, snap, stored)
}

func TestMemoryStorageCreateSnapshotOutOfDate(t *testing.T) {
	s := NewMemoryStorage()
	require.NoError(t, s.Append([]*raftpb.Entry{makeEntry(1, 1)}))

	_, err := s.CreateSnapshot(1, nil, nil)
	require.NoError(t, err)

	// 再建一个更早的 idx
	_, err = s.CreateSnapshot(1, nil, nil)
	assert.ErrorIs(t, err, ErrSnapOutOfDate)
}

func TestMemoryStorageCreateSnapshotOutOfBound(t *testing.T) {
	s := NewMemoryStorage()
	require.NoError(t, s.Append([]*raftpb.Entry{makeEntry(1, 1)}))

	// idx 3 > lastIndex(1)
	_, err := s.CreateSnapshot(3, nil, nil)
	assert.ErrorIs(t, err, ErrOutOfBound)
}

// ========================================
// ApplySnapshot
// ========================================

func TestMemoryStorageApplySnapshot(t *testing.T) {
	s := NewMemoryStorage()
	require.NoError(t, s.Append([]*raftpb.Entry{makeEntry(1, 1), makeEntry(2, 2)}))

	snap := &raftpb.Snapshot{
		Metadata: &raftpb.SnapshotMetadata{
			LastIncludedIndex: 5,
			LastIncludedTerm:  3,
			ConfState:         &raftpb.ConfState{Nodes: []uint64{1, 2, 3}},
		},
		Data: []byte("new_data"),
	}
	require.NoError(t, s.ApplySnapshot(snap))

	// entries 被重置，firstIndex = lastIncludedIndex + 1
	first, _ := s.FirstIndex()
	assert.Equal(t, uint64(6), first)

	// 旧条目被压缩
	_, err := s.Entries(1, 3)
	assert.ErrorIs(t, err, ErrCompacted)

	// 哨兵条目 term 正确
	term, err := s.TermOfLog(5)
	require.NoError(t, err)
	assert.Equal(t, uint64(3), term)

	// ConfState 更新
	_, cs, _ := s.InitialState()
	assert.Equal(t, []uint64{1, 2, 3}, cs.Nodes)
}

func TestMemoryStorageApplySnapshotOutOfDate(t *testing.T) {
	s := NewMemoryStorage()
	require.NoError(t, s.Append([]*raftpb.Entry{makeEntry(1, 1)}))

	_, err := s.CreateSnapshot(1, nil, nil)
	require.NoError(t, err)

	// 旧快照（idx <= 已有快照）
	oldSnap := &raftpb.Snapshot{
		Metadata: &raftpb.SnapshotMetadata{LastIncludedIndex: 1},
	}
	assert.ErrorIs(t, s.ApplySnapshot(oldSnap), ErrSnapOutOfDate)
}

// ========================================
// Compact
// ========================================

func TestMemoryStorageCompact(t *testing.T) {
	s := NewMemoryStorage()
	require.NoError(t, s.Append([]*raftpb.Entry{makeEntry(1, 1), makeEntry(2, 2), makeEntry(3, 3)}))

	require.NoError(t, s.Compact(2))

	// FirstIndex 变为 compacted sentinel + 1
	first, _ := s.FirstIndex()
	assert.Equal(t, uint64(3), first)

	// 旧条目不可读
	_, err := s.Entries(1, 2)
	assert.ErrorIs(t, err, ErrCompacted)

	// 新哨兵 term 正确
	term, err := s.TermOfLog(2)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), term)
}

func TestMemoryStorageCompactLowerThanOffset(t *testing.T) {
	s := NewMemoryStorage()
	assert.ErrorIs(t, s.Compact(0), ErrCompacted)
}

func TestMemoryStorageCompactOutOfBound(t *testing.T) {
	s := NewMemoryStorage()
	assert.ErrorIs(t, s.Compact(100), ErrOutOfBound)
}

// ========================================
// Snapshot (empty)
// ========================================

func TestMemoryStorageSnapshotEmpty(t *testing.T) {
	s := NewMemoryStorage()

	snap, err := s.Snapshot()
	require.NoError(t, err)
	assert.Equal(t, uint64(0), snap.Metadata.LastIncludedIndex)
}
