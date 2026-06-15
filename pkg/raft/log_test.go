package raft

import (
	"testing"

	"github.com/DecarbonizedGlucose/rkv/api/proto/pkg/raftpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeEntry(index, term uint64) *raftpb.Entry {
	return &raftpb.Entry{Index: index, Term: term}
}

func newTestRaftLog(entries ...*raftpb.Entry) *RaftLog {
	ents := []*raftpb.Entry{{Index: 0}}
	ents = append(ents, entries...)
	return &RaftLog{
		entries: ents,
	}
}

func TestRaftLogAppend(t *testing.T) {
	l := newTestRaftLog()
	l.append(makeEntry(1, 1), makeEntry(2, 1))

	assert.Equal(t, uint64(2), l.lastLogIndex())
	assert.Equal(t, uint64(1), l.lastLogTerm())
}

func TestRaftLogAppendMultipleBatches(t *testing.T) {
	l := newTestRaftLog()
	l.append(makeEntry(1, 1))
	l.append(makeEntry(2, 2), makeEntry(3, 2))

	assert.Equal(t, uint64(3), l.lastLogIndex())
	assert.Equal(t, uint64(2), l.lastLogTerm())
}

func TestRaftLogTrunc(t *testing.T) {
	l := newTestRaftLog(
		makeEntry(1, 1),
		makeEntry(2, 1),
		makeEntry(3, 2),
		makeEntry(4, 2),
	)

	l.trunc(3)
	assert.Equal(t, uint64(2), l.lastLogIndex())
}

func TestRaftLogTruncBeforeIncluded(t *testing.T) {
	l := newTestRaftLog(makeEntry(1, 1))
	l.trunc(0)
	assert.Equal(t, uint64(1), l.lastLogIndex())
}

func TestRaftLogTruncExactLast(t *testing.T) {
	l := newTestRaftLog(makeEntry(1, 1), makeEntry(2, 1))
	l.trunc(3)
	assert.Equal(t, uint64(2), l.lastLogIndex())
}

func TestRaftLogTerm(t *testing.T) {
	l := newTestRaftLog(makeEntry(1, 1), makeEntry(2, 2))

	assert.Equal(t, uint64(1), l.term(1))
	assert.Equal(t, uint64(2), l.term(2))
}

func TestRaftLogTermOutOfBounds(t *testing.T) {
	l := newTestRaftLog(makeEntry(1, 1))

	assert.Equal(t, uint64(0), l.term(0))
	assert.Equal(t, uint64(0), l.term(100))
}

func TestRaftLogTermEmpty(t *testing.T) {
	l := newTestRaftLog()
	assert.Equal(t, uint64(0), l.lastLogTerm())
}

func TestRaftLogSlice(t *testing.T) {
	l := newTestRaftLog(makeEntry(1, 1), makeEntry(2, 1), makeEntry(3, 2))

	ents := l.slice(2, 4)
	require.Len(t, ents, 2)
	assert.Equal(t, uint64(2), ents[0].Index)
	assert.Equal(t, uint64(3), ents[1].Index)
}

func TestRaftLogSliceClamp(t *testing.T) {
	l := newTestRaftLog(makeEntry(1, 1), makeEntry(2, 1))

	ents := l.slice(1, 100)
	assert.Len(t, ents, 2)
}

func TestRaftLogSliceEmpty(t *testing.T) {
	l := newTestRaftLog(makeEntry(1, 1))
	assert.Nil(t, l.slice(2, 2))
}

func TestRaftLogSliceAfterIncluded(t *testing.T) {
	l := newTestRaftLog(makeEntry(1, 1), makeEntry(2, 2), makeEntry(3, 3))

	// 模拟 handleInstallSnapshot 的手动裁剪流程：
	// 构造新哨兵（带 term），保留 lastIncluded 之后的条目
	lastIncluded := uint64(2)
	newEntries := []*raftpb.Entry{{Index: 0, Term: l.term(lastIncluded)}}
	relPos := lastIncluded - l.getLastIncluded()
	newEntries = append(newEntries, l.entries[relPos+1:]...)
	l.entries = newEntries
	l.setLastIncluded(lastIncluded)

	ents := l.slice(3, 4)
	require.Len(t, ents, 1)
	assert.Equal(t, uint64(3), ents[0].Index)
}

func TestRaftLogMatchTerm(t *testing.T) {
	l := newTestRaftLog(makeEntry(1, 1))

	assert.True(t, l.matchTerm(1, 1))
	assert.False(t, l.matchTerm(1, 2))
}

func TestRaftLogMatchTermOutOfBounds(t *testing.T) {
	l := newTestRaftLog(makeEntry(1, 1))

	// 哨兵 Index=0, Term=0, idx=0 通不过 idx<lastIncluded 的检查（用了<而非<=）
	// 实际会返回 true（哨兵的 Term=0 与参数 0 匹配）
	assert.True(t, l.matchTerm(0, 0))
	assert.False(t, l.matchTerm(100, 0))
}

func TestRaftLogLastIndexOfTerm(t *testing.T) {
	l := newTestRaftLog(
		makeEntry(1, 1),
		makeEntry(2, 1),
		makeEntry(3, 2),
	)

	idx, ok := l.lastIndexOfTerm(1)
	assert.True(t, ok)
	assert.Equal(t, uint64(2), idx)
}

func TestRaftLogLastIndexOfTermNotFound(t *testing.T) {
	l := newTestRaftLog(makeEntry(1, 1))
	_, ok := l.lastIndexOfTerm(99)
	assert.False(t, ok)
}

func TestRaftLogUnstableEntries(t *testing.T) {
	l := newTestRaftLog(makeEntry(1, 1), makeEntry(2, 1), makeEntry(3, 1))
	l.stabledIndex = 1

	unstable := l.unstableEntries()
	require.Len(t, unstable, 2)
	assert.Equal(t, uint64(2), unstable[0].Index)
}

func TestRaftLogUnstableAllStabled(t *testing.T) {
	l := newTestRaftLog(makeEntry(1, 1), makeEntry(2, 1))
	l.stabledIndex = 2

	assert.Nil(t, l.unstableEntries())
}

func TestRaftLogUnstableEmptyLog(t *testing.T) {
	l := newTestRaftLog()
	assert.Nil(t, l.unstableEntries())
}

func TestRaftLogNextEntries(t *testing.T) {
	l := newTestRaftLog(makeEntry(1, 1), makeEntry(2, 1), makeEntry(3, 1))
	l.appliedIndex = 1

	next := l.nextEntries(3)
	require.Len(t, next, 2)
	assert.Equal(t, uint64(2), next[0].Index)
}

func TestRaftLogNextEntriesAppliedUpToDate(t *testing.T) {
	l := newTestRaftLog(makeEntry(1, 1))
	l.appliedIndex = 1

	assert.Nil(t, l.nextEntries(1))
}

func TestRaftLogNextEntriesAppliedAhead(t *testing.T) {
	l := newTestRaftLog(makeEntry(1, 1))
	l.appliedIndex = 5

	assert.Nil(t, l.nextEntries(1))
}

func TestRaftLogSetLastIncluded(t *testing.T) {
	l := newTestRaftLog(
		makeEntry(1, 1),
		makeEntry(2, 2),
		makeEntry(3, 3),
	)

	// 模拟 handleInstallSnapshot 的手动裁剪 + setLastIncluded(2)
	lastIncluded := uint64(2)
	newEntries := []*raftpb.Entry{{Index: 0, Term: l.term(lastIncluded)}}
	relPos := lastIncluded - l.getLastIncluded()
	newEntries = append(newEntries, l.entries[relPos+1:]...)
	l.entries = newEntries
	l.setLastIncluded(lastIncluded)

	assert.Equal(t, uint64(2), l.getLastIncluded())
	assert.Equal(t, uint64(3), l.lastLogIndex())

	// idx=1 已低于 included
	assert.Equal(t, uint64(0), l.term(1))
	// 哨兵条目正确反映 lastIncluded 处的 term
	assert.Equal(t, uint64(2), l.term(2))
	assert.Equal(t, uint64(3), l.term(3))
}

func TestRaftLogNewFromStorage(t *testing.T) {
	s := NewMemoryStorage()
	require.NoError(t, s.Append([]*raftpb.Entry{
		makeEntry(1, 1),
		makeEntry(2, 1),
		makeEntry(3, 1),
	}))

	l, err := newRaftLog(s)
	require.NoError(t, err)
	assert.Equal(t, uint64(3), l.lastLogIndex())
	assert.Equal(t, uint64(0), l.getLastIncluded())
}
