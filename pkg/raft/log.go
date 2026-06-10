package raft

import (
	"github.com/DecarbonizedGlucose/rkv/api/proto/pkg/raftpb"
)

type RaftLog struct {
	storage RaftStorage

	// 日志条目, entries[0]是哨兵条目,
	// entries[i]真实索引为 lastIncludedIndex + i
	entries []*raftpb.Entry

	// 快照中的最后一个日志索引
	//lastIncludedIndex uint64

	// 已经落入WAL的日志条目的数量
	stabledIndex uint64

	// 已经应用到状态机的日志条目的数量
	appliedIndex uint64

	// 已经提交的日志条目的数量
	//committedIndex uint64

	// 待安装的快照
	pendingSnapshot *raftpb.Snapshot
}

func newRaftLog(storage RaftStorage) *RaftLog {
	if storage == nil {
		panic("raft: storage must not be nil")
	}
	// hs, _, err := storage.InitialState()
	// if err != nil {
	// 	panic("raft: read initial state: " + err.Error())
	// }
	firstIndex, err := storage.FirstIndex() // firstIndex 一定 >= 1
	if err != nil {
		panic("raft: read first index: " + err.Error())
	}
	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic("raft: read last index: " + err.Error())
	}
	// 哨兵条目代表快照覆盖的最后一条日志
	dummy := &raftpb.Entry{Index: firstIndex - 1}
	if firstIndex == 0 {
		dummy.Index = 0
		firstIndex = 1
	}

	entries := []*raftpb.Entry{dummy}
	if firstIndex <= lastIndex {
		stored, err := storage.Entries(firstIndex, lastIndex+1)
		if err != nil {
			panic("raft: load entries: " + err.Error())
		}
		entries = append(entries, stored...)
	}

	return &RaftLog{
		storage:      storage,
		stabledIndex: lastIndex,
		appliedIndex: dummy.Index,
		//committedIndex: hs.CommitIndex,
		entries: entries,
		//lastIncludedIndex: dummy.Index,
	}
}

func (l *RaftLog) lastLogIndex() uint64 {
	return l.getLastIncluded() + uint64(len(l.entries)) - 1
}

func (l *RaftLog) lastLogTerm() uint64 {
	if len(l.entries) == 0 {
		return 0
	}
	return l.entries[len(l.entries)-1].Term
}

func (l *RaftLog) setLastIncluded(idx uint64) {
	//l.lastIncludedIndex = idx
	l.entries[0].Index = idx
}

func (l *RaftLog) getLastIncluded() uint64 {
	return l.entries[0].Index
}

func (l *RaftLog) term(idx uint64) uint64 {
	if idx < l.getLastIncluded() || idx > l.lastLogIndex() {
		return 0
	}
	return l.entries[idx-l.getLastIncluded()].Term
}

func (l *RaftLog) slice(start, end uint64) []*raftpb.Entry {
	// [start, end)

	start = max(start, l.getLastIncluded())
	end = min(end, l.lastLogIndex()+1)
	if start >= end {
		return nil
	}
	return l.entries[start-l.getLastIncluded() : end-l.getLastIncluded()]
}

func (l *RaftLog) append(entries ...*raftpb.Entry) {
	l.entries = append(l.entries, entries...)
}

func (l *RaftLog) trunc(from uint64) {
	if from <= l.getLastIncluded() {
		return
	}
	l.entries = l.entries[:from-l.getLastIncluded()]
}

func (l *RaftLog) matchTerm(idx, term uint64) bool {
	if idx < l.getLastIncluded() || idx > l.lastLogIndex() {
		return false
	}
	return l.term(idx) == term
}

// func (l *RaftLog) entry(idx uint64) *raftpb.Entry {
// 	if idx < l.getLastIncluded() || idx > l.lastLogIndex() {
// 		return nil
// 	}
// 	return l.entries[idx-l.getLastIncluded()]
// }

func (l *RaftLog) lastIndexOfTerm(term uint64) (uint64, bool) {
	for i := l.lastLogIndex(); i >= l.getLastIncluded(); i-- {
		if l.term(i) == term {
			return i, true
		}
		if i == l.getLastIncluded() {
			break
		}
	}
	return 0, false
}

// func (l *RaftLog) ResetFromSnapshot(snapIndex, snapTerm uint64) {
// 	if snapIndex < l.getLastIncluded() {
// 		return
// 	}
// 	l.setLastIncluded(snapIndex)
// 	l.entries = []*raftpb.Entry{
// 		{
// 			Index: snapIndex,
// 			Term:  snapTerm,
// 		},
// 	}
// 	l.appliedIndex = snapIndex
// 	//l.committedIndex = snapIndex
// }

// 返回未落盘WAL的日志条目
func (l *RaftLog) unstableEntries() []*raftpb.Entry {
	if l.stabledIndex >= l.lastLogIndex() {
		return nil
	}
	return l.entries[l.stabledIndex+1-l.getLastIncluded():]
}

// 返回已经提交但未应用的日志条目
func (l *RaftLog) nextEntries(committedIndex uint64) []*raftpb.Entry {
	if l.appliedIndex >= committedIndex {
		// 崩溃恢复可能发生
		return nil
	}
	return l.entries[l.appliedIndex+1-l.getLastIncluded() : committedIndex+1-l.getLastIncluded()]
}

func (l *RaftLog) maybeCompact() {
	first, err := l.storage.FirstIndex()
	if err != nil {
		return
	}
	if first <= l.getLastIncluded() {
		return
	}
	compactIndex := first - 1
	term, err := l.storage.TermOfLog(compactIndex)
	if err != nil {
		return
	}
	if compactIndex > l.lastLogIndex() {
		l.entries = []*raftpb.Entry{{Index: compactIndex, Term: term}} // 新 dummy!
		return
	}
	l.entries = append([]*raftpb.Entry{{Index: compactIndex, Term: term}}, l.entries[compactIndex-l.getLastIncluded()+1:]...)
}
