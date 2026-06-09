package raft

import (
	"errors"
	"sync"

	"github.com/DecarbonizedGlucose/rkv/api/proto/pkg/raftpb"
)

var (
	ErrCompacted                      = errors.New("requested index is unavailable due to compaction")
	ErrSnapOutOfDate                  = errors.New("requested index is older than the existing snapshot")
	ErrUnavailable                    = errors.New("requested entry at index is unavailable")
	ErrSnapshotTemporarilyUnavailable = errors.New("snapshot is temporarily unavailable")
	ErrOutOfBound                     = errors.New("requested index is out of bound")
)

type RaftStorage interface {
	InitialState() (*raftpb.HardState, *raftpb.ConfState, error)

	Entries(lo, hi uint64) ([]*raftpb.Entry, error)

	TermOfLog(idx uint64) (uint64, error)

	LastIndex() (uint64, error)

	FirstIndex() (uint64, error)

	Snapshot() (*raftpb.Snapshot, error)
}

type MemoryStorage struct {
	sync.Mutex
	entries   []*raftpb.Entry
	snapshot  *raftpb.Snapshot
	hardState *raftpb.HardState
}

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		entries:   []*raftpb.Entry{{}},
		hardState: &raftpb.HardState{},
		snapshot: &raftpb.Snapshot{
			Metadata: &raftpb.SnapshotMetadata{
				ConfState: &raftpb.ConfState{}, // Nodes = nil
			},
		},
	}
}

func (s *MemoryStorage) InitialState() (*raftpb.HardState, *raftpb.ConfState, error) {
	hs := s.hardState
	if hs == nil {
		hs = &raftpb.HardState{}
	}
	return hs, s.snapshot.Metadata.ConfState, nil
}

func (s *MemoryStorage) SetHardState(hs *raftpb.HardState) {
	s.Lock()
	defer s.Unlock()
	s.hardState = hs
}

// SetConfState 覆盖当前存储中的 ConfState。通常在首次启动时调用，
// 用于将配置中的集群成员信息写入空的 ConfState。
func (s *MemoryStorage) SetConfState(cs *raftpb.ConfState) {
	s.Lock()
	defer s.Unlock()
	s.snapshot.Metadata.ConfState = cs
}

func (s *MemoryStorage) Entries(lo, hi uint64) ([]*raftpb.Entry, error) {
	s.Lock()
	defer s.Unlock()

	offset := s.entries[0].Index
	if lo <= offset {
		return nil, ErrCompacted
	}
	if hi > s.lastIndex()+1 {
		return nil, ErrOutOfBound
	}

	entries := s.entries[lo-offset : hi-offset]
	if len(s.entries) == 1 && len(entries) == 0 {
		return nil, ErrUnavailable
	}
	return entries, nil
}

func (s *MemoryStorage) TermOfLog(idx uint64) (uint64, error) {
	s.Lock()
	defer s.Unlock()

	offset := s.entries[0].Index
	if idx < offset {
		return 0, ErrCompacted
	}
	if int(idx-offset) >= len(s.entries) {
		return 0, ErrUnavailable
	}
	return s.entries[idx-offset].Term, nil
}

func (s *MemoryStorage) lastIndex() uint64 {
	return s.entries[0].Index + uint64(len(s.entries)) - 1
}

func (s *MemoryStorage) LastIndex() (uint64, error) {
	s.Lock()
	defer s.Unlock()
	return s.lastIndex(), nil
}

func (s *MemoryStorage) firstIndex() uint64 {
	return s.entries[0].Index
}

func (s *MemoryStorage) FirstIndex() (uint64, error) {
	s.Lock()
	defer s.Unlock()
	return s.firstIndex(), nil
}

func (s *MemoryStorage) Snapshot() (*raftpb.Snapshot, error) {
	s.Lock()
	defer s.Unlock()
	return s.snapshot, nil
}

func (s *MemoryStorage) ApplySnapshot(snap *raftpb.Snapshot) error {
	s.Lock()
	defer s.Unlock()

	msIndex := s.snapshot.Metadata.LastIncludedIndex
	snapIndex := snap.Metadata.LastIncludedIndex
	if msIndex >= snapIndex {
		return ErrSnapOutOfDate
	}

	s.snapshot = snap
	s.entries = []*raftpb.Entry{{Index: snapIndex, Term: snap.Metadata.LastIncludedTerm}}
	return nil
}

func (s *MemoryStorage) CreateSnapshot(idx uint64, confState *raftpb.ConfState, data []byte) (*raftpb.Snapshot, error) {
	s.Lock()
	defer s.Unlock()

	if idx <= s.snapshot.Metadata.LastIncludedIndex {
		return nil, ErrSnapOutOfDate
	}

	offset := s.entries[0].Index
	if idx > s.lastIndex() {
		return nil, ErrOutOfBound
	}

	s.snapshot.Metadata.LastIncludedIndex = idx
	s.snapshot.Metadata.LastIncludedTerm = s.entries[idx-offset].Term
	if confState != nil {
		s.snapshot.Metadata.ConfState = confState
	}
	s.snapshot.Data = data
	return s.snapshot, nil
}

func (s *MemoryStorage) Compact(compactIdx uint64) error {
	s.Lock()
	defer s.Unlock()

	offset := s.entries[0].Index
	if compactIdx <= offset {
		return ErrCompacted
	}
	if compactIdx > s.lastIndex() {
		return ErrOutOfBound
	}

	i := compactIdx - offset
	entries := make([]*raftpb.Entry, 1, 1+len(s.entries)-int(i))
	entries[0] = &raftpb.Entry{
		Index: s.entries[i].Index,
		Term:  s.entries[i].Term,
	}
	entries = append(entries, s.entries[i+1:]...)
	s.entries = entries
	return nil
}

func (s *MemoryStorage) Append(entries []*raftpb.Entry) error {
	if len(entries) == 0 {
		return nil
	}

	s.Lock()
	defer s.Unlock()

	first := s.firstIndex()
	last := s.lastIndex()

	if last > first {
		return nil
	}

	if first > entries[0].Index {
		entries = entries[first-entries[0].Index:]
	}

	offset := entries[0].Index - s.entries[0].Index
	switch {
	case uint64(len(s.entries)) > offset:
		s.entries = append([]*raftpb.Entry{}, s.entries[:offset]...)
		s.entries = append(s.entries, entries...)
	case uint64(len(s.entries)) == offset:
		s.entries = append(s.entries, entries...)
	default:
		return ErrOutOfBound
	}
	return nil
}
