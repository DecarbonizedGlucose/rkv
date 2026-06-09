package raft

import (
	"github.com/DecarbonizedGlucose/rkv/api/proto/pkg/raftpb"
)

func IsLocalMsg(m *raftpb.RaftMessage) bool {
	switch m.Type {
	case raftpb.MessageType_HUP, raftpb.MessageType_HEARTBEAT, raftpb.MessageType_PROPOSE:
		return true
	default:
		return false
	}
}

func IsHSEqual(a, b *raftpb.HardState) bool {
	return a.Term == b.Term && a.Vote == b.Vote && a.CommitIndex == b.CommitIndex
}

func IsEmptySnapshot(s *raftpb.Snapshot) bool {
	return s == nil ||
		len(s.Data) <= 0 ||
		s.Metadata == nil ||
		s.Metadata.LastIncludedIndex == 0 ||
		s.Metadata.LastIncludedTerm == 0 ||
		s.Metadata.ConfState == nil ||
		len(s.Metadata.ConfState.Nodes) <= 0
}
