package raft

import (
	"github.com/DecarbonizedGlucose/rkv/api/proto/pkg/raftpb"
)

func IsLocalMsg(m *raftpb.RaftMessage) bool {
	switch m.Type {
	case raftpb.MessageType_UNSPECIFIED, raftpb.MessageType_HANDSHAKE, raftpb.MessageType_HUP, raftpb.MessageType_HEARTBEAT, raftpb.MessageType_PROPOSE:
		return true
	default:
		return false
	}
}

func IsHSEqual(a, b *raftpb.HardState) bool {
	return a.Term == b.Term && a.Vote == b.Vote && a.CommitIndex == b.CommitIndex
}

func HardStateCopy(from *raftpb.HardState) *raftpb.HardState {
	if from == nil {
		return nil
	}
	return &raftpb.HardState{
		Term:        from.Term,
		Vote:        from.Vote,
		CommitIndex: from.CommitIndex,
	}
}

func IsSSEqual(a, b *SoftState) bool {
	return a.LeaderID == b.LeaderID && a.RaftState == b.RaftState
}

func SoftStateCopy(from *SoftState) *SoftState {
	if from == nil {
		return nil
	}
	return &SoftState{
		LeaderID:  from.LeaderID,
		RaftState: from.RaftState,
	}
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
