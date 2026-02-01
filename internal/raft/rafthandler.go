package raft

import (
	"context"

	raftpb "github.com/DecarbonizedGlucose/rkv/api/raftrpc"
	"github.com/DecarbonizedGlucose/rkv/internal/utils"
)

// logical server handling Raft RPCs
type RaftHandler struct {
	rf *Raft
	raftpb.UnimplementedRaftConsensusServer
	raftpb.UnimplementedRaftPersistenceServer
}

func MakeRaftHandler(rf *Raft) *RaftHandler {
	return &RaftHandler{rf: rf}
}

func (s *RaftHandler) RequestVote(
	ctx context.Context,
	req *raftpb.RequestVoteRequest,
) (*raftpb.RequestVoteResponse, error) {
	if utils.IsCtxFailed(ctx) {
		return nil, ctx.Err()
	}
	res := &raftpb.RequestVoteResponse{}
	s.rf.RequestVote(req, res)
	return res, nil
}

func (s *RaftHandler) AppendEntries(
	ctx context.Context,
	req *raftpb.AppendEntriesRequest,
) (*raftpb.AppendEntriesResponse, error) {
	if utils.IsCtxFailed(ctx) {
		return nil, ctx.Err()
	}
	res := &raftpb.AppendEntriesResponse{}
	s.rf.AppendEntries(req, res)
	return res, nil
}

func (s *RaftHandler) InstallSnapshot(
	ctx context.Context,
	req *raftpb.InstallSnapshotRequest,
) (*raftpb.InstallSnapshotResponse, error) {
	if utils.IsCtxFailed(ctx) {
		return nil, ctx.Err()
	}
	res := &raftpb.InstallSnapshotResponse{}
	s.rf.InstallSnapshot(req, res)
	return res, nil
}
