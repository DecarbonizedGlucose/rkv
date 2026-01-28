package raft

import (
	"context"
	"time"

	raftpb "github.com/DecarbonizedGlucose/rkv/api/raftrpc"
)

/* ==================== Logical Server ==================== */

type raftConsensusServer struct {
	raftpb.UnimplementedRaftConsensusServer
}

type raftPersistenceServer struct {
	raftpb.UnimplementedRaftPersistenceServer
}

/* ==================== Election and Voting ==================== */

func (rf *Raft) callRequestVote(
	client raftpb.RaftConsensusClient,
	req *raftpb.RequestVoteRequest,
	res *raftpb.RequestVoteResponse,
) bool {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	res, err := client.RequestVote(ctx, req)
	return err == nil
}

/* ==================== Append Entries ==================== */

func (rf *Raft) callAppendEntries(
	client raftpb.RaftConsensusClient,
	req *raftpb.AppendEntriesRequest,
	res *raftpb.AppendEntriesResponse,
) bool {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	res, err := client.AppendEntries(ctx, req)
	return err == nil
}

/* ==================== Persistence =================== */

func (rf *Raft) callInstallSnapshot(
	client raftpb.RaftPersistenceClient,
	req *raftpb.InstallSnapshotRequest,
	res *raftpb.InstallSnapshotResponse,
) bool {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	res, err := client.InstallSnapshot(ctx, req)
	return err == nil
}
