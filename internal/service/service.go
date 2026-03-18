package service

import (
	"github.com/DecarbonizedGlucose/rkv/internal/raft"
	"github.com/DecarbonizedGlucose/rkv/internal/types"
	"google.golang.org/grpc"
)

type ServiceNode struct {
	peers        []*raft.Peer
	raftInstance *raft.Raft
	raftHandler  *raft.RaftHandler
	raftApplier  *RaftApplier
	persister    *raft.Persister
	executor     *KVExecutor
}

func MakeServiceNode(cfg *types.ServerConfig, opts []grpc.DialOption) *ServiceNode {
	peers, me := raft.MakePeers(cfg, opts)
	persister := raft.MakePersister(&cfg.StoragePath, &cfg.RaftStatePersistedPath)
	applyChannel := make(chan *types.ApplyMsg)
	raftInstance := raft.MakeRaft(peers, me, persister, applyChannel)
	raftHandler := raft.MakeRaftHandler(raftInstance)
	executor := MakeKVExecutor(cfg)
	raftApplier := MakeRaftApplier(cfg.MaxRaftState, executor, raftInstance)
	return &ServiceNode{
		peers:        peers,
		raftInstance: raftInstance,
		raftHandler:  raftHandler,
		raftApplier:  raftApplier,
		persister:    persister,
		executor:     executor,
	}
}

func (rn *ServiceNode) Kill() {
	rn.raftApplier.Kill()
	rn.executor.SafeStop()
}
