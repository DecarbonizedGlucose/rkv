package service

import (
	"sync"
	"sync/atomic"

	"github.com/DecarbonizedGlucose/rkv/internal/engine"
	"github.com/DecarbonizedGlucose/rkv/internal/raft"
	"github.com/DecarbonizedGlucose/rkv/internal/types"
	"google.golang.org/grpc"
)

type RSM struct {
	mu           sync.Mutex
	rf           types.Raft
	applyCh      chan types.ApplyMsg
	maxraftstate int
	// may waitingOps, uses (Client ID, Request ID) for deduplication
	sm       engine.Storage
	shutdown atomic.Bool
}

func MakeRSM(
	servers []*grpc.ClientConn,
	me int,
	persister *raft.Persister,
	maxraftstate int,
	sm engine.Storage,
) *RSM {
	rsm := &RSM{
		mu:           sync.Mutex{},
		maxraftstate: maxraftstate,
		applyCh:      make(chan types.ApplyMsg),
		sm:           sm,
	}
	rsm.shutdown.Store(false)
	rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	/*
		read snapshot
	*/
	return rsm
}

func (rsm *RSM) Kill() {
	rsm.shutdown.Store(true)
	rsm.rf.Kill()
}

func (rsm *RSM) applyLoop() {

}
