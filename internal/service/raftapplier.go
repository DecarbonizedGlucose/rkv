package service

import (
	//"sync"
	"sync/atomic"
	"time"

	kvpb "github.com/DecarbonizedGlucose/rkv/api/kvrpc"
	rapb "github.com/DecarbonizedGlucose/rkv/api/raftapplier"
	"github.com/DecarbonizedGlucose/rkv/internal/raft"
	"github.com/DecarbonizedGlucose/rkv/internal/types"
	"google.golang.org/grpc"
)

type kvexecutor interface {
	Execute(req *rapb.RequestWithMeta) *rapb.Response
	Snapshot() []byte
	Restore(snapshot []byte)
}

type RaftApplier struct {
	//mu           sync.Mutex
	rf           types.Raft
	applyCh      chan *types.ApplyMsg
	maxraftstate int
	exec         kvexecutor
	shutdown     atomic.Bool
	waitingCmds  map[int]chan *rapb.Response
}

func MakeRaftApplier(
	servers []*grpc.ClientConn,
	me int,
	persister *raft.Persister,
	maxraftstate int,
	exec kvexecutor,
) *RaftApplier {
	ra := &RaftApplier{
		maxraftstate: maxraftstate,
		applyCh:      make(chan *types.ApplyMsg),
		exec:         exec,
		waitingCmds:  make(map[int]chan *rapb.Response),
	}
	ra.shutdown.Store(false)
	ra.rf = raft.Make(servers, me, persister, ra.applyCh)
	/*
		read snapshot
	*/
	go ra.applyLoop()
	return ra
}

func (ra *RaftApplier) Kill() {
	ra.shutdown.Store(true)
	ra.rf.Kill()
}

func (ra *RaftApplier) Submit(req *rapb.RequestWithMeta) (res *rapb.Response, err kvpb.StatusCode) {
	if ra.shutdown.Load() {
		return nil, kvpb.StatusCode_NOT_LEADER
	}

	index, term, isLeader := ra.rf.Start(req)
	if !isLeader {
		return nil, kvpb.StatusCode_NOT_LEADER
	}

	ra.waitingCmds[index] = make(chan *rapb.Response)

	result, err := func() (*rapb.Response, kvpb.StatusCode) {
		timer := time.NewTimer(1500 * time.Millisecond)
		defer timer.Stop()
		for {
			if ra.shutdown.Load() {
				// this server peer is dead
				return nil, kvpb.StatusCode_NOT_LEADER
			}
			select {
			case <-timer.C:
				// timeout
				return nil, kvpb.StatusCode_TIMEOUT
			case <-time.After(300 * time.Millisecond):
				currentTerm, stillLeader := ra.rf.GetState()
				if !stillLeader || currentTerm != term {
					// leader changed
					return nil, kvpb.StatusCode_NOT_LEADER
				}
			case res := <-ra.waitingCmds[index]:
				return res, kvpb.StatusCode_OK
			}
		}
	}()

	return result, err
}

func (ra *RaftApplier) kill() {
	ra.shutdown.Store(true)
}

func (ra *RaftApplier) applyLoop() {
	for {
		msg, ok := <-ra.applyCh
		if !ok {
			ra.kill()
			return
		}
		if ra.shutdown.Load() {
			return
		}
		if msg.CommandValid {
			ra.applyCommand(msg)
		} else {
			ra.applySnapshot(msg)
		}
	}
}

func (ra *RaftApplier) applyCommand(msg *types.ApplyMsg) {
	req := &msg.Command
	res := ra.exec.Execute(req)
	if ch, exists := ra.waitingCmds[msg.CommandIndex]; exists {
		ch <- res
	}

	// when logs are too many...
	// generate new snapshot
	// implementation of snapshot may change,
	// so modify here anytime
	if ra.maxraftstate != 1 && ra.rf.PersistBytes() > (ra.maxraftstate*19)/20 {
		go ra.createSnapshot(msg.CommandIndex)
	}
}

func (ra *RaftApplier) applySnapshot(msg *types.ApplyMsg) {
	// TODO
}

func (ra *RaftApplier) createSnapshot(lastIncludedIndex int) {
	// TODO
}
