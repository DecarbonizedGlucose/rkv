package service

import (
	//"sync"
	"sync/atomic"
	"time"

	kvpb "github.com/DecarbonizedGlucose/rkv/api/kvrpc"
	"github.com/DecarbonizedGlucose/rkv/internal/raft"
	"github.com/DecarbonizedGlucose/rkv/internal/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type kvexecutor interface {
	Execute(req *kvpb.RequestWithMeta) *kvpb.Response
	Snapshot() []byte
	Restore(snapshot []byte)
}

type RaftApplier struct {
	//mu           sync.Mutex
	rf           *raft.Raft
	applyCh      chan *types.ApplyMsg
	maxraftstate int
	exec         kvexecutor
	shutdown     atomic.Bool
	waitingCmds  map[int]chan *kvpb.Response
}

func MakeRaftApplier(
	maxraftstate int,
	exec kvexecutor,
	rf *raft.Raft,
) *RaftApplier {
	ra := &RaftApplier{
		maxraftstate: maxraftstate,
		applyCh:      make(chan *types.ApplyMsg),
		exec:         exec,
		waitingCmds:  make(map[int]chan *kvpb.Response),
	}
	ra.shutdown.Store(false)
	ra.rf = rf
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

func (ra *RaftApplier) Submit(req *kvpb.RequestWithMeta) (res *kvpb.Response, err error) {
	if ra.shutdown.Load() {
		return nil, status.Error(codes.Unavailable, "server is shutting down")
	}

	index, term, isLeader := ra.rf.Start(req)
	if !isLeader {
		return nil, status.Error(codes.FailedPrecondition, "not the leader")
	}

	ra.waitingCmds[index] = make(chan *kvpb.Response)

	result, err := func() (*kvpb.Response, error) {
		timer := time.NewTimer(1500 * time.Millisecond)
		defer timer.Stop()
		for {
			if ra.shutdown.Load() {
				// this server peer is dead
				return nil, status.Error(codes.FailedPrecondition, "not the leader")
			}
			select {
			case <-timer.C:
				// timeout
				return nil, status.Error(codes.DeadlineExceeded, "timeout")
			case <-time.After(300 * time.Millisecond):
				currentTerm, stillLeader := ra.rf.GetState()
				if !stillLeader || currentTerm != term {
					// leader changed
					return nil, status.Error(codes.FailedPrecondition, "not the leader")
				}
			case res := <-ra.waitingCmds[index]:
				return res, nil
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
	req := msg.Command
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
