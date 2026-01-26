package raft

import (
	"sync"
	"sync/atomic"

	raftpb "github.com/DecarbonizedGlucose/rkv/api/raftrpc"
	"github.com/DecarbonizedGlucose/rkv/internal/types"
	"google.golang.org/grpc"
)

const (
	Leader int = iota
	Candidate
	Follower
)

type Raft struct {
	mu                sync.Mutex                   // state lock
	peers             []*grpc.ClientConn           // ends of logical connections
	consensusEnds     []raftpb.RaftConsensusClient // consensus rpc peers
	snapshotEnds      []raftpb.RaftSnapshotClient  // snapshot rpc peers
	persister         *Persister
	me                int // this server's index into peers[]
	dead              int32
	currentTerm       int
	votedFor          int
	log               []*raftpb.LogEntry  // log entries
	commitIndex       int                 // highest log entry known to be committed
	lastApplied       int                 // highest log entry applied to state machine
	nextIndex         []int               // for each server, index of the next log entry to send
	matchIndex        []int               // for each server, index of highest log entry known to be replicated
	state             int                 // current state
	electionCh        chan struct{}       // election timeout notification
	resetELTimeoutCh  chan struct{}       // reset election timeout
	heartbeatCh       chan struct{}       // heartbeat timeout notification
	resetHBTimeoutCh  chan struct{}       // heartbeat timeout reset / heartbeat right now
	killCh            chan struct{}       // shutdown notification
	applyCh           chan types.ApplyMsg // channel to apply log entries to state machine
	applyCond         *sync.Cond          // condition variable to apply log entries
	lastIncludedIndex int                 // highest log entry included in snapshot
	lastIncludedTerm  int                 // highest log entry term included in snapshot
}

func Make(peers []*grpc.ClientConn, me int, persister *Persister, applyCh chan types.ApplyMsg) types.Raft {
	rf := &Raft{}
	return rf
}

/* ==================== Start and Kill ==================== */

func (rf *Raft) Start(command any) (int, int, bool) {
	return 0, 0, false
}

func (rf *Raft) Kill() {
	if !rf.killed() {
		atomic.StoreInt32(&rf.dead, 1)
	}
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

/* ==================== Election and Voting ==================== */

/* ==================== AppendEntries ==================== */

/* ==================== Persistence ==================== */

func (rf *Raft) Snapshot(index int, snapshot []byte) {

}

func (rf *Raft) PersistBytes() int {
	return 0
}

/* ==================== utils ==================== */

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, (rf.state == Leader)
}
