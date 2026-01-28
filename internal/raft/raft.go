package raft

import (
	"sync"
	"sync/atomic"
	"time"

	rapb "github.com/DecarbonizedGlucose/rkv/api/raftapplier"
	raftpb "github.com/DecarbonizedGlucose/rkv/api/raftrpc"
	"github.com/DecarbonizedGlucose/rkv/internal/types"
	"github.com/DecarbonizedGlucose/rkv/internal/utils"
	"google.golang.org/grpc"
)

/* ==================== Definition and Construction ==================== */

const (
	Leader int = iota
	Candidate
	Follower
)

type Raft struct {
	peers           []*grpc.ClientConn             // ends of logical connections
	consensusEnds   []raftpb.RaftConsensusClient   // consensus rpc peers
	persistenceEnds []raftpb.RaftPersistenceClient // persistence rpc peers
	persister       *Persister
	me              int        // this server's index into peers[]
	mu              sync.Mutex // state lock

	dead             int32
	currentTerm      int64
	votedFor         int
	log              []*raftpb.LogEntry   // log entries
	commitIndex      int                  // highest log entry known to be committed
	lastApplied      int                  // highest log entry applied to state machine
	nextIndex        []int                // for each server, index of the next log entry to send
	matchIndex       []int                // for each server, index of highest log entry known to be replicated
	state            int                  // current state
	electionCh       chan struct{}        // election timeout notification
	resetELTimeoutCh chan struct{}        // reset election timeout
	heartbeatCh      chan struct{}        // heartbeat timeout notification
	resetHBTimeoutCh chan struct{}        // heartbeat timeout reset / heartbeat right now
	killCh           chan struct{}        // shutdown notification
	applyCh          chan *types.ApplyMsg // channel to apply log entries to state machine
	applyCond        *sync.Cond           // condition variable to apply log entries

	lastIncludedIndex int   // highest log entry included in snapshot
	lastIncludedTerm  int64 // highest log entry term included in snapshot
}

func Make(peers []*grpc.ClientConn, me int, persister *Persister, applyCh chan *types.ApplyMsg) types.Raft {
	rf := &Raft{}
	rf.me = me
	rf.peers = peers
	n := len(peers)
	rf.consensusEnds = make([]raftpb.RaftConsensusClient, n)
	rf.persistenceEnds = make([]raftpb.RaftPersistenceClient, n)
	for i, peer := range rf.peers {
		if i == rf.me {
			rf.consensusEnds[i] = nil
			rf.persistenceEnds[i] = nil
		} else {
			rf.consensusEnds[i] = raftpb.NewRaftConsensusClient(peer)
			rf.persistenceEnds[i] = raftpb.NewRaftPersistenceClient(peer)
		}
	}
	rf.mu = sync.Mutex{}
	atomic.StoreInt32(&rf.dead, 0)
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = append([]*raftpb.LogEntry(nil), &raftpb.LogEntry{Log: nil, Term: 0})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, n)
	rf.matchIndex = make([]int, n)
	rf.state = Follower
	rf.electionCh = make(chan struct{}, 1)
	rf.resetELTimeoutCh = make(chan struct{}, 1)
	rf.heartbeatCh = make(chan struct{}, 1)
	rf.resetHBTimeoutCh = make(chan struct{}, 1)
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	// Read Snapshot
	// rf.readPersist(persisier.ReadRaftState)
	rf.commitIndex = max(rf.commitIndex, rf.lastIncludedIndex)
	rf.lastApplied = max(rf.lastApplied, rf.lastIncludedIndex)

	go rf.applier()
	go rf.electionTimer()
	go rf.heartbeatTimer()
	go rf.ticker()

	return rf
}

/* ==================== Life and State ==================== */

func (rf *Raft) Start(command *rapb.RequestWithMeta) (int, int64, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return -1, rf.currentTerm, false
	}
	rf.heartbeatNow()
	index := rf.getLastLogIndex() + 1
	term := rf.currentTerm
	newLog := &raftpb.LogEntry{Log: command, Term: term}
	rf.log = append(rf.log, newLog)
	// TODO
	// rf.persist()
	rf.nextIndex[rf.me] = index + 1
	rf.matchIndex[rf.me] = index
	return index, term, true
}

func (rf *Raft) Kill() {
	if !rf.killed() {
		atomic.StoreInt32(&rf.dead, 1)
	}
}

func (rf *Raft) killed() bool {
	d := atomic.LoadInt32(&rf.dead)
	return d == 1
}

func (rf *Raft) GetState() (int64, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, (rf.state == Leader)
}

/* ==================== Election and Voting ==================== */

func (rf *Raft) RequestVote(
	req *raftpb.RequestVoteRequest,
	reply *raftpb.RequestVoteResponse,
) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		return
	}
	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	if req.Term < rf.currentTerm {
		// refuse old term vote
		return
	}
	if req.Term > rf.currentTerm {
		// update term and convert to follower
		rf.initFollowerState(req.Term)
		rf.resetElectionTimer()
	}

	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.getLastLogTerm()
	upToDate := func() bool {
		if req.LastLogTerm != lastLogTerm {
			return req.LastLogTerm > lastLogTerm
		} else {
			return req.LastLogIndex >= int64(lastLogIndex)
		}
	}
	if (rf.votedFor == -1 || int64(rf.votedFor) == req.CandidateId) && upToDate() {
		// vote for candidate
		rf.votedFor = int(req.CandidateId)
		reply.VoteGranted = true
		rf.resetElectionTimer()
		// TODO
		// rf.persist()
	}
}

func (rf *Raft) sendRequestVote(
	server int,
	req *raftpb.RequestVoteRequest,
	voteCount *int32,
) {
	res := &raftpb.RequestVoteResponse{}
	ok := rf.callRequestVote(rf.consensusEnds[server], req, res)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() || rf.state != Candidate || req.Term != rf.currentTerm {
		return
	}
	if res.Term > rf.currentTerm {
		// finding higher term, become follower
		rf.initFollowerState(res.Term)
		rf.resetElectionTimer()
		return
	}
	if res.VoteGranted {
		if atomic.AddInt32(voteCount, 1) > int32(len(rf.peers)/2) {
			// votes has been over than half
			// become leader
			rf.initLeaderState()
			rf.heartbeatNow()
		}
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	if rf.killed() || rf.state == Leader {
		rf.mu.Unlock()
		return
	}
	rf.initCandidateState()
	rf.resetElectionTimer()
	req := &raftpb.RequestVoteRequest{
		Term:         rf.currentTerm,
		CandidateId:  int64(rf.me),
		LastLogIndex: int64(rf.getLastLogIndex()),
		LastLogTerm:  rf.getLastLogTerm(),
	}
	rf.mu.Unlock()

	var voteCount int32 = 1 // vote for self
	for i := range rf.peers {
		if i != rf.me {
			go rf.sendRequestVote(i, req, &voteCount)
		}
	}
}

/* ==================== Append Entries ==================== */

func (rf *Raft) AppendEntries(
	req *raftpb.AppendEntriesRequest,
	res *raftpb.AppendEntriesResponse,
) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		return
	}
	res.Success = false
	res.Term = rf.currentTerm

	if req.Term < rf.currentTerm {
		return
	}
	if req.Term > rf.currentTerm || rf.state != Follower {
		// update term and become follower
		rf.initFollowerState(req.Term)
	}
	rf.resetElectionTimer()

	if req.PrevLogIndex < int64(rf.lastIncludedIndex) || req.PrevLogIndex > int64(rf.getLastLogIndex()) {
		// log dismatch, and no place to insert
		res.ConflictIndex = int64(rf.getLastLogIndex() + 1)
		res.ConflictTerm = -1
		return
	}
	if rf.getTerm(int(req.PrevLogIndex)) != req.PrevLogTerm {
		// log dismatch, find conflict term and index
		conflictTerm := rf.getTerm(int(req.PrevLogIndex))
		idx := req.PrevLogIndex
		for rf.getRelPos(int(idx)) >= 1 && rf.getTerm(int(idx)-1) == conflictTerm {
			idx--
		}
		res.ConflictTerm = conflictTerm
		res.ConflictIndex = idx
		return
	}
	// log match, append new entries
	res.Success = true
	index := req.PrevLogIndex + 1
	var i int64
	for i = 0; i < int64(len(req.Entries)); i++ {
		if index+i <= int64(rf.getLastLogIndex()) {
			if rf.getTerm(int(index+i)) != req.Entries[i].Term {
				// cut off the conflicting entries and all that follow it
				rf.log = rf.log[:rf.getRelPos(int(index+i))]
				rf.log = append(rf.log, req.Entries[i:]...)
				break
			}
		} else {
			// append new entries directly
			rf.log = append(rf.log, req.Entries[i:]...)
			break
		}
	}
	// TODO
	// rf.persist()
	// update commit index
	if req.LeaderCommit > int64(rf.commitIndex) {
		rf.commitIndex = min(int(req.LeaderCommit), rf.getLastLogIndex())
		rf.applyCond.Signal()
	}
}

func (rf *Raft) sendAppendEntries(server int, req *raftpb.AppendEntriesRequest) {
	rf.mu.Lock()
	if rf.killed() || rf.state == Leader || req.Term != rf.currentTerm {
		rf.mu.Unlock()
		return
	}
	nextIndex := rf.nextIndex[server]
	if nextIndex <= rf.lastIncludedIndex {
		// needs to send snapshot
		rf.mu.Unlock()
		go rf.sendInstallSnapshot(server)
		return
	}
	req.PrevLogIndex = int64(nextIndex - 1)
	req.PrevLogTerm = rf.getTerm(nextIndex - 1)
	req.Entries = append([]*raftpb.LogEntry(nil), rf.log[rf.getRelPos(nextIndex):]...)
	rf.mu.Unlock()
	res := &raftpb.AppendEntriesResponse{}
	ok := rf.callAppendEntries(rf.consensusEnds[server], req, res)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() || rf.state != Leader || req.Term != rf.currentTerm {
		// already not leader or term changed
		return
	}
	if res.Term > rf.currentTerm {
		// fonud higher term, become follower
		rf.initFollowerState(res.Term)
		rf.resetElectionTimer()
		return
	}
	if res.Success {
		rf.matchIndex[server] = int(req.PrevLogIndex) + len(req.Entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1
		lastLogIndex := rf.getLastLogIndex()
		for N := lastLogIndex; N > rf.commitIndex; N-- {
			if N <= rf.lastIncludedIndex {
				break
			}
			if rf.getTerm(N) != rf.currentTerm {
				continue
			}
			count := 1
			for i := range rf.peers {
				if i != rf.me && rf.matchIndex[i] >= N {
					count++
				}
			}
			if count > len(rf.peers)/2 {
				rf.commitIndex = N
				rf.applyCond.Signal()
				break
			}
		}
	} else {
		if res.ConflictTerm == -1 {
			// its log is too short or too long
			rf.nextIndex[server] = int(res.ConflictIndex)
		} else {
			// find the conflict term's first index
			conflictIndex := -1
			for i := rf.getLastLogIndex(); i >= int(res.ConflictIndex); i-- {
				if rf.getTerm(i) == res.ConflictTerm {
					conflictIndex = i
					break
				}
			}
			if conflictIndex == -1 {
				// no log of this term at local
				rf.nextIndex[server] = int(res.ConflictIndex)
			} else {
				// go back to the first place of the term
				rf.nextIndex[server] = conflictIndex
			}
		}
	}
}

func (rf *Raft) broadcastHeartbeat() {
	rf.mu.Lock()
	if rf.killed() || rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	req := &raftpb.AppendEntriesRequest{
		Term:         rf.currentTerm,
		LeaderId:     int64(rf.me),
		LeaderCommit: int64(rf.commitIndex),
	}
	rf.mu.Unlock()
	for i := range rf.peers {
		if rf.killed() {
			break
		}
		if i == rf.me {
			continue
		}
		go rf.sendAppendEntries(i, req)
	}
}

/* ==================== Persistence ==================== */

func (rf *Raft) InstallSnapshot(
	req *raftpb.InstallSnapshotRequest,
	res *raftpb.InstallSnapshotResponse,
) {
	rf.mu.Lock()

	res.Term = rf.currentTerm
	if req.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}
	if req.Term > rf.currentTerm {
		rf.initFollowerState(req.Term)
	}
	rf.resetElectionTimer()
	if req.LastIncludedIndex <= int64(rf.lastIncludedIndex) || req.LastIncludedIndex < int64(rf.commitIndex) {
		// snapshot is older than existing one
		rf.mu.Unlock()
		return
	}

	// cut log
	newLog := append([]*raftpb.LogEntry(nil), &raftpb.LogEntry{Log: nil, Term: 0})
	// save log entries after lastIncludedIndex
	if req.LastIncludedIndex <= int64(rf.getLastLogIndex()) {
		relPos := rf.getRelPos(int(req.LastIncludedIndex))
		newLog = append(newLog, rf.log[relPos+1:]...)
	}
	rf.log = newLog

	rf.lastIncludedIndex = int(req.LastIncludedIndex)
	rf.lastIncludedTerm = req.LastIncludedTerm
	rf.commitIndex = max(rf.commitIndex, int(req.LastIncludedIndex))
	rf.lastApplied = max(rf.lastApplied, int(req.LastIncludedIndex))
	// persist state and snapshot
	// TODO
	//rf.persistWithSnapshot(req.Data)

	msg := &types.ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      req.Data,
		SnapshotTerm:  req.Term,
		SnapshotIndex: int(req.LastIncludedIndex),
	}
	rf.mu.Unlock()
	rf.applyCh <- msg
}

func (rf *Raft) sendInstallSnapshot(server int) {
	rf.mu.Lock()
	if rf.state != Leader || rf.killed() {
		rf.mu.Unlock()
		return
	}
	req := &raftpb.InstallSnapshotRequest{
		Term:              rf.currentTerm,
		LeaderId:          int64(rf.me),
		LastIncludedIndex: int64(rf.lastIncludedIndex),
		LastIncludedTerm:  rf.lastIncludedTerm,
		//TODO
		//Data: rf.persister.ReadSnapshot(),
	}
	rf.mu.Unlock()
	res := &raftpb.InstallSnapshotResponse{}
	ok := rf.callInstallSnapshot(rf.persistenceEnds[server], req, res)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if req.Term != rf.currentTerm || rf.state != Leader || rf.killed() {
		return
	}
	if res.Term > rf.currentTerm {
		rf.initFollowerState(res.Term)
		return
	}
	// update nextIndex and match index
	rf.nextIndex[server] = rf.lastIncludedIndex + 1
	rf.matchIndex[server] = rf.lastIncludedIndex
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {

}

func (rf *Raft) PersistBytes() int {
	return 0
}

/* ==================== Goroutines and Timers ==================== */

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.applyCond.L.Lock()
		for rf.commitIndex <= rf.lastApplied && !rf.killed() {
			rf.applyCond.Wait()
		}
		if rf.killed() {
			rf.applyCond.L.Unlock()
			return
		}

		if rf.lastApplied < rf.lastIncludedIndex {
			// apply wrapped snapshot
			rf.lastApplied = rf.lastIncludedIndex
			msg := &types.ApplyMsg{
				CommandValid:  false,
				SnapshotValid: true,
				// TODO
				//Snapshot: rf.persister.ReadSnapshot(),
				SnapshotIndex: rf.lastIncludedIndex,
			}
			rf.applyCh <- msg
			rf.lastApplied = rf.lastIncludedIndex
			rf.applyCond.L.Unlock()
			continue
		}

		if rf.commitIndex > rf.lastApplied {
			// apply raw log entries
			start := rf.lastApplied + 1
			end := rf.commitIndex
			for i := start; i <= end; i++ {
				if i <= rf.lastIncludedIndex {
					continue
				}
				relPos := rf.getRelPos(i)
				if relPos < 0 || relPos > len(rf.log) {
					break
				}
				msg := &types.ApplyMsg{
					SnapshotValid: false,
					CommandValid:  true,
					Command:       rf.log[relPos].Log,
					CommandIndex:  i,
				}
				rf.applyCond.L.Unlock()
				rf.applyCh <- msg
				rf.applyCond.L.Lock()
				rf.lastApplied = i
			}
			rf.applyCond.L.Unlock()
		} else {
			rf.applyCond.L.Unlock()
		}
	}
}

func (rf *Raft) electionTimer() {
	timer := time.NewTimer(utils.RandomTimeout())
	defer timer.Stop()

	for !rf.killed() {
		select {
		case <-timer.C:
			// election timeout, notify to start election
			rf.mu.Lock()
			isLeader := rf.state == Leader
			rf.mu.Unlock()
			if !isLeader {
				select {
				case rf.electionCh <- struct{}{}:
				default:
				}
			}
			timer.Reset(utils.RandomTimeout())
		case <-rf.resetELTimeoutCh:
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(utils.RandomTimeout())
		case <-rf.killCh:
			if !timer.Stop() {
				<-timer.C
			}
			return
		}
	}
}

func (rf *Raft) resetElectionTimer() {
	select {
	case rf.resetELTimeoutCh <- struct{}{}:
	default:
	}
}

func (rf *Raft) heartbeatTimer() {
	timer := time.NewTimer(utils.ConstTimeout())
	defer timer.Stop()

	for !rf.killed() {
		select {
		case <-timer.C:
			// heartbeat notify when timeout
			rf.mu.Lock()
			isLeader := rf.state == Leader
			rf.mu.Unlock()
			if isLeader {
				select {
				case rf.heartbeatCh <- struct{}{}:
				default:
				}
			}
			timer.Reset(utils.ConstTimeout())
		case <-rf.resetHBTimeoutCh:
			// heart beat at once
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(0)
		case <-rf.killCh:
			if !timer.Stop() {
				<-timer.C
			}
			return
		}
	}
}

func (rf *Raft) heartbeatNow() {
	select {
	case rf.resetHBTimeoutCh <- struct{}{}:
	default:
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		select {
		case <-rf.electionCh:
			go rf.startElection()
		case <-rf.heartbeatCh:
			rf.mu.Lock()
			isLeader := rf.state == Leader
			rf.mu.Unlock()
			if isLeader {
				go rf.broadcastHeartbeat()
			}
		case <-rf.killCh:
			return
		}
	}
}

/* ==================== Utils ==================== */

func (rf *Raft) getRelPos(absPos int) int {
	return absPos - rf.lastIncludedIndex
}

func (rf *Raft) getLastLogIndex() int {
	if len(rf.log) == 0 {
		return rf.lastIncludedIndex
	}
	return rf.lastIncludedIndex + len(rf.log) - 1
}

func (rf *Raft) getLastLogTerm() int64 {
	if len(rf.log) == 0 {
		return rf.lastIncludedTerm
	}
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) getTerm(index int) int64 {
	if index == rf.lastIncludedIndex {
		return rf.lastIncludedTerm
	}
	if index < rf.lastIncludedIndex || rf.getRelPos(index) >= len(rf.log) {
		return -1
	}
	return rf.log[rf.getRelPos(index)].Term
}

// MUST CALLED WITHIN LOCK
func (rf *Raft) initLeaderState() {
	rf.state = Leader
	rf.votedFor = -1
	lastLogIndex := rf.getLastLogIndex()
	for i := range rf.peers {
		rf.nextIndex[i] = lastLogIndex + 1
		rf.matchIndex[i] = 0
	}
	rf.matchIndex[rf.me] = lastLogIndex
}

// MUST CALLED WITHIN LOCK
func (rf *Raft) initFollowerState(newTerm int64) {
	rf.state = Follower
	rf.currentTerm = newTerm
	rf.votedFor = -1
	// TODO
	// rf.persist()
}

// MUST CALLED WITHIN LOCK
func (rf *Raft) initCandidateState() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	// TODO
	// rf.persist()
}
