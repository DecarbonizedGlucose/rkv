package raft

import (
	"fmt"
	"log"
	"sort"

	"google.golang.org/protobuf/proto"

	"github.com/DecarbonizedGlucose/rkv/api/proto/pkg/raftpb"
)

type Raft struct {
	id        uint64
	state     stateType
	leader_id uint64

	hardState *raftpb.HardState
	raftLog   *RaftLog

	prs   map[uint64]*Progress
	votes map[uint64]bool

	msgs []*raftpb.RaftMessage // 待发送的消息

	electionTimeout           int
	randomizedElectionTimeout int
	electionElapsed           int

	heartbeatTimeout int
	heartbeatElapsed int
}

func newRaft(cfg *Config) (*Raft, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	hs, cs, err := cfg.Storage.InitialState()
	if err != nil {
		return nil, fmt.Errorf("raft: read initial state: %v", err)
	}

	peers := cs.Nodes
	if len(peers) == 0 {
		peers = cfg.Peers
	}
	prs := make(map[uint64]*Progress)
	for _, id := range peers {
		prs[id] = &Progress{}
	}

	log, err := newRaftLog(cfg.Storage)
	if err != nil {
		return nil, fmt.Errorf("raft: create log: %v", err)
	}

	return &Raft{
		id:                        cfg.ID,
		hardState:                 proto.Clone(hs).(*raftpb.HardState),
		state:                     stateFollower,
		prs:                       prs,
		votes:                     make(map[uint64]bool),
		raftLog:                   log,
		electionTimeout:           cfg.ElectionTimeout,
		randomizedElectionTimeout: randomizedElectionTimeout(cfg.ElectionTimeout),
		heartbeatTimeout:          cfg.HeartbeatTimeout,
	}, nil
}

// ========================================
// 状态机相关
// ========================================

func (r *Raft) resetRandomizedElectionTimeout() {
	r.randomizedElectionTimeout = randomizedElectionTimeout(r.electionTimeout)
}

func (r *Raft) tick() {
	switch r.state {
	case stateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			//r.broadcastHeartbeat()
			msg := &raftpb.RaftMessage{
				Type: raftpb.MessageType_HEARTBEAT,
			}
			r.step(msg) // local message, 直接发给自己处理
		}
	case stateCandidate, stateFollower:
		r.electionElapsed++
		if r.electionElapsed >= r.randomizedElectionTimeout {
			r.electionElapsed = 0
			r.resetRandomizedElectionTimeout()
			//r.startElection()
			msg := &raftpb.RaftMessage{
				Type: raftpb.MessageType_HUP,
			}
			r.step(msg) // local message, 直接发给自己处理
		}
	}
}

func (r *Raft) becomeFollower(term, leader_id uint64) {
	r.state = stateFollower
	r.leader_id = leader_id
	r.electionElapsed = 0

	if term > r.hardState.Term {
		r.hardState.Term = term
		r.hardState.Vote = 0
	}
}

func (r *Raft) becomeCandidate() {
	r.state = stateCandidate
	r.leader_id = 0
	r.electionElapsed = 0
	r.hardState.Term++
	r.hardState.Vote = r.id
	r.votes = map[uint64]bool{r.id: true}
}

func (r *Raft) becomeLeader() {
	r.state = stateLeader
	r.leader_id = r.id
	r.electionElapsed = 0

	for _, pr := range r.prs {
		pr.MatchIndex = 0
		pr.NextIndex = r.raftLog.lastLogIndex() + 1
	}

	// Raft 安全性：新 Leader 不能直接提交前任 term 的 entry。
	// 追加一条空 no-op entry（Data=nil）并广播，当 no-op 被多数派确认后，
	// CommitIndex 推进，前任遗留的已复制 entry 随之全部被 apply。
	// StateMachine.Apply 对 Data==nil 的 entry 直接跳过，对上层透明。
	r.handlePropose(&raftpb.Entry{})
}

func (r *Raft) maybeCommit() {
	matches := make([]uint64, 0, len(r.prs))
	for _, pr := range r.prs {
		matches = append(matches, pr.MatchIndex)
	}
	sort.Slice(matches, func(i, j int) bool {
		return matches[i] > matches[j]
	})
	majorityMatch := matches[len(matches)/2]

	if r.raftLog.matchTerm(majorityMatch, r.hardState.Term) && majorityMatch > r.hardState.CommitIndex {
		r.hardState.CommitIndex = majorityMatch
	}
}

// ========================================
// RPC 发送和处理核心
// ========================================

func (r *Raft) step(m *raftpb.RaftMessage) error {
	// 忽略非法 / 传输层消息
	if m.Type == raftpb.MessageType_UNSPECIFIED || m.Type == raftpb.MessageType_HANDSHAKE {
		return nil
	}

	// 通用term守卫，收到更高term马上降级
	if m.Term > r.hardState.Term {
		r.becomeFollower(m.Term, 0) // 允许暂时未知 Leader
	}

	switch m.Type {
	case raftpb.MessageType_HUP:
		if r.state != stateLeader {
			r.startElection()
		}

	case raftpb.MessageType_HEARTBEAT:
		if r.state == stateLeader {
			r.broadcastHeartbeat()
		}

	case raftpb.MessageType_PROPOSE:
		if r.state != stateLeader {
			return ErrNotLeader
		}
		r.handlePropose(m.Body.(*raftpb.RaftMessage_Propose).Propose)

	case raftpb.MessageType_APPEND_REQ:
		r.handleAppend(m)

	case raftpb.MessageType_APPEND_RESP:
		r.handleAppendResponse(m)

	case raftpb.MessageType_REQUEST_VOTE_REQ:
		r.handleRequestVote(m)

	case raftpb.MessageType_REQUEST_VOTE_RESP:
		r.handleRequestVoteResponse(m)

	case raftpb.MessageType_INSTALL_SNAPSHOT_REQ:
		r.handleInstallSnapshot(m)

	case raftpb.MessageType_INSTALL_SNAPSHOT_RESP:
		r.handleInstallSnapshotResponse(m)
	}
	return nil
}

func (r *Raft) send(m *raftpb.RaftMessage) {
	r.msgs = append(r.msgs, m)
}

func (r *Raft) broadcastHeartbeat() {
	for id := range r.prs {
		if id != r.id {
			r.sendAppend(id)
		}
	}
}

func (r *Raft) sendAppend(to uint64) {
	pr := r.prs[to]

	if pr.NextIndex <= r.raftLog.getLastIncluded() {
		r.sendInstallSnapshot(to)
		return
	}

	req := &raftpb.AppendEntriesRequest{
		Term:         r.hardState.Term,
		LeaderId:     r.id,
		PrevLogIndex: pr.NextIndex - 1,
		PrevLogTerm:  r.raftLog.term(pr.NextIndex - 1),
		Entries:      r.raftLog.slice(pr.NextIndex, r.raftLog.lastLogIndex()+1),
		LeaderCommit: r.hardState.CommitIndex,
	}
	msg := &raftpb.RaftMessage{
		From: r.id,
		To:   to,
		Type: raftpb.MessageType_APPEND_REQ,
		Term: r.hardState.Term,
		Body: &raftpb.RaftMessage_AppendReq{
			AppendReq: req,
		},
	}
	r.send(msg)
}

func (r *Raft) handleAppend(m *raftpb.RaftMessage) {
	req := m.Body.(*raftpb.RaftMessage_AppendReq).AppendReq
	resp := &raftpb.AppendEntriesResponse{
		Term:    r.hardState.Term,
		Success: false,
	}
	msg := &raftpb.RaftMessage{
		From: r.id,
		To:   m.From,
		Type: raftpb.MessageType_APPEND_RESP,
		Term: r.hardState.Term,
		Body: &raftpb.RaftMessage_AppendResp{
			AppendResp: resp,
		},
	}

	if m.Term < r.hardState.Term {
		r.send(msg)
		return
	}

	// step() 已处理 m.Term > hardState.Term 的降级，这里只为记录 leader 身份
	r.becomeFollower(m.Term, m.From)
	msg.Term = r.hardState.Term

	// 日志没有可插入点
	if req.PrevLogIndex < r.raftLog.getLastIncluded() || req.PrevLogIndex > r.raftLog.lastLogIndex() {
		r.send(msg)
		return
	}

	// 日志不匹配, 回退寻找插入点
	if r.raftLog.term(req.PrevLogIndex) != req.PrevLogTerm {
		conflictTerm := r.raftLog.term(req.PrevLogIndex)
		idx := req.PrevLogIndex
		for idx > r.raftLog.getLastIncluded() && r.raftLog.term(idx-1) == conflictTerm {
			idx--
		}
		resp.ConflictIndex = idx
		resp.ConflictTerm = conflictTerm
		r.send(msg)
		return
	}

	// 日志匹配, 插入日志
	resp.Success = true
	r.raftLog.trunc(req.PrevLogIndex + 1)
	r.raftLog.append(copyEntries(req.Entries)...)
	resp.LastLogIndex = r.raftLog.lastLogIndex()

	if req.LeaderCommit > r.hardState.CommitIndex {
		r.hardState.CommitIndex = min(req.LeaderCommit, r.raftLog.lastLogIndex())
	}
	r.send(msg)
}

func (r *Raft) handleAppendResponse(m *raftpb.RaftMessage) {
	// step() 已处理 m.Term > hardState.Term 的降级
	if m.Term < r.hardState.Term {
		return
	}

	resp := m.Body.(*raftpb.RaftMessage_AppendResp).AppendResp

	pr := r.prs[m.From]
	if pr == nil {
		return
	}

	if resp.Success {
		pr.MatchIndex = resp.LastLogIndex
		pr.NextIndex = resp.LastLogIndex + 1
	} else {
		if resp.ConflictTerm == 0 {
			pr.NextIndex = resp.ConflictIndex
		} else {
			idx, ok := r.raftLog.lastIndexOfTerm(resp.ConflictTerm)
			if ok {
				pr.NextIndex = idx + 1
			} else {
				pr.NextIndex = resp.ConflictIndex
			}
		}
	}

	if pr.MatchIndex > pr.NextIndex {
		pr.MatchIndex = pr.NextIndex - 1
	}

	r.maybeCommit()
}

func (r *Raft) startElection() {
	r.becomeCandidate()

	if len(r.prs) == 1 {
		r.becomeLeader()
		return
	}

	req := &raftpb.RequestVoteRequest{
		Term:         r.hardState.Term,
		CandidateId:  r.id,
		LastLogIndex: r.raftLog.lastLogIndex(),
		LastLogTerm:  r.raftLog.lastLogTerm(),
	}

	for pr := range r.prs {
		if pr != r.id {
			r.sendRequestVote(pr, req)
		}
	}
}

func (r *Raft) sendRequestVote(to uint64, req *raftpb.RequestVoteRequest) {
	r.send(&raftpb.RaftMessage{
		From: r.id,
		To:   to,
		Type: raftpb.MessageType_REQUEST_VOTE_REQ,
		Term: r.hardState.Term,
		Body: &raftpb.RaftMessage_VoteReq{
			VoteReq: req,
		},
	})
}

func (r *Raft) handleRequestVote(m *raftpb.RaftMessage) {
	req := m.Body.(*raftpb.RaftMessage_VoteReq).VoteReq
	resp := &raftpb.RequestVoteResponse{
		Term:        r.hardState.Term,
		VoteGranted: false,
		VoterId:     r.id,
	}
	msg := &raftpb.RaftMessage{
		From: r.id,
		To:   m.From,
		Type: raftpb.MessageType_REQUEST_VOTE_RESP,
		Term: r.hardState.Term,
		Body: &raftpb.RaftMessage_VoteResp{
			VoteResp: resp,
		},
	}

	if req.Term < r.hardState.Term {
		r.send(msg)
		return
	}

	// step() 已处理 m.Term > hardState.Term 的降级，此处只更新 term 后的状态
	r.becomeFollower(req.Term, r.leader_id)
	msg.Term = r.hardState.Term

	if r.hardState.Vote != 0 && r.hardState.Vote != m.From { // 幂等判断
		// 已经投给其他人了
		r.send(msg)
		return
	}

	lastLogIndex := r.raftLog.lastLogIndex()
	lastLogTerm := r.raftLog.lastLogTerm()
	upToDate := func() bool {
		if req.LastLogTerm != lastLogTerm {
			return req.LastLogTerm > lastLogTerm
		} else {
			return req.LastLogIndex >= lastLogIndex
		}
	}

	granted := upToDate()
	resp.VoteGranted = granted

	if granted {
		r.hardState.Vote = m.From
		r.electionElapsed = 0
	}

	r.send(msg)
}

func (r *Raft) handleRequestVoteResponse(m *raftpb.RaftMessage) {
	// 只有 Candidate 才处理投票响应；Leader/Follower 收到的是过期消息。
	if r.state != stateCandidate {
		return
	}
	if m.Term < r.hardState.Term {
		return
	}

	resp := m.Body.(*raftpb.RaftMessage_VoteResp).VoteResp
	r.votes[m.From] = resp.VoteGranted

	granted, rejected := 0, 0
	for _, v := range r.votes {
		if v {
			granted++
		} else {
			rejected++
		}
	}

	maj := len(r.prs)/2 + 1
	if granted >= maj {
		r.becomeLeader()
	} else if rejected >= maj {
		r.becomeFollower(r.hardState.Term, 0)
	}
}

func (r *Raft) sendInstallSnapshot(to uint64) {
	// TODO: 需要优化
	// 1. 避免每次都发送整个快照

	snapshot, err := r.raftLog.storage.Snapshot()
	if err == ErrSnapshotTemporarilyUnavailable {
		// 快照尚未就绪，等下次心跳重试，属正常路径，不记录。
		return
	}
	if err != nil {
		log.Printf("raft[%d]: read snapshot for peer %d: %v", r.id, to, err)
		return
	}
	if snapshot.Metadata == nil {
		// Metadata 为 nil 是本地构造 bug，不会因重试而自愈。
		log.Printf("raft[%d]: BUG: snapshot metadata is nil, cannot send to peer %d", r.id, to)
		return
	}
	data, err := proto.Marshal(snapshot)
	if err != nil {
		log.Printf("raft[%d]: marshal snapshot for peer %d: %v", r.id, to, err)
		return
	}
	req := &raftpb.InstallSnapshotRequest{
		Term:              r.hardState.Term,
		LeaderId:          r.id,
		LastIncludedIndex: r.raftLog.getLastIncluded(),
		LastIncludedTerm:  r.raftLog.term(r.raftLog.getLastIncluded()),
		Data:              data,
	}
	msg := &raftpb.RaftMessage{
		From: r.id,
		To:   to,
		Type: raftpb.MessageType_INSTALL_SNAPSHOT_REQ,
		Term: r.hardState.Term,
		Body: &raftpb.RaftMessage_SnapReq{
			SnapReq: req,
		},
	}
	r.send(msg)
}

func (r *Raft) handleInstallSnapshot(m *raftpb.RaftMessage) {
	req := m.Body.(*raftpb.RaftMessage_SnapReq).SnapReq
	resp := &raftpb.InstallSnapshotResponse{
		Term:    r.hardState.Term,
		Success: false,
	}
	msg := &raftpb.RaftMessage{
		From: r.id,
		To:   m.From,
		Type: raftpb.MessageType_INSTALL_SNAPSHOT_RESP,
		Term: r.hardState.Term,
		Body: &raftpb.RaftMessage_SnapResp{
			SnapResp: resp,
		},
	}

	if r.hardState.Term > req.Term || req.Data == nil {
		r.send(msg)
		return
	}
	if req.LastIncludedIndex <= r.raftLog.getLastIncluded() || req.LastIncludedIndex < r.hardState.CommitIndex {
		r.send(msg)
		return
	}

	newLog := []*raftpb.Entry{{Term: req.LastIncludedTerm}}
	if req.LastIncludedIndex < r.raftLog.lastLogIndex() {
		relPos := req.LastIncludedIndex - r.raftLog.getLastIncluded()
		newLog = append(newLog, r.raftLog.entries[relPos+1:]...)
	}
	r.raftLog.entries = newLog
	r.raftLog.setLastIncluded(req.LastIncludedIndex)
	r.raftLog.stabledIndex = req.LastIncludedIndex
	r.raftLog.appliedIndex = req.LastIncludedIndex
	r.hardState.CommitIndex = max(r.hardState.CommitIndex, req.LastIncludedIndex)

	snapshot := &raftpb.Snapshot{}
	err := proto.Unmarshal(req.Data, snapshot)
	if err != nil || r.raftLog.getLastIncluded() > snapshot.Metadata.LastIncludedIndex {
		r.send(msg)
		return
	}

	r.becomeFollower(req.Term, m.From)
	msg.Term = r.hardState.Term
	r.electionElapsed = 0

	resp.Success = true

	r.raftLog.pendingSnapshot = snapshot

	r.send(msg)
}

func (r *Raft) handleInstallSnapshotResponse(m *raftpb.RaftMessage) {
	// step() 已处理 m.Term > hardState.Term 的降级
	if m.Term < r.hardState.Term {
		return
	}

	resp := m.Body.(*raftpb.RaftMessage_SnapResp).SnapResp
	if !resp.Success {
		return
	}

	pr := r.prs[m.From]
	if pr == nil {
		return
	}
	pr.MatchIndex = r.raftLog.getLastIncluded()
	pr.NextIndex = r.raftLog.getLastIncluded() + 1
}

func copyEntries(src []*raftpb.Entry) []*raftpb.Entry {
	dst := make([]*raftpb.Entry, len(src))
	for i, e := range src {
		dst[i] = proto.Clone(e).(*raftpb.Entry)
	}
	return dst
}

func (r *Raft) handlePropose(entry *raftpb.Entry) {
	index := r.raftLog.lastLogIndex() + 1
	entry.Index = index           // 补充rawnode.go不设置的日志索引
	entry.Term = r.hardState.Term // 补充rawnode.go不设置的日志任期
	r.raftLog.append(entry)
	r.prs[r.id].MatchIndex = index
	r.prs[r.id].NextIndex = index + 1
	r.broadcastHeartbeat()
	r.maybeCommit()
}

// ========================================
// Ready 相关
// ========================================

func (r *Raft) clearMsgs() {
	r.msgs = r.msgs[:0]
}
