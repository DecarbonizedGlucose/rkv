package raft

import (
	"github.com/DecarbonizedGlucose/rkv/api/proto/pkg/raftpb"
	"google.golang.org/protobuf/proto"
)

type SoftState struct {
	LeaderID  uint64    // 当前集群 Leader 的节点 ID
	RaftState stateType // 当前节点角色：Follower / Candidate / Leader
}

type Ready struct {
	*SoftState                             // 角色变化时才非 nil
	HardState        *raftpb.HardState     // term、vote、commit，变化时才非 nil
	Entries          []*raftpb.Entry       // 尚未持久化的日志条目，需写入 WAL
	Snapshot         *raftpb.Snapshot      // Follower 待应用的快照，非 nil 时优先处理
	CommittedEntries []*raftpb.Entry       // 已提交、待应用到状态机的日志条目
	Messages         []*raftpb.RaftMessage // 待发送给其他节点的 Raft 消息
}

type PrevState struct {
	PrevSoftState *SoftState
	PrevHardState *raftpb.HardState
}

type RawNode struct {
	Raft *Raft
	PrevState
}

func NewRawNode(config *Config) (*RawNode, error) {
	raft := newRaft(config)

	return &RawNode{
		Raft: raft,
		PrevState: PrevState{
			PrevSoftState: &SoftState{
				LeaderID:  raft.leader_id,
				RaftState: raft.state,
			},
			PrevHardState: &raftpb.HardState{
				Term:        raft.hardState.Term,
				Vote:        raft.hardState.Vote,
				CommitIndex: raft.hardState.CommitIndex,
			},
		},
	}, nil
}

func (rn *RawNode) Tick() {
	rn.Raft.tick()
}

func (rn *RawNode) Campaign() {
	msg := &raftpb.RaftMessage{
		Type: raftpb.MessageType_HUP,
	}
	rn.Raft.step(msg)
}

func (rn *RawNode) Propose(data []byte) error {
	entry := &raftpb.Entry{Data: data} // 这里不设置 Index 和 Term，Raft 内部会在 append 时自动填充
	msg := &raftpb.RaftMessage{
		Type: raftpb.MessageType_PROPOSE,
		Body: &raftpb.RaftMessage_Propose{Propose: entry},
	}
	return rn.Raft.step(msg)
}

func (rn *RawNode) Step(m *raftpb.RaftMessage) error {
	if IsLocalMsg(m) {
		return ErrStepLocalMsg
	}
	if pr := rn.Raft.prs[m.From]; pr != nil || !IsLocalMsg(m) {
		return rn.Raft.step(m)
	}
	return ErrStepPeerNotFound
}

func (rn *RawNode) Ready() Ready {
	softState := &SoftState{
		LeaderID:  rn.Raft.leader_id,
		RaftState: rn.Raft.state,
	}
	if rn.PrevSoftState != nil && *softState == *rn.PrevSoftState {
		softState = nil
	}

	hardState := &raftpb.HardState{
		Term:        rn.Raft.hardState.Term,
		Vote:        rn.Raft.hardState.Vote,
		CommitIndex: rn.Raft.hardState.CommitIndex,
	}
	if rn.PrevHardState != nil && IsHSEqual(hardState, rn.PrevHardState) {
		hardState = nil
	}

	msg := rn.Raft.msgs
	if len(msg) <= 0 {
		msg = nil
	}

	rd := Ready{
		SoftState:        softState,
		HardState:        hardState,
		Entries:          rn.Raft.raftLog.unstableEntries(),
		Snapshot:         rn.Raft.raftLog.pendingSnapshot,
		CommittedEntries: rn.Raft.raftLog.nextEntries(rn.Raft.hardState.CommitIndex),
		Messages:         msg,
	}
	return rd
}

// HasReady 判断是否有待处理的工作或状态变化。
func (rn *RawNode) HasReady() bool {
	// 检查 SoftState 变化（Leader 切换）
	if rn.PrevSoftState == nil ||
		rn.Raft.leader_id != rn.PrevSoftState.LeaderID ||
		rn.Raft.state != rn.PrevSoftState.RaftState {
		return true
	}
	// 检查 HardState 变化
	state := &raftpb.HardState{
		Term:        rn.Raft.hardState.Term,
		Vote:        rn.Raft.hardState.Vote,
		CommitIndex: rn.Raft.hardState.CommitIndex,
	}
	if rn.PrevHardState == nil || !IsHSEqual(state, rn.PrevHardState) {
		return true
	}
	return len(rn.Raft.msgs) > 0 ||
		len(rn.Raft.raftLog.unstableEntries()) > 0 ||
		len(rn.Raft.raftLog.nextEntries(rn.Raft.hardState.CommitIndex)) > 0 ||
		rn.Raft.raftLog.pendingSnapshot != nil
}

func (rn *RawNode) Advance(rd *Ready) {
	if len(rd.CommittedEntries) > 0 {
		rn.Raft.raftLog.appliedIndex = rd.CommittedEntries[len(rd.CommittedEntries)-1].Index
	}
	if len(rd.Entries) > 0 {
		rn.Raft.raftLog.stabledIndex = rd.Entries[len(rd.Entries)-1].Index
	}
	if !IsEmptySnapshot(rd.Snapshot) {
		rn.Raft.raftLog.pendingSnapshot = nil
	}
	rn.Raft.clearMsgs()
	if rd.SoftState != nil {
		rn.PrevSoftState = SoftStateCopy(rd.SoftState)
	}
	if rd.HardState != nil {
		rn.PrevHardState = proto.Clone(rd.HardState).(*raftpb.HardState)
	}
	rn.Raft.raftLog.maybeCompact()
}

func (rn *RawNode) GetProgress() map[uint64]Progress {
	prs := make(map[uint64]Progress)
	if rn.Raft.state == stateLeader {
		for id, p := range rn.Raft.prs {
			prs[id] = *p
		}
	}
	return prs
}
