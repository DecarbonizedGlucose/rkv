package raft

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/DecarbonizedGlucose/rkv/api/proto/pkg/raftpb"
)

// ========================================
// Test helpers
// ========================================

func newTestConfig(id uint64, peers []uint64) *Config {
	return &Config{
		ID:               id,
		Peers:            peers,
		ElectionTimeout:  10,
		HeartbeatTimeout: 3,
		Storage:          NewMemoryStorage(),
	}
}

func newTestRaft(id uint64, peers []uint64) *Raft {
	r, err := newRaft(newTestConfig(id, peers))
	if err != nil {
		panic("newTestRaft: " + err.Error())
	}
	return r
}

// readMessages 读取并清空 Raft 内部消息队列
func readMessages(r *Raft) []*raftpb.RaftMessage {
	msgs := r.msgs
	r.msgs = nil
	return msgs
}

// relayAppendResponses 将 leaders 发给 follower 的 Append 转发，并把 follower 的响应回传
func relayAppendResponses(t *testing.T, leaders, follower *Raft) {
	t.Helper()
	for _, m := range readMessages(leaders) {
		if m.To == follower.id {
			follower.step(m)
			resps := readMessages(follower)
			for _, resp := range resps {
				if resp.Type == raftpb.MessageType_APPEND_RESP {
					require.NoError(t, leaders.step(resp))
				}
			}
		}
	}
}

// assertState 判断 Raft 节点是否为指定状态
func assertState(t *testing.T, r *Raft, expected stateType) {
	t.Helper()
	if r.state != expected {
		t.Errorf("expected state %s, got %s", expected.str(), r.state.str())
	}
}

// ========================================
// Leader Election
// ========================================

func TestSingleNodeBecomeLeader(t *testing.T) {
	r := newTestRaft(1, []uint64{1})

	r.step(&raftpb.RaftMessage{Type: raftpb.MessageType_HUP})
	assertState(t, r, stateLeader)
}

func TestLeaderElection3Nodes(t *testing.T) {
	n1 := newTestRaft(1, []uint64{1, 2, 3})
	n2 := newTestRaft(2, []uint64{1, 2, 3})
	n3 := newTestRaft(3, []uint64{1, 2, 3})

	// n1 发起选举
	n1.step(&raftpb.RaftMessage{Type: raftpb.MessageType_HUP})
	msgs := readMessages(n1)
	require.NotEmpty(t, msgs, "candidate should send vote requests")

	// n2 和 n3 投赞成
	for _, msg := range msgs {
		switch msg.To {
		case 2:
			n2.step(msg)
			n1.step(readMessages(n2)[0])
		case 3:
			n3.step(msg)
			n1.step(readMessages(n3)[0])
		}
	}
	assertState(t, n1, stateLeader)
	assert.Equal(t, uint64(1), n1.hardState.Term)
}

func TestLeaderElectionWithNopSteppers(t *testing.T) {
	n1 := newTestRaft(1, []uint64{1, 2, 3})
	n2 := newTestRaft(2, []uint64{1, 2, 3})

	n1.step(&raftpb.RaftMessage{Type: raftpb.MessageType_HUP})
	msgs := readMessages(n1)

	// n2 收到投票请求但因日志落后而拒绝
	// 先让 n2 日志领先，n1 日志落后 → upToDate=false → VoteGranted=false
	n2.raftLog.append(makeEntry(1, 2), makeEntry(2, 2))
	n1.raftLog.append(makeEntry(1, 1))

	n1.step(&raftpb.RaftMessage{Type: raftpb.MessageType_HUP})
	msgs = readMessages(n1)
	for _, msg := range msgs {
		if msg.To == 2 {
			n2.step(msg)
			resps := readMessages(n2)
			require.Len(t, resps, 1)
			assert.False(t, resps[0].Body.(*raftpb.RaftMessage_VoteResp).VoteResp.VoteGranted)
			// n3 不存在，不回复 → n1 只有自我一票，不足多数
		}
	}
	assertState(t, n1, stateCandidate)
}

func TestLeaderCycle(t *testing.T) {
	n1 := newTestRaft(1, []uint64{1, 2, 3})
	n2 := newTestRaft(2, []uint64{1, 2, 3})
	n3 := newTestRaft(3, []uint64{1, 2, 3})

	// === 阶段 1: n1 当选 Leader ===
	n1.step(&raftpb.RaftMessage{Type: raftpb.MessageType_HUP})

	// 消费 n1 发出的 vote 请求，拿到投票
	msgs := readMessages(n1)
	for i := 0; i < len(msgs); i++ {
		switch msgs[i].To {
		case 2:
			n2.step(msgs[i])
			n1.step(readMessages(n2)[0])
		case 3:
			n3.step(msgs[i])
			n1.step(readMessages(n3)[0])
		}
	}
	// consume becomeLeader heartbeat
	_ = readMessages(n1)
	assertState(t, n1, stateLeader)

	// === 阶段 2: n2 发起选举，n1 退位 ===
	n2.step(&raftpb.RaftMessage{Type: raftpb.MessageType_HUP})
	assertState(t, n2, stateCandidate)
	assert.Equal(t, n1.hardState.Term+1, n2.hardState.Term)

	// n2 发 vote 给 n1 和 n3
	msgs = readMessages(n2)
	for i := 0; i < len(msgs); i++ {
		switch msgs[i].To {
		case 1:
			n1.step(msgs[i]) // n1 收到高 term vote → 降级
			n2.step(readMessages(n1)[0])
		case 3:
			n3.step(msgs[i])
			n2.step(readMessages(n3)[0])
		}
	}
	// consume becomeLeader heartbeat
	_ = readMessages(n2)

	assertState(t, n2, stateLeader)
	assertState(t, n1, stateFollower)
	assert.Equal(t, n2.hardState.Term, n1.hardState.Term)
}

func TestFollowerStepdownOnHigherTerm(t *testing.T) {
	n1 := newTestRaft(1, []uint64{1, 2})
	n2 := newTestRaft(2, []uint64{1, 2})

	// n1 is Leader at term 1
	n1.step(&raftpb.RaftMessage{Type: raftpb.MessageType_HUP})
	for _, m := range readMessages(n1) {
		n2.step(m)
		n1.step(readMessages(n2)[0])
	}
	require.True(t, n1.state == stateLeader, "n1 should be Leader after election, got %s", n1.state.str())
	require.Equal(t, uint64(1), n1.hardState.Term)

	// n2 campaigns at term 2, sends vote to n1
	n2.step(&raftpb.RaftMessage{Type: raftpb.MessageType_HUP})
	msgs := readMessages(n2)
	for _, m := range msgs {
		if m.To == 1 {
			n1.step(m) // Leader sees higher-term vote → should step down
		}
	}

	assert.True(t, n1.state == stateFollower, "expected Follower, got %s", n1.state.str())
	assert.Equal(t, uint64(2), n1.hardState.Term)
}

func TestQuorumCheckIgnoresStaleRound(t *testing.T) {
	leader := newTestRaft(1, []uint64{1, 2, 3})
	follower := newTestRaft(2, []uint64{1, 2, 3})
	leader.becomeCandidate()
	leader.becomeLeader()
	_ = readMessages(leader) // 丢弃当选时复制 no-op 的普通 Append

	err := leader.checkQuorum(1)
	require.NoError(t, err)
	round1ID := leader.quorumRound
	round1 := readMessages(leader)
	var staleResponse *raftpb.RaftMessage
	for _, msg := range round1 {
		if msg.To == follower.id {
			require.NoError(t, follower.step(msg))
			staleResponse = readMessages(follower)[0]
			break
		}
	}
	require.NotNil(t, staleResponse)

	leader.abortQuorum() // 模拟上一轮超时
	leader.quorumConfirmed = nil
	err = leader.checkQuorum(2)
	require.NoError(t, err)
	require.NotEqual(t, round1ID, leader.quorumRound)
	round2 := readMessages(leader)
	require.NoError(t, leader.step(staleResponse))
	assert.Empty(t, leader.quorumConfirmed)

	var current *raftpb.RaftMessage
	for _, msg := range round2 {
		if msg.To == follower.id {
			require.NoError(t, follower.step(msg))
			current = readMessages(follower)[0]
			break
		}
	}
	require.NotNil(t, current)
	require.NoError(t, leader.step(current))
	require.Len(t, leader.quorumConfirmed, 1)
	assert.Equal(t, uint64(2), leader.quorumConfirmed[0].RequestID)
	assert.True(t, leader.raftLog.matchTerm(leader.hardState.CommitIndex, leader.hardState.Term))
}

func TestFollowerReadIndex(t *testing.T) {
	leader := newTestRaft(1, []uint64{1, 2, 3})
	follower := newTestRaft(2, []uint64{1, 2, 3})
	leader.becomeCandidate()
	leader.becomeLeader()
	relayAppendResponses(t, leader, follower) // 提交当前 term 的 no-op
	require.True(t, leader.raftLog.matchTerm(leader.hardState.CommitIndex, leader.hardState.Term))

	require.NoError(t, follower.requestReadIndex(100))
	request := readMessages(follower)[0]
	require.Equal(t, raftpb.MessageType_READ_INDEX_REQ, request.Type)
	require.NoError(t, leader.step(request))

	var appendToFollower *raftpb.RaftMessage
	for _, msg := range readMessages(leader) {
		if msg.Type == raftpb.MessageType_APPEND_REQ && msg.To == follower.id {
			appendToFollower = msg
		}
	}
	require.NotNil(t, appendToFollower)
	require.NoError(t, follower.step(appendToFollower))
	require.NoError(t, leader.step(readMessages(follower)[0]))

	var response *raftpb.RaftMessage
	for _, msg := range readMessages(leader) {
		if msg.Type == raftpb.MessageType_READ_INDEX_RESP {
			response = msg
		}
	}
	require.NotNil(t, response)
	assert.True(t, response.GetReadIndexResp().Success)
	assert.Equal(t, leader.hardState.CommitIndex, response.GetReadIndexResp().ReadIndex)

	require.NoError(t, follower.step(response))
	require.Len(t, follower.readStates, 1)
	assert.Equal(t, uint64(100), follower.readStates[0].RequestID)
	assert.Equal(t, leader.hardState.CommitIndex, follower.readStates[0].Index)
}

func TestReadIndexAndLeaderLeaseShareNextRound(t *testing.T) {
	leader := newTestRaft(1, []uint64{1, 2, 3})
	follower := newTestRaft(2, []uint64{1, 2, 3})
	leader.becomeCandidate()
	leader.becomeLeader()
	relayAppendResponses(t, leader, follower)

	require.NoError(t, follower.requestReadIndex(1))
	require.NoError(t, leader.step(readMessages(follower)[0]))
	firstRound := readMessages(leader)

	require.NoError(t, follower.requestReadIndex(2))
	require.NoError(t, leader.step(readMessages(follower)[0]))
	require.NoError(t, leader.checkQuorum(99)) // Leader 本地 Lease consumer
	assert.Len(t, leader.activeQuorum, 1)
	assert.Len(t, leader.queuedQuorum, 2)

	for _, msg := range firstRound {
		if msg.Type == raftpb.MessageType_APPEND_REQ && msg.To == follower.id {
			require.NoError(t, follower.step(msg))
			require.NoError(t, leader.step(readMessages(follower)[0]))
			break
		}
	}
	responses := readMessages(leader)
	require.Len(t, responses, 1)
	assert.Equal(t, uint64(1), responses[0].GetReadIndexResp().RequestId)

	// Ready.Advance 清除第一轮确认后才启动下一轮。
	leader.quorumConfirmed = nil
	leader.maybeStartQueuedQuorum()
	assert.NotZero(t, leader.quorumRound)
	assert.Len(t, leader.activeQuorum, 2)

	secondRound := readMessages(leader)
	for _, msg := range secondRound {
		if msg.Type == raftpb.MessageType_APPEND_REQ && msg.To == follower.id {
			require.NoError(t, follower.step(msg))
			require.NoError(t, leader.step(readMessages(follower)[0]))
			break
		}
	}
	require.Len(t, leader.quorumConfirmed, 1)
	assert.Equal(t, uint64(99), leader.quorumConfirmed[0].RequestID)
	responses = readMessages(leader)
	require.Len(t, responses, 1)
	assert.Equal(t, uint64(2), responses[0].GetReadIndexResp().RequestId)
}

func TestQuorumRoundTimeoutClearsConsumers(t *testing.T) {
	leader := newTestRaft(1, []uint64{1, 2, 3, 4, 5})
	leader.becomeCandidate()
	leader.becomeLeader()
	_ = readMessages(leader)

	for _, requestID := range []uint64{1, 2} {
		require.NoError(t, leader.step(&raftpb.RaftMessage{
			From: 2, To: 1, Term: leader.hardState.Term, Type: raftpb.MessageType_READ_INDEX_REQ,
			Body: &raftpb.RaftMessage_ReadIndexReq{ReadIndexReq: &raftpb.ReadIndexRequest{RequestId: requestID}},
		}))
	}
	require.NoError(t, leader.checkQuorum(99))
	_ = readMessages(leader)

	for range leader.electionTimeout {
		leader.tick()
	}
	assert.Zero(t, leader.quorumRound)
	assert.Empty(t, leader.activeQuorum)
	assert.Empty(t, leader.queuedQuorum)

	failures := 0
	for _, msg := range readMessages(leader) {
		if msg.Type == raftpb.MessageType_READ_INDEX_RESP && !msg.GetReadIndexResp().Success {
			failures++
		}
	}
	assert.Equal(t, 2, failures)
	require.Len(t, leader.quorumConfirmed, 1)
	assert.Equal(t, uint64(99), leader.quorumConfirmed[0].RequestID)
	assert.True(t, leader.quorumConfirmed[0].Rejected)

	// 超时后的 scheduler 必须能接受新一轮，不能永久卡在 in-progress。
	require.NoError(t, leader.checkQuorum(100))
	assert.NotZero(t, leader.quorumRound)
	assert.Len(t, leader.activeQuorum, 1)
}

func TestLeaderStepdownRejectsPendingQuorumConsumers(t *testing.T) {
	leader := newTestRaft(1, []uint64{1, 2, 3})
	leader.becomeCandidate()
	leader.becomeLeader()
	_ = readMessages(leader)

	for _, requestID := range []uint64{1, 2} {
		require.NoError(t, leader.step(&raftpb.RaftMessage{
			From: 2, To: 1, Term: leader.hardState.Term, Type: raftpb.MessageType_READ_INDEX_REQ,
			Body: &raftpb.RaftMessage_ReadIndexReq{ReadIndexReq: &raftpb.ReadIndexRequest{RequestId: requestID}},
		}))
	}
	require.NoError(t, leader.checkQuorum(99))
	_ = readMessages(leader)

	leader.becomeFollower(leader.hardState.Term+1, 0)
	assert.Zero(t, leader.quorumRound)
	assert.Empty(t, leader.activeQuorum)
	assert.Empty(t, leader.queuedQuorum)

	failures := 0
	for _, msg := range readMessages(leader) {
		if msg.Type == raftpb.MessageType_READ_INDEX_RESP && !msg.GetReadIndexResp().Success {
			failures++
		}
	}
	assert.Equal(t, 2, failures)
	require.Len(t, leader.quorumConfirmed, 1)
	assert.Equal(t, uint64(99), leader.quorumConfirmed[0].RequestID)
	assert.True(t, leader.quorumConfirmed[0].Rejected)
}

// ========================================
// Log Replication
// ========================================

func TestLogReplicationBasic(t *testing.T) {
	n1 := newTestRaft(1, []uint64{1, 2, 3})
	n2 := newTestRaft(2, []uint64{1, 2, 3})

	// n1 当选
	n1.step(&raftpb.RaftMessage{Type: raftpb.MessageType_HUP})
	msgs := readMessages(n1)
	for _, m := range msgs {
		if m.To == 2 {
			n2.step(m)
			n1.step(readMessages(n2)[0])
		}
	}
	require.True(t, n1.state == stateLeader, "n1 should be Leader")

	// Propose
	n1.step(&raftpb.RaftMessage{
		Type: raftpb.MessageType_PROPOSE,
		Body: &raftpb.RaftMessage_Propose{Propose: &raftpb.Entry{Data: []byte("hello")}},
	})
	relayAppendResponses(t, n1, n2)

	// n2 日志应与 n1 一致
	assert.Equal(t, n1.raftLog.lastLogIndex(), n2.raftLog.lastLogIndex())
	assert.True(t, n1.hardState.CommitIndex >= 1)
}

func TestLogReplicationConflict(t *testing.T) {
	n1 := newTestRaft(1, []uint64{1, 2, 3})
	n2 := newTestRaft(2, []uint64{1, 2, 3})

	// n1 当选，n2 确认
	n1.step(&raftpb.RaftMessage{Type: raftpb.MessageType_HUP})
	for _, m := range readMessages(n1) {
		if m.To == 2 {
			n2.step(m)
			n1.step(readMessages(n2)[0])
		}
	}
	require.True(t, n1.state == stateLeader, "n1 should be Leader")

	// 先 replica 两条正常 entry，n2 同步两份
	n1.step(&raftpb.RaftMessage{
		Type: raftpb.MessageType_PROPOSE,
		Body: &raftpb.RaftMessage_Propose{Propose: &raftpb.Entry{Data: []byte("1")}},
	})
	relayAppendResponses(t, n1, n2)
	n1.step(&raftpb.RaftMessage{
		Type: raftpb.MessageType_PROPOSE,
		Body: &raftpb.RaftMessage_Propose{Propose: &raftpb.Entry{Data: []byte("2")}},
	})
	relayAppendResponses(t, n1, n2)
	// index: 1=no-op(from becomeLeader), 2="1", 3="2"
	require.Equal(t, uint64(3), n1.raftLog.lastLogIndex())
	require.Equal(t, uint64(3), n2.raftLog.lastLogIndex())

	// n2 制造冲突：把 index=3 (entries[3]) 的 term 改成 9
	n2.raftLog.entries[3].Term = 9

	// propose 第三条 → leader PrevLogIndex=2, PrevLogTerm=1
	// n2 的 term(2)=9 ≠ 1 → 拒绝 → leader 回退 → 第二轮成功
	n1.step(&raftpb.RaftMessage{
		Type: raftpb.MessageType_PROPOSE,
		Body: &raftpb.RaftMessage_Propose{Propose: &raftpb.Entry{Data: []byte("3")}},
	})

	// 第一轮：n2 拒绝
	msgs := readMessages(n1)
	var rejected bool
	for _, m := range msgs {
		if m.To == 2 {
			n2.step(m)
			resps := readMessages(n2)
			for _, resp := range resps {
				if resp.Type == raftpb.MessageType_APPEND_RESP {
					rejected = !resp.Body.(*raftpb.RaftMessage_AppendResp).AppendResp.Success
					n1.step(resp)
				}
			}
		}
	}
	require.True(t, rejected, "n2 should reject due to conflicting term")

	// leader 收到拒绝后已调整 NextIndex，手动触发重试
	n1.broadcastHeartbeat()
	msgs = readMessages(n1)
	for _, m := range msgs {
		if m.To == 2 {
			n2.step(m)
			require.True(t, readMessages(n2)[0].Body.(*raftpb.RaftMessage_AppendResp).AppendResp.Success)
		}
	}

	assert.Equal(t, n1.raftLog.lastLogIndex(), n2.raftLog.lastLogIndex())
}

func TestCommitWithHeartbeat(t *testing.T) {
	n1 := newTestRaft(1, []uint64{1, 2, 3})
	n2 := newTestRaft(2, []uint64{1, 2, 3})

	// n1 当选
	n1.step(&raftpb.RaftMessage{Type: raftpb.MessageType_HUP})
	for _, m := range readMessages(n1) {
		n2.step(m)
		n1.step(readMessages(n2)[0])
	}

	// Propose 并让 n2 确认
	n1.step(&raftpb.RaftMessage{
		Type: raftpb.MessageType_PROPOSE,
		Body: &raftpb.RaftMessage_Propose{Propose: &raftpb.Entry{Data: []byte("y")}},
	})
	relayAppendResponses(t, n1, n2)

	// n1 commit 后，心跳应把 commit 带给 n2
	n1.step(&raftpb.RaftMessage{Type: raftpb.MessageType_HEARTBEAT})
	msgs := readMessages(n1)
	for _, m := range msgs {
		if m.To == 2 {
			n2.step(m)
			// index 1=no-op (from becomeLeader), index 2="y"; commit covers both
			assert.Equal(t, uint64(2), n2.hardState.CommitIndex)
		}
	}
}

// ========================================
// Vote
// ========================================

func TestVoteRequestLogUpToDate(t *testing.T) {
	n1 := newTestRaft(1, []uint64{1, 2})
	n2 := newTestRaft(2, []uint64{1, 2})

	// n1 比 n2 日志新
	n1.raftLog.append(makeEntry(1, 1), makeEntry(2, 1))
	n2.raftLog.append(makeEntry(1, 1))

	n1.step(&raftpb.RaftMessage{Type: raftpb.MessageType_HUP})
	msgs := readMessages(n1)

	for _, m := range msgs {
		if m.To == 2 {
			n2.step(m)
			resps := readMessages(n2)
			require.Len(t, resps, 1)
			assert.True(t, resps[0].Body.(*raftpb.RaftMessage_VoteResp).VoteResp.VoteGranted)
		}
	}
}

func TestVoteRejectStaleLog(t *testing.T) {
	n1 := newTestRaft(1, []uint64{1, 2})
	n2 := newTestRaft(2, []uint64{1, 2})

	// n2 日志更新
	n2.raftLog.append(makeEntry(1, 2), makeEntry(2, 2))
	n1.raftLog.append(makeEntry(1, 1))

	n1.step(&raftpb.RaftMessage{Type: raftpb.MessageType_HUP})
	msgs := readMessages(n1)

	for _, m := range msgs {
		if m.To == 2 {
			n2.step(m)
			resps := readMessages(n2)
			require.Len(t, resps, 1)
			assert.False(t, resps[0].Body.(*raftpb.RaftMessage_VoteResp).VoteResp.VoteGranted)
		}
	}
}

func TestVoteOncePerTerm(t *testing.T) {
	n1 := newTestRaft(1, []uint64{1, 2, 3})
	n2 := newTestRaft(2, []uint64{1, 2, 3})

	// n1 请求投票
	n1.step(&raftpb.RaftMessage{Type: raftpb.MessageType_HUP})
	msgs := readMessages(n1)

	for _, m := range msgs {
		if m.To == 2 {
			n2.step(m)
			resps := readMessages(n2)
			require.Len(t, resps, 1)
			assert.True(t, resps[0].Body.(*raftpb.RaftMessage_VoteResp).VoteResp.VoteGranted)
			assert.Equal(t, n1.hardState.Term, n2.hardState.Vote)
		}
	}

	// n3 也请求投票（同一 term），n2 应该拒绝（已投给 n1）
	n3 := newTestRaft(3, []uint64{1, 2, 3})

	// 手工设置 n3 为与 n1 同一 term 的 candidate
	n3.hardState.Term = n1.hardState.Term
	n3.state = stateCandidate
	n3.hardState.Vote = 3
	n3.votes = map[uint64]bool{3: true}
	req := &raftpb.RequestVoteRequest{
		Term:         n3.hardState.Term,
		CandidateId:  3,
		LastLogIndex: n3.raftLog.lastLogIndex(),
		LastLogTerm:  n3.raftLog.lastLogTerm(),
	}
	n3.sendRequestVote(2, req)
	msgs = readMessages(n3)

	for _, m := range msgs {
		if m.To == 2 {
			n2.step(m)
			resps := readMessages(n2)
			require.Len(t, resps, 1)
			assert.False(t, resps[0].Body.(*raftpb.RaftMessage_VoteResp).VoteResp.VoteGranted)
		}
	}
}

// ========================================
// Snapshot
// ========================================

func TestSendSnapshotToLaggingFollower(t *testing.T) {
	t.Skip("requires Log integration with Compact, skipped for now")

	n1 := newTestRaft(1, []uint64{1})

	// n1 当选 Leader（单节点立即当选）
	n1.step(&raftpb.RaftMessage{Type: raftpb.MessageType_HUP})
	require.True(t, n1.state == stateLeader, "expected Leader, got %s", n1.state.str())

	// 写入日志并创建快照
	n1.raftLog.append(makeEntry(1, 1), makeEntry(2, 1))
	n1.raftLog.stabledIndex = 2

	_, err := n1.raftLog.storage.(*MemoryStorage).CreateSnapshot(1,
		&raftpb.ConfState{Nodes: []uint64{1, 2}}, []byte("snapdata"))
	require.NoError(t, err)
	n1.raftLog.storage.(*MemoryStorage).Compact(1)

	// 直接调用 sendInstallSnapshot 验证快照消息生成
	n1.sendInstallSnapshot(2)
	msgs := readMessages(n1)

	var snapMsg *raftpb.RaftMessage
	for _, m := range msgs {
		if m.Type == raftpb.MessageType_INSTALL_SNAPSHOT_REQ {
			snapMsg = m
		}
	}
	require.NotNil(t, snapMsg, "should send snapshot when storage has one")
}

func TestFollowerInstallSnapshotRejectOld(t *testing.T) {
	n2 := newTestRaft(2, []uint64{1, 2})

	// n2 已有更高的 lastIncluded
	n2.raftLog.setLastIncluded(5)

	msg := &raftpb.RaftMessage{
		From: 1,
		To:   2,
		Term: 1,
		Type: raftpb.MessageType_INSTALL_SNAPSHOT_REQ,
		Body: &raftpb.RaftMessage_SnapReq{SnapReq: &raftpb.InstallSnapshotRequest{
			Term:              1,
			LeaderId:          1,
			LastIncludedIndex: 3, // 比 n2 的 lastIncluded(5) 旧
			LastIncludedTerm:  1,
			Data:              []byte("irrelevant"),
		}},
	}

	n2.step(msg)
	resps := readMessages(n2)
	require.Len(t, resps, 1)
	assert.False(t, resps[0].Body.(*raftpb.RaftMessage_SnapResp).SnapResp.Success)
}

func TestFollowerInstallSnapshotRejectsMissingMetadata(t *testing.T) {
	n2 := newTestRaft(2, []uint64{1, 2})

	msg := &raftpb.RaftMessage{
		From: 1,
		To:   2,
		Term: 1,
		Type: raftpb.MessageType_INSTALL_SNAPSHOT_REQ,
		Body: &raftpb.RaftMessage_SnapReq{SnapReq: &raftpb.InstallSnapshotRequest{
			Term:              1,
			LeaderId:          1,
			LastIncludedIndex: 1,
			LastIncludedTerm:  1,
			Data:              []byte{},
		}},
	}

	require.NotPanics(t, func() { n2.step(msg) })
	resps := readMessages(n2)
	require.Len(t, resps, 1)
	assert.False(t, resps[0].Body.(*raftpb.RaftMessage_SnapResp).SnapResp.Success)
}

// TestSnapshotTransfer 验证快照传输全流程：
// Leader 日志压缩后，落后的 Follower 收到 INSTALL_SNAPSHOT_REQ，
// 正确设置 pendingSnapshot，并返回 Success 响应，Leader 更新 Progress。
func TestSnapshotTransfer(t *testing.T) {
	snapData := []byte("kv-snapshot")
	cs := &raftpb.ConfState{Nodes: []uint64{1, 2}}

	// === 阶段 1：单节点 Leader 提案 5 条 entry，CommitIndex 推进到 6 ===
	n1 := newTestRaft(1, []uint64{1})
	n1.step(&raftpb.RaftMessage{Type: raftpb.MessageType_HUP})
	assertState(t, n1, stateLeader)
	// no-op entry 在 index=1，CommitIndex=1
	for i := 2; i <= 6; i++ {
		n1.step(&raftpb.RaftMessage{
			Type: raftpb.MessageType_PROPOSE,
			Body: &raftpb.RaftMessage_Propose{Propose: &raftpb.Entry{Data: []byte{byte(i)}}},
		})
	}
	require.Equal(t, uint64(6), n1.hardState.CommitIndex)

	// === 阶段 2：将内存日志同步到 MemoryStorage，创建快照并压缩 ===
	ms := n1.raftLog.storage.(*MemoryStorage)
	// entries[0] 是哨兵，entries[1:] 是真实 entry（index 1..6）
	require.NoError(t, ms.Append(n1.raftLog.entries[1:]))
	_, err := ms.CreateSnapshot(6, cs, snapData)
	require.NoError(t, err)
	require.NoError(t, ms.Compact(6))
	n1.raftLog.maybeCompact() // 更新内存哨兵 lastIncluded=6
	require.Equal(t, uint64(6), n1.raftLog.getLastIncluded())

	// === 阶段 3：将 n2 注册进 n1 的 prs，NextIndex=1 < lastIncluded=6 ===
	n1.prs[2] = &Progress{NextIndex: 1, MatchIndex: 0}
	n1.sendAppend(2)
	msgs := readMessages(n1)
	require.Len(t, msgs, 1)
	snapMsg := msgs[0]
	require.Equal(t, raftpb.MessageType_INSTALL_SNAPSHOT_REQ, snapMsg.Type)

	// === 阶段 4：n2 接收快照，pendingSnapshot 应被设置 ===
	n2 := newTestRaft(2, []uint64{1, 2})
	require.NoError(t, n2.step(snapMsg))

	require.NotNil(t, n2.raftLog.pendingSnapshot, "pendingSnapshot must be set after receiving snapshot")
	assert.Equal(t, uint64(6), n2.raftLog.pendingSnapshot.Metadata.LastIncludedIndex)
	assert.Equal(t, snapData, n2.raftLog.pendingSnapshot.Data)
	assert.Equal(t, uint64(6), n2.hardState.CommitIndex)
	assert.Equal(t, uint64(6), n2.raftLog.getLastIncluded())

	// === 阶段 5：n2 返回 Success 响应，n1 更新 n2 的 Progress ===
	resps := readMessages(n2)
	require.Len(t, resps, 1)
	snapResp := resps[0]
	assert.Equal(t, raftpb.MessageType_INSTALL_SNAPSHOT_RESP, snapResp.Type)
	assert.True(t, snapResp.Body.(*raftpb.RaftMessage_SnapResp).SnapResp.Success)

	require.NoError(t, n1.step(snapResp))
	assert.Equal(t, uint64(6), n1.prs[2].MatchIndex)
	assert.Equal(t, uint64(7), n1.prs[2].NextIndex)
}
