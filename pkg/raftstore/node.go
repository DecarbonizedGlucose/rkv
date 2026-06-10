package raftstore

import (
	"errors"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/DecarbonizedGlucose/rkv/api/proto/pkg/raftpb"
	"github.com/DecarbonizedGlucose/rkv/pkg/raft"
	"github.com/DecarbonizedGlucose/rkv/pkg/raft_transport"
)

var (
	ErrStopped = errors.New("raftstore: node has been stopped")
)

const (
	defaultTickInterval  = 100 * time.Millisecond
	defaultSnapshotCount = 10000
)

// StateMachine 是 Raft 共识层驱动的状态机。
// Apply 按序处理已提交的日志条目，实现必须容忍重复应用。
type StateMachine interface {
	Apply(entries []*raftpb.Entry) (acked int, err error)
	SnapshotData() ([]byte, error)
	ApplySnapshot(snap *raftpb.Snapshot) error
}

// 在raft.RaftStorage的基础上追加 Ready 循环所需的写方法
type Storage interface {
	raft.RaftStorage
	SaveHardState(hs *raftpb.HardState) error
	Append(entries []*raftpb.Entry) error
	ApplySnapshot(snap *raftpb.Snapshot) error
	CreateSnapshot(idx uint64, cs *raftpb.ConfState, data []byte) error
	Compact(idx uint64) error
}

// proposal 将待提案数据与结果通道绑定，供 run goroutine 消费。
type proposal struct {
	data   []byte
	respCh chan error
}

// Node 是单个 Raft 参与者的集成中枢，连接 RawNode、持久化存储、
// 网络传输和状态机应用。
type Node struct {
	rn        *raft.RawNode
	storage   Storage
	transport raft_transport.Transport
	sm        StateMachine

	propCh chan *proposal

	// 从 Ready.SoftState 中提取，通过 LeaderID() 对外暴露。
	leaderID atomic.Uint64

	snapshotCount uint64
	tickInterval  time.Duration

	ticker *time.Ticker
	stopCh chan struct{}
	doneCh chan struct{}
}

// Config 包含创建 Node 所需的全部参数。
type Config struct {
	RaftConfig   *raft.Config
	Storage      Storage
	Transport    raft_transport.Transport
	StateMachine StateMachine

	// TickInterval 是 raft tick 周期，默认 100ms。
	TickInterval time.Duration

	// SnapshotCount 是两次快照之间允许的最大已应用日志条数，默认 10000。
	SnapshotCount uint64
}

// 创建并启动一个 Raft 节点。
func NewNode(cfg *Config) (*Node, error) {
	// 首次启动，若ConfState中没有成员信息，则使用配置中的 Peers 列表初始化。
	_, cs, err := cfg.Storage.InitialState()
	if err != nil {
		return nil, fmt.Errorf("read initial state: %w", err)
	}
	if len(cs.Nodes) == 0 && len(cfg.RaftConfig.Peers) > 0 {
		cs.Nodes = make([]uint64, len(cfg.RaftConfig.Peers))
		copy(cs.Nodes, cfg.RaftConfig.Peers)
		// 用初始 ConfState 创建一个索引为 0 的快照
		if err := cfg.Storage.CreateSnapshot(0, cs, nil); err != nil {
			return nil, fmt.Errorf("bootstrap ConfState: %w", err)
		}
	}

	rn, err := raft.NewRawNode(cfg.RaftConfig)
	if err != nil {
		return nil, err
	}
	if err := cfg.Transport.Start(); err != nil {
		return nil, err
	}
	snapCount := cfg.SnapshotCount
	if snapCount == 0 {
		snapCount = defaultSnapshotCount
	}
	tickInterval := cfg.TickInterval
	if tickInterval == 0 {
		tickInterval = defaultTickInterval
	}

	n := &Node{
		rn:            rn,
		storage:       cfg.Storage,
		transport:     cfg.Transport,
		sm:            cfg.StateMachine,
		propCh:        make(chan *proposal, 256),
		snapshotCount: snapCount,
		tickInterval:  tickInterval,
		ticker:        time.NewTicker(tickInterval),
		stopCh:        make(chan struct{}),
		doneCh:        make(chan struct{}),
	}

	go n.run()
	return n, nil
}

// 将数据提交到 Raft 日志中，阻塞直到日志追加完成（尚未 commit）。
// 若当前节点不是 Leader 则返回 ErrNotLeader。
func (n *Node) Propose(data []byte) error {
	p := &proposal{
		data:   data,
		respCh: make(chan error, 1),
	}

	select {
	case n.propCh <- p:
	case <-n.stopCh:
		return ErrStopped
	}

	select {
	case err := <-p.respCh:
		return err
	case <-n.stopCh:
		return ErrStopped
	}
}

func (n *Node) LeaderID() uint64 {
	return n.leaderID.Load()
}

// 关闭节点，丢弃所有未完成的提案。
func (n *Node) Stop() {
	close(n.stopCh)
	<-n.doneCh
}

// 主循环，串行处理 tick、网络消息、提案和 Ready。
func (n *Node) run() {
	defer close(n.doneCh)
	defer n.ticker.Stop()
	defer n.transport.Stop()

	for {
		select {
		case <-n.ticker.C:
			n.rn.Tick()
		case msg := <-n.transport.Recv():
			//n.rn.Step(msg)
			if err := n.rn.Step(msg); err != nil {
				if !errors.Is(err, raft.ErrStepPeerNotFound) {
					log.Printf("raftstore: step message from node %d: %v\n", msg.From, err)
				}
			}
		case p := <-n.propCh:
			p.respCh <- n.rn.Propose(p.data)
		case <-n.stopCh:
			return
		}

		if !n.rn.HasReady() {
			continue
		}
		n.processReady()
	}
}

// processReady 消费单轮 Ready，按规范顺序处理。
// 顺序不可任意调整：快照必须先于新日志应用，HardState 必须先于 entries 持久化。
func (n *Node) processReady() {
	rd := n.rn.Ready()

	// 持久化
	n.applySnapshot(&rd)
	n.persistHardState(&rd)
	n.appendEntries(&rd)

	// 发送消息
	n.sendMessages(rd.Messages)

	// 应用已提交的日志条目到状态机
	n.applyCommittedEntries(&rd)

	// 可能触发创建新快照并压缩旧日志
	n.maybeSnapshot(&rd)
	// 更新 Leader ID 缓存
	n.updateLeader(&rd)

	n.rn.Advance(&rd)
}

// 将快照写入存储并恢复状态机，Follower 侧收到快照时触发。
func (n *Node) applySnapshot(rd *raft.Ready) {
	if rd.Snapshot == nil {
		return
	}
	if err := n.storage.ApplySnapshot(rd.Snapshot); err != nil {
		panic("raftstore: apply snapshot to storage: " + err.Error())
	}
	if err := n.sm.ApplySnapshot(rd.Snapshot); err != nil {
		panic("raftstore: apply snapshot to state machine: " + err.Error())
	}
}

// 持久化 term、vote、commit 等状态。
func (n *Node) persistHardState(rd *raft.Ready) {
	if rd.HardState == nil {
		return
	}
	if err := n.storage.SaveHardState(rd.HardState); err != nil {
		panic("raftstore: persist HardState: " + err.Error())
	}
}

// 将不稳定日志条目写入持久化存储。
func (n *Node) appendEntries(rd *raft.Ready) {
	if len(rd.Entries) == 0 {
		return
	}
	if err := n.storage.Append(rd.Entries); err != nil {
		panic("raftstore: append entries: " + err.Error())
	}
}

// 将 Raft 消息通过 Transport 发出。
// 发送失败可容忍。
func (n *Node) sendMessages(msgs []*raftpb.RaftMessage) {
	for _, msg := range msgs {
		if err := n.transport.Send(msg); err != nil {
			// 消息发送失败，暂时考虑记录。之后可改进为重试机制。
			log.Printf("raftstore: send message to node %d: %v\n", msg.To, err)
		}
	}
}

// 将已提交的日志条目应用到状态机。
// StateMachine.Apply 须容忍重复调用（同一条 entry 可能被 apply 多次）。
func (n *Node) applyCommittedEntries(rd *raft.Ready) {
	if len(rd.CommittedEntries) == 0 {
		return
	}
	if _, err := n.sm.Apply(rd.CommittedEntries); err != nil {
		panic("raftstore: apply to state machine: " + err.Error())
	}
}

// 在日志量超过阈值时创建快照并压缩旧日志。
func (n *Node) maybeSnapshot(rd *raft.Ready) {
	if len(rd.CommittedEntries) == 0 {
		return
	}

	lastIndex := rd.CommittedEntries[len(rd.CommittedEntries)-1].Index

	firstIndex, err := n.storage.FirstIndex()
	if err != nil {
		return
	}
	if lastIndex-firstIndex < n.snapshotCount {
		return
	}
	data, err := n.sm.SnapshotData()
	if err != nil {
		return
	}
	_, cs, err := n.storage.InitialState()
	if err != nil {
		return
	}
	if err := n.storage.CreateSnapshot(lastIndex, cs, data); err != nil {
		return
	}
	if err := n.storage.Compact(lastIndex); err != nil {
		return
	}
}

func (n *Node) updateLeader(rd *raft.Ready) {
	if rd.SoftState != nil {
		n.leaderID.Store(rd.SoftState.LeaderID)
	}
}
