package raftstore

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DecarbonizedGlucose/rkv/api/proto/pkg/raftpb"
	"github.com/DecarbonizedGlucose/rkv/pkg/raft"
	"github.com/DecarbonizedGlucose/rkv/pkg/raft_transport"
)

const (
	defaultTickInterval  = 100 * time.Millisecond
	defaultSnapshotCount = 10000
)

type ApplyResult struct {
	ProposalID uint64
	Data       []byte
}

// StateMachine 是 Raft 共识层驱动的状态机。
// Apply 按序处理已提交的日志条目，实现必须容忍重复应用。
type StateMachine interface {
	Apply(entries []*raftpb.Entry) (results []ApplyResult, err error)
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

	Close() error
}

// proposal 将待提案数据与结果通道绑定，供 run goroutine 消费。
type proposal struct {
	data       []byte
	proposalID uint64
	startErrCh chan error
	resultCh   chan []byte
}

type readRequest struct {
	resultCh chan readPermitResult
}

type readPermitResult struct {
	permit ReadPermit
	err    error
}

type readIndexRequest struct {
	resultCh chan readIndexResult
}

type readIndexResult struct {
	index uint64
	err   error
}

type pendingReadIndex struct {
	request  *readIndexRequest
	index    uint64
	deadline time.Time
}

// ReadPermit 证明节点在 ValidUntil 前以 Term 身份获得过多数派确认。
type ReadPermit struct {
	Term       uint64
	ValidUntil time.Time
}

// Node 是单个 Raft 参与者的集成中枢，连接 RawNode、持久化存储、
// 网络传输和状态机应用。
type Node struct {
	rn        *raft.RawNode
	storage   Storage
	transport raft_transport.Transport
	sm        StateMachine

	propCh           chan *proposal
	pending          map[uint64]*proposal // proposalID -> proposal，记录已提交但未完成的提案
	readCh           chan *readRequest
	readWaiters      []*readRequest
	readRound        uint64
	confirmRequestID uint64
	confirmTerm      uint64
	confirmUntil     time.Time
	leaseTerm        uint64
	leaseUntil       time.Time
	leaseDuration    time.Duration
	readIndexCh      chan *readIndexRequest
	pendingReadIndex map[uint64]*pendingReadIndex
	readIndexTimeout time.Duration

	// 从 Ready.SoftState 中提取，通过 LeaderID() 对外暴露。
	leaderID atomic.Uint64
	myID     uint64 // 本节点 ID，仅引用

	raftTerm atomic.Uint64

	snapshotCount uint64
	tickInterval  time.Duration

	ticker   *time.Ticker
	stopOnce sync.Once
	stopCh   chan struct{}
	doneCh   chan struct{}

	// fatalCh 在 run goroutine 遇到不可恢复错误（持久化失败、apply 损坏）时关闭，
	// fatalErr 记录原因。Server 通过 Done()/Err() 感知并走有序释放路径，
	// 而非在底层 os.Exit 跳过资源清理。
	fatalCh  chan struct{}
	fatalErr error
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
		rn:               rn,
		storage:          cfg.Storage,
		transport:        cfg.Transport,
		sm:               cfg.StateMachine,
		propCh:           make(chan *proposal, 256),
		pending:          make(map[uint64]*proposal),
		readCh:           make(chan *readRequest, 256),
		readRound:        initialReadRound(),
		myID:             cfg.RaftConfig.ID,
		snapshotCount:    snapCount,
		tickInterval:     tickInterval,
		leaseDuration:    time.Duration(cfg.RaftConfig.ElectionTimeout) * tickInterval / 2,
		readIndexCh:      make(chan *readIndexRequest, 256),
		pendingReadIndex: make(map[uint64]*pendingReadIndex),
		readIndexTimeout: time.Duration(cfg.RaftConfig.ElectionTimeout) * tickInterval,
		ticker:           time.NewTicker(tickInterval),
		stopCh:           make(chan struct{}),
		doneCh:           make(chan struct{}),
		fatalCh:          make(chan struct{}),
	}
	n.raftTerm.Store(rn.Term())

	go n.run()
	return n, nil
}

func initialReadRound() uint64 {
	var b [8]byte
	if _, err := rand.Read(b[:]); err == nil {
		if round := binary.LittleEndian.Uint64(b[:]); round != 0 {
			return round
		}
	}
	return uint64(time.Now().UnixNano())
}

// 将数据提交到 Raft 日志中，直到日志被应用返回。
// 若当前节点不是 Leader 则返回 ErrNotLeader。
//
// 返回的 []byte 是 proposalResult marshalled 后的结果。
func (n *Node) Propose(data []byte, proposalID uint64) ([]byte, error) {
	p := &proposal{
		data:       data,
		proposalID: proposalID,
		startErrCh: make(chan error, 1),
		resultCh:   make(chan []byte, 1),
	}

	select {
	case n.propCh <- p:
	case <-n.stopCh:
		return nil, ErrStopped
	}

	select {
	case err := <-p.startErrCh:
		if err != nil {
			return nil, err
		}
	case <-n.stopCh:
		return nil, ErrStopped
	}

	select {
	case result := <-p.resultCh:
		return result, nil
	case <-n.stopCh:
		return nil, ErrStopped
	}
}

func (n *Node) IsLeader() bool {
	return n.LeaderID() == n.myID
}

func (n *Node) LeaderID() uint64 {
	return n.leaderID.Load()
}

func (n *Node) Term() uint64 {
	return n.raftTerm.Load()
}

// AcquireReadPermit 在当前 Leader 的 lease 内返回读许可；lease 失效时会先发起一次多数派确认。
func (n *Node) AcquireReadPermit(ctx context.Context) (ReadPermit, error) {
	r := &readRequest{resultCh: make(chan readPermitResult, 1)}
	select {
	case n.readCh <- r:
	case <-ctx.Done():
		return ReadPermit{}, ctx.Err()
	case <-n.stopCh:
		return ReadPermit{}, ErrStopped
	}
	select {
	case result := <-r.resultCh:
		return result.permit, result.err
	case <-ctx.Done():
		return ReadPermit{}, ctx.Err()
	case <-n.stopCh:
		return ReadPermit{}, ErrStopped
	}
}

// ValidateReadPermit 用于在固定存储 revision 后确认许可没有过期或跨 term。
func (n *Node) ValidateReadPermit(p ReadPermit) bool {
	return p.Term != 0 && n.IsLeader() && n.Term() == p.Term && time.Now().Before(p.ValidUntil)
}

// ReadIndex 向当前 Leader 请求安全的 commit index，并等待本地状态机应用到该位置。
func (n *Node) ReadIndex(ctx context.Context) (uint64, error) {
	r := &readIndexRequest{resultCh: make(chan readIndexResult, 1)}
	select {
	case n.readIndexCh <- r:
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-n.stopCh:
		return 0, ErrStopped
	}
	select {
	case result := <-r.resultCh:
		return result.index, result.err
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-n.stopCh:
		return 0, ErrStopped
	}
}

// 关闭节点，丢弃所有未完成的提案。可重复调用。
func (n *Node) Stop() {
	n.triggerStop()
	<-n.doneCh
}

// triggerStop 关闭 stopCh，幂等。Stop 和 markFatal
// 都通过它发起停机，由 sync.Once 保证 stopCh 只关一次。
func (n *Node) triggerStop() {
	n.stopOnce.Do(func() {
		close(n.stopCh)
	})
}

// Done 在节点因不可恢复错误自行停机时关闭，供 Server 感知后释放资源。
// 正常 Stop() 不会关闭它。
func (n *Node) Done() <-chan struct{} {
	return n.fatalCh
}

// Err 返回导致节点致命停机的错误。仅在 Done() 关闭后读取才有意义。
func (n *Node) Err() error {
	return n.fatalErr
}

// markFatal 仅由 run goroutine 调用：记录致命错误、关闭 fatalCh 通知 Server，
// 并发起停机。fatalErr 必须在关闭 fatalCh 前写入，保证 Server 观察到关闭后能读到。
func (n *Node) markFatal(err error) {
	n.fatalErr = err
	close(n.fatalCh)
	n.triggerStop()
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
			err := n.rn.Propose(p.data)
			p.startErrCh <- err
			if err == nil {
				n.pending[p.proposalID] = p
			}
		case r := <-n.readCh:
			n.handleReadRequest(r)
		case r := <-n.readIndexCh:
			n.handleReadIndexRequest(r)
		case <-n.stopCh:
			return
		}

		n.expireQuorumCheck()
		n.expireReadIndexes()
		for n.rn.HasReady() {
			if err := n.processReady(); err != nil {
				// 持久化失败或 apply 损坏：不可恢复。记录后发起有序停机，
				// 由 Server 经 Done() 感知并释放资源，绝不在此 os.Exit。
				log.Printf("raftstore: fatal error in ready loop, shutting down: %v\n", err)
				n.markFatal(err)
				return
			}
		}
	}
}

// processReady 消费单轮 Ready，按规范顺序处理。
// 顺序不可任意调整：快照必须先于新日志应用，HardState 必须先于 entries 持久化。
//
// 返回非 nil error 表示发生不可恢复故障（持久化失败 / apply 损坏）：
// 此时不发送消息、不 apply、不 Advance，由调用方走有序停机。
// 一旦中途失败即返回，避免把"未落盘"的状态当成"已落盘"推进，导致数据丢失。
func (n *Node) processReady() error {
	rd := n.rn.Ready()

	// 持久化：任一失败都属致命，立即返回，绝不继续推进。
	if err := n.applySnapshot(&rd); err != nil {
		return fmt.Errorf("apply snapshot: %w", err)
	}
	if err := n.persistHardState(&rd); err != nil {
		return fmt.Errorf("persist hard state: %w", err)
	}
	if err := n.appendEntries(&rd); err != nil {
		return fmt.Errorf("append entries: %w", err)
	}

	// 发送消息（失败可容忍，仅记录）。必须在持久化成功之后。
	n.sendMessages(rd.Messages)

	// 应用已提交的日志条目到状态机。apply 失败一律视为不可恢复：
	// 不推进 appliedIndex 会让 HasReady() 持续为真，故此处
	// 直接上报致命错误。
	results, err := n.applyCommittedEntries(&rd)
	if err != nil {
		return fmt.Errorf("apply committed entries: %w", err)
	}

	// 可能触发创建新快照并压缩旧日志
	n.maybeSnapshot(&rd)
	// 更新 Leader ID 缓存
	n.updateLeader(&rd)

	n.rn.Advance(&rd)
	n.finishQuorumCheck(&rd)
	n.handleReadStates(rd.ReadStates)
	n.releaseAppliedReadIndexes()

	for _, r := range results {
		if p, ok := n.pending[r.ProposalID]; ok {
			delete(n.pending, r.ProposalID)
			p.resultCh <- r.Data
		}
	}
	return nil
}

func (n *Node) handleReadRequest(r *readRequest) {
	now := time.Now()
	term := n.rn.Term()
	if !n.rn.IsLeader() {
		r.resultCh <- readPermitResult{err: ErrNotLeader}
		return
	}
	if n.leaseTerm == term && now.Before(n.leaseUntil) {
		r.resultCh <- readPermitResult{permit: ReadPermit{Term: term, ValidUntil: n.leaseUntil}}
		return
	}
	n.readWaiters = append(n.readWaiters, r)
	if n.confirmRequestID != 0 {
		return
	}
	n.startLeaseQuorum(now, term)
}

func (n *Node) startLeaseQuorum(now time.Time, term uint64) {
	n.readRound++
	if n.readRound == 0 {
		n.readRound++
	}
	n.confirmRequestID = n.readRound
	n.confirmTerm = term
	n.confirmUntil = now.Add(n.leaseDuration)
	if err := n.rn.CheckQuorum(n.confirmRequestID); err != nil {
		n.failReadWaiters(ErrNotLeader)
	}
}

func (n *Node) finishQuorumCheck(rd *raft.Ready) {
	if !n.rn.IsLeader() || (n.confirmRequestID != 0 && n.rn.Term() != n.confirmTerm) {
		n.leaseTerm = 0
		n.leaseUntil = time.Time{}
		n.failReadWaiters(ErrNotLeader)
		return
	}
	for _, confirmed := range rd.QuorumConfirmed {
		if confirmed.RequestID != n.confirmRequestID {
			continue
		}
		if confirmed.Rejected || !time.Now().Before(n.confirmUntil) {
			n.failReadWaiters(ErrQuorumTimeout)
			return
		}
		n.leaseTerm = confirmed.Term
		n.leaseUntil = n.confirmUntil
		permit := ReadPermit{Term: n.leaseTerm, ValidUntil: n.leaseUntil}
		for _, r := range n.readWaiters {
			r.resultCh <- readPermitResult{permit: permit}
		}
		n.readWaiters = nil
		n.clearQuorumCheck()
		return
	}
}

func (n *Node) expireQuorumCheck() {
	if len(n.readWaiters) > 0 && !n.confirmUntil.IsZero() && !time.Now().Before(n.confirmUntil) {
		n.failReadWaiters(ErrQuorumTimeout)
	}
}

func (n *Node) failReadWaiters(err error) {
	for _, r := range n.readWaiters {
		r.resultCh <- readPermitResult{err: err}
	}
	n.readWaiters = nil
	n.clearQuorumCheck()
}

func (n *Node) clearQuorumCheck() {
	n.confirmRequestID = 0
	n.confirmTerm = 0
	n.confirmUntil = time.Time{}
}

func (n *Node) handleReadIndexRequest(r *readIndexRequest) {
	n.readRound++
	if n.readRound == 0 {
		n.readRound++
	}
	requestID := n.readRound
	if err := n.rn.ReadIndex(requestID); err != nil {
		r.resultCh <- readIndexResult{err: err}
		return
	}
	n.pendingReadIndex[requestID] = &pendingReadIndex{
		request:  r,
		deadline: time.Now().Add(n.readIndexTimeout),
	}
}

func (n *Node) handleReadStates(states []raft.ReadState) {
	for _, state := range states {
		pending := n.pendingReadIndex[state.RequestID]
		if pending == nil {
			continue
		}
		if state.Rejected {
			delete(n.pendingReadIndex, state.RequestID)
			pending.request.resultCh <- readIndexResult{err: ErrNotLeader}
			continue
		}
		pending.index = state.Index
	}
}

func (n *Node) releaseAppliedReadIndexes() {
	applied := n.rn.AppliedIndex()
	for requestID, pending := range n.pendingReadIndex {
		if pending.index == 0 || pending.index > applied {
			continue
		}
		delete(n.pendingReadIndex, requestID)
		pending.request.resultCh <- readIndexResult{index: pending.index}
	}
}

func (n *Node) expireReadIndexes() {
	now := time.Now()
	for requestID, pending := range n.pendingReadIndex {
		if now.Before(pending.deadline) {
			continue
		}
		delete(n.pendingReadIndex, requestID)
		pending.request.resultCh <- readIndexResult{err: ErrReadIndexTimeout}
	}
}

// 将快照写入存储并恢复状态机，Follower 侧收到快照时触发。
// 返回 error 表示持久化或状态机恢复失败，属不可恢复。
func (n *Node) applySnapshot(rd *raft.Ready) error {
	if rd.Snapshot == nil {
		return nil
	}
	if err := n.storage.ApplySnapshot(rd.Snapshot); err != nil {
		return fmt.Errorf("snapshot to storage: %w", err)
	}
	if err := n.sm.ApplySnapshot(rd.Snapshot); err != nil {
		return fmt.Errorf("snapshot to state machine: %w", err)
	}
	return nil
}

// 持久化 term、vote、commit 等状态。返回 error 属不可恢复。
func (n *Node) persistHardState(rd *raft.Ready) error {
	if rd.HardState == nil {
		return nil
	}
	if err := n.storage.SaveHardState(rd.HardState); err != nil {
		return err
	}
	// 仅在落盘成功后才对外暴露，避免外部读到未持久化的 term。
	n.raftTerm.Store(rd.HardState.Term)
	return nil
}

// 将不稳定日志条目写入持久化存储。返回 error 属不可恢复。
func (n *Node) appendEntries(rd *raft.Ready) error {
	if len(rd.Entries) == 0 {
		return nil
	}
	return n.storage.Append(rd.Entries)
}

// 将已持久化的 Raft 消息交给 Transport。
// 连接状态、失败统计和日志由 Transport 负责；
// Raft 协议会在后续 tick 中重新生成需要重发的消息。
func (n *Node) sendMessages(msgs []*raftpb.RaftMessage) {
	for _, msg := range msgs {
		_ = n.transport.Send(msg)
	}
}

// 将已提交的日志条目应用到状态机。
// StateMachine.Apply 须容忍重复调用（同一条 entry 可能被 apply 多次）。
func (n *Node) applyCommittedEntries(rd *raft.Ready) ([]ApplyResult, error) {
	if len(rd.CommittedEntries) == 0 {
		return nil, nil
	}
	return n.sm.Apply(rd.CommittedEntries)
}

// 在日志量超过阈值时创建快照并压缩旧日志。
// 快照失败不是致命错误，下一轮还能再试，但必须记录，否则 WAL 无限增长时无从排查。
func (n *Node) maybeSnapshot(rd *raft.Ready) {
	if len(rd.CommittedEntries) == 0 {
		return
	}

	lastIndex := rd.CommittedEntries[len(rd.CommittedEntries)-1].Index

	firstIndex, err := n.storage.FirstIndex()
	if err != nil {
		log.Printf("raftstore: maybeSnapshot: read first index: %v", err)
		return
	}
	if lastIndex-firstIndex < n.snapshotCount {
		return
	}
	data, err := n.sm.SnapshotData()
	if err != nil {
		log.Printf("raftstore: maybeSnapshot: get snapshot data: %v", err)
		return
	}
	_, cs, err := n.storage.InitialState()
	if err != nil {
		log.Printf("raftstore: maybeSnapshot: read conf state: %v", err)
		return
	}
	if err := n.storage.CreateSnapshot(lastIndex, cs, data); err != nil {
		log.Printf("raftstore: maybeSnapshot: create snapshot at index %d: %v", lastIndex, err)
		return
	}
	if err := n.storage.Compact(lastIndex); err != nil {
		// 快照已成功，日志压缩失败只是多占空间，下次重启会重新压缩。
		log.Printf("raftstore: maybeSnapshot: compact log up to index %d: %v", lastIndex, err)
	}
}

func (n *Node) updateLeader(rd *raft.Ready) {
	if rd.SoftState != nil {
		n.leaderID.Store(rd.SoftState.LeaderID)
	}
}
