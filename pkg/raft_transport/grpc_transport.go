package raft_transport

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	"github.com/DecarbonizedGlucose/rkv/api/proto/pkg/raftpb"
)

// 统一 BidiStreamingClient / BidiStreamingServer
type peerStream interface {
	Send(*raftpb.RaftMessage) error
	Recv() (*raftpb.RaftMessage, error)
}

// Raft-gRPC Transport
type RGTransport struct {
	nodeID    uint64
	selfAddr  string
	peerAddrs map[uint64]string // map peerID -> addr

	recvCh      chan *raftpb.RaftMessage // 接收消息的通道
	dropCounter atomic.Uint64            // 因队列满丢弃的消息计数

	mu    sync.RWMutex
	sends map[uint64]peerStream
	// sendFailures 按 peer 记录连续发送失败次数，仅用于指数采样日志。
	sendFailures map[uint64]uint64

	grpcSrv *grpc.Server
	ctx     context.Context
	cancel  context.CancelFunc
}

type Config struct {
	NodeID   uint64
	SelfAddr string
	Peers    map[uint64]string
}

func New(cfg *Config) *RGTransport {
	if cfg.Peers == nil {
		cfg.Peers = make(map[uint64]string)
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &RGTransport{
		nodeID:       cfg.NodeID,
		selfAddr:     cfg.SelfAddr,
		peerAddrs:    cfg.Peers,
		recvCh:       make(chan *raftpb.RaftMessage, 512),
		sends:        make(map[uint64]peerStream),
		sendFailures: make(map[uint64]uint64),
		ctx:          ctx,
		cancel:       cancel,
	}
}

// ==================== 对外接口 ====================

// 创建 gRPC 服务器并监听，启动连接管理协程
func (t *RGTransport) Start() error {
	lis, err := net.Listen("tcp", t.selfAddr)
	if err != nil {
		return fmt.Errorf("transport: listen %s: %w", t.selfAddr, err)
	}

	kaParams := keepalive.ServerParameters{
		MaxConnectionIdle:     15 * time.Minute,
		MaxConnectionAge:      30 * time.Minute,
		MaxConnectionAgeGrace: 5 * time.Second,
		Time:                  30 * time.Second,
		Timeout:               10 * time.Second,
	}

	t.grpcSrv = grpc.NewServer(grpc.KeepaliveParams(kaParams))
	raftpb.RegisterRaftTransportServer(t.grpcSrv, &serverHandler{t: t})

	go func() {
		if err := t.grpcSrv.Serve(lis); err != nil {
			// GracefulStop/Stop 触发时 Serve 返回 nil，其余均属意外错误。
			log.Printf("raft_transport: grpc server exited: %v", err)
		}
	}()

	for id, addr := range t.peerAddrs {
		if id == t.nodeID {
			continue
		}
		if id > t.nodeID {
			go t.connectLoop(id, addr)
		}
		// 只允许小ID节点连接到大ID节点
		// 避免重复连接和竞态问题
	}

	return nil
}

// 向双向流发送消息
func (t *RGTransport) Send(msg *raftpb.RaftMessage) error {
	if msg.To == t.nodeID {
		return fmt.Errorf("transport: send to self")
	}
	t.mu.RLock()
	stream, ok := t.sends[msg.To]
	t.mu.RUnlock()
	if !ok {
		err := fmt.Errorf("transport: no active stream to peer %d", msg.To)
		t.recordSendFailure(msg.To, err)
		return err
	}
	if err := stream.Send(msg); err != nil {
		t.recordSendFailure(msg.To, err)
		return err
	}
	t.mu.Lock()
	delete(t.sendFailures, msg.To)
	t.mu.Unlock()
	return nil
}

func (t *RGTransport) recordSendFailure(peerID uint64, err error) {
	t.mu.Lock()
	t.sendFailures[peerID]++
	cnt := t.sendFailures[peerID]
	t.mu.Unlock()
	if cnt&(cnt-1) == 0 { // 1、2、4、8…
		log.Printf("raft_transport[%d]: send to peer %d failed (count=%d): %v", t.nodeID, peerID, cnt, err)
	}
}

// 获取只读的接收通道
func (t *RGTransport) Recv() <-chan *raftpb.RaftMessage {
	return t.recvCh
}

// 停止服务器和所有连接
func (t *RGTransport) Stop() error {
	t.cancel()
	if t.grpcSrv != nil {
		t.grpcSrv.GracefulStop()
	}
	return nil
}

// ==================== 连接管理 ====================

// 不断尝试连接到指定 peer，直到成功或上下文取消
func (t *RGTransport) connectLoop(peerID uint64, addr string) {
	backoff := time.Second
	var failures uint64
	for {
		established, err := t.establishStream(peerID, addr)
		if err != nil {
			select {
			case <-t.ctx.Done():
				return
			default:
			}
			if established {
				// 该 stream 曾成功建立；断线后的重连从新周期开始计数。
				backoff = time.Second
				failures = 0
			}
			failures++
			if failures&(failures-1) == 0 { // 1、2、4、8…
				log.Printf("raft_transport[%d]: peer %d stream unavailable (attempt=%d): %v; retry in %s", t.nodeID, peerID, failures, err, backoff)
			}
			select {
			case <-t.ctx.Done():
				return
			case <-time.After(backoff):
				backoff = min(backoff*2, 30*time.Second)
			}
			continue
		}
		backoff = time.Second
		failures = 0
	}
}

// 尝试建立到 peer 的 gRPC 连接并启动消息接收循环
// 这个函数返回意味着流RPC已经结束，连接被关闭或发生错误
func (t *RGTransport) establishStream(peerID uint64, addr string) (bool, error) {
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                30 * time.Second,
			Timeout:             10 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	if err != nil {
		return false, err
	}
	defer conn.Close()

	client := raftpb.NewRaftTransportClient(conn)
	stream, err := client.StreamMessage(t.ctx)
	if err != nil {
		return false, err
	}

	// Handshake
	if err := stream.Send(&raftpb.RaftMessage{From: t.nodeID, To: peerID, Type: raftpb.MessageType_HANDSHAKE}); err != nil {
		return false, err
	}

	t.mu.Lock()
	t.sends[peerID] = stream
	delete(t.sendFailures, peerID)
	t.mu.Unlock()
	log.Printf("raft_transport[%d]: stream to peer %d established (%s)", t.nodeID, peerID, addr)

	return true, t.serveStream(peerID, stream) // 这里是 Client
}

// 通用接收函数，支持Client和Server
func (t *RGTransport) serveStream(peerID uint64, stream peerStream) error {
	defer func() {
		t.mu.Lock()
		delete(t.sends, peerID)
		t.mu.Unlock()
	}()

	for {
		msg, err := stream.Recv()
		if err != nil {
			return err
		}

		select {
		case t.recvCh <- msg:
		case <-t.ctx.Done():
			return t.ctx.Err()
		default:
			// recvCh 满，消息被丢弃。Raft 依赖重传，单次丢弃不影响正确性，
			// 但持续丢弃说明消费跟不上（可能 run goroutine 卡住），需告警。
			cnt := t.dropCounter.Add(1)
			if cnt&(cnt-1) == 0 { // 指数采样：1,2,4,8… 次才打印
				log.Printf("raft_transport: recv queue full, dropped message from peer %d (total dropped=%d)", peerID, cnt)
			}
		}
	}
}

// ==================== Server Handler ====================

type serverHandler struct {
	raftpb.UnimplementedRaftTransportServer
	t *RGTransport
}

func (h *serverHandler) StreamMessage(stream grpc.BidiStreamingServer[raftpb.RaftMessage, raftpb.RaftMessage]) error {
	// Handshake：接收第一个消息获取 peerID
	msg, err := stream.Recv()
	if err != nil {
		return err
	}

	peerID := msg.From
	if msg.Type == raftpb.MessageType_HANDSHAKE && (peerID == 0 || peerID == h.t.nodeID) {
		return fmt.Errorf("invalid peer id from handshake")
	}

	h.t.mu.Lock()
	h.t.sends[peerID] = stream
	delete(h.t.sendFailures, peerID)
	h.t.mu.Unlock()
	log.Printf("raft_transport[%d]: stream from peer %d established", h.t.nodeID, peerID)

	// 进入接收循环
	return h.t.serveStream(peerID, stream) // 这里是 Server
}
