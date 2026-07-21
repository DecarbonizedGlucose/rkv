package server

import (
	"context"
	"log"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/proto"

	"github.com/DecarbonizedGlucose/rkv/api/proto/pkg/kvpb"
	"github.com/DecarbonizedGlucose/rkv/api/proto/pkg/rpcpb"
	"github.com/DecarbonizedGlucose/rkv/pkg/kv"
	"github.com/DecarbonizedGlucose/rkv/pkg/lease"
	"github.com/DecarbonizedGlucose/rkv/pkg/option"
	"github.com/DecarbonizedGlucose/rkv/pkg/raft"
	tr "github.com/DecarbonizedGlucose/rkv/pkg/raft_transport"
	"github.com/DecarbonizedGlucose/rkv/pkg/raftstore"
	"github.com/DecarbonizedGlucose/rkv/pkg/storage"
	"github.com/DecarbonizedGlucose/rkv/pkg/watch"
)

// Server 聚合 KV / Watch / Lease 三个 gRPC 服务，统一管理资源生命周期。
type Server struct {
	ctx context.Context

	kvServer    *kv.KVServer
	watchServer *watch.WatchServer
	leaseServer *lease.LeaseServer
	grpcServer  *grpc.Server

	// 资源释放
	node      *raftstore.Node
	transport tr.Transport
	tw        *lease.TimeWheel
	raftStor  raftstore.Storage
	kvStor    storage.Storage

	revMgr *kv.RevisionManager
	pidMgr *kv.ProposalIDManager

	o *option.Option
}

func NewServer(ctx context.Context, o *option.Option) (*Server, error) {
	s := &Server{
		ctx: ctx,
		o:   o,
	}
	if err := o.Validate(); err != nil {
		return nil, err
	}

	raftStor, err := raftstore.NewRaftStorage(o.RaftDir) // source 1
	if err != nil {
		defer s.tryRelease()
		return nil, err
	}
	s.raftStor = raftStor

	kvStor, maxRev, err := storage.NewBadgerStorage(o.DataDir) // source 2
	if err != nil {
		defer s.tryRelease()
		return nil, err
	}
	s.kvStor = kvStor

	s.revMgr = kv.NewRevisionManager(maxRev)
	s.pidMgr = &kv.ProposalIDManager{}

	sm := kv.NewStateMachine(kvStor, s.revMgr)
	trConfig := &tr.Config{
		NodeID:   o.NodeID,
		Peers:    o.PeersAddr,
		SelfAddr: o.RaftAddr,
	}
	transport := tr.New(trConfig)
	s.transport = transport
	raftConfig := &raft.Config{
		ID:               o.NodeID,
		Peers:            o.Peers,
		ElectionTimeout:  o.ElectionTimeout,
		HeartbeatTimeout: o.HeartbeatTimeout,
		Storage:          raftStor,
	}
	rsConfig := &raftstore.Config{
		RaftConfig:    raftConfig,
		Storage:       raftStor,
		Transport:     transport,
		StateMachine:  sm,
		SnapshotCount: o.SnapshotCount,
		TickInterval:  o.TickInterval,
	}

	node, err := raftstore.NewNode(rsConfig) // source 3
	if err != nil {
		defer s.tryRelease()
		return nil, err
	}
	s.node = node

	proposeLease := func(cmd *kvpb.Command) error {
		cmdBytes, err := proto.Marshal(cmd)
		if err != nil {
			return err
		}
		rev := s.revMgr.Next()
		op := &kv.ProposalOperation{
			ProposalID: s.pidMgr.Next(),
			Revision:   rev,
			Command:    cmdBytes,
		}
		data, err := kv.MarshalProposalOperation(op)
		if err != nil {
			return err
		}
		_, err = node.Propose(data, op.ProposalID)
		return err
	}

	s.kvServer = kv.NewKVServer(node, kvStor, s.revMgr, o.NodeID, s.pidMgr, o.AllowFollowerRead)

	wm := watch.NewWatchManager(s.revMgr)
	s.watchServer = watch.NewWatchServer(wm, node, o.NodeID, s.revMgr, kvStor)

	s.tw = lease.NewTimeWheel(lease.DefaultTimeWheelConfig())
	lm := lease.NewLeaseManager(s.tw, node, proposeLease)
	s.leaseServer = lease.NewLeaseServer(lm, node, o.NodeID, s.revMgr)

	sm.SetWatchPublisher(wm)
	sm.SetLeaseHandler(lm)

	return s, nil
}

func (s *Server) Serve() error {
	defer s.tryRelease()

	lis, err := net.Listen("tcp", s.o.GRPCAddr)
	if err != nil {
		return err
	}
	s.grpcServer = grpc.NewServer()
	rpcpb.RegisterKVServiceServer(s.grpcServer, s.kvServer)
	rpcpb.RegisterWatchServiceServer(s.grpcServer, s.watchServer)
	rpcpb.RegisterLeaseServiceServer(s.grpcServer, s.leaseServer)
	reflection.Register(s.grpcServer) // 适配 grpcurl

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.grpcServer.Serve(lis)
	}()

	select {
	case <-s.ctx.Done():
		// 收到退出信号，优雅停机。
		s.grpcServer.GracefulStop()
		return nil
	case <-s.node.Done():
		// 节点因不可恢复错误自行停机：停掉 gRPC 拒绝新请求，
		// 然后经 defer tryRelease 释放存储，返回致命错误供上层决定退出码。
		s.grpcServer.GracefulStop()
		return s.node.Err()
	case err := <-errCh:
		// gRPC Serve 自身出错（如端口被占）。
		return err
	}
}

// tryRelease 按依赖反序释放资源，幂等可重复调用。
// 顺序：先停 Node（停止对两个存储的读写）-> 停 Transport -> 关 Raft 存储 -> 关 KV 存储。
func (s *Server) tryRelease() {
	if s.tw != nil {
		s.tw.Stop()
		s.tw = nil
	}
	if s.node != nil {
		s.node.Stop() // 其内部 run goroutine 退出时会 Stop transport
		s.node = nil
	}
	if s.transport != nil {
		// Node 不存在（如NewNode 中途失败）时 transport 可能仍在运行；
		// Stop 幂等，重复调用安全。
		if err := s.transport.Stop(); err != nil {
			log.Printf("server: stop transport: %v", err)
		}
		s.transport = nil
	}
	if s.raftStor != nil {
		if err := s.raftStor.Close(); err != nil {
			log.Printf("server: close raft storage: %v", err)
		}
		s.raftStor = nil
	}
	if s.kvStor != nil {
		if err := s.kvStor.Close(); err != nil {
			log.Printf("server: close kv storage: %v", err)
		}
		s.kvStor = nil
	}
}
