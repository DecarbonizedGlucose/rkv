package server

import (
	"context"
	"net"

	"github.com/DecarbonizedGlucose/rkv/api/proto/pkg/rpcpb"
	"github.com/DecarbonizedGlucose/rkv/pkg/kv"
	"github.com/DecarbonizedGlucose/rkv/pkg/option"
	"github.com/DecarbonizedGlucose/rkv/pkg/raft"
	tr "github.com/DecarbonizedGlucose/rkv/pkg/raft_transport"
	"github.com/DecarbonizedGlucose/rkv/pkg/raftstore"
	"github.com/DecarbonizedGlucose/rkv/pkg/storage"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type Server struct {
	ctx context.Context

	kvServer   *kv.KVServer
	grpcServer *grpc.Server

	// 资源释放
	node     *raftstore.Node
	raftStor raftstore.Storage
	kvStor   storage.Storage

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

	revMgr := kv.NewRevisionManager(maxRev)
	sm := kv.NewStateMachine(kvStor, revMgr)
	trConfig := &tr.Config{
		NodeID:   o.NodeID,
		Peers:    o.PeersAddr,
		SelfAddr: o.RaftAddr,
	}
	transport := tr.New(trConfig)
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
	}

	node, err := raftstore.NewNode(rsConfig) // source 3
	if err != nil {
		defer s.tryRelease()
		return nil, err
	}
	s.node = node

	kvServer := kv.NewKVServer(node, kvStor, revMgr, o.NodeID)
	s.kvServer = kvServer
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
	reflection.Register(s.grpcServer) // 适配 grpcurl

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.grpcServer.Serve(lis)
	}()

	select {
	case <-s.ctx.Done():
		s.grpcServer.GracefulStop()
	case err := <-errCh:
		return err
	}

	return nil
}

func (s *Server) tryRelease() {
	if s.node != nil {
		s.node.Stop()
		s.node = nil
	}
	if s.raftStor != nil {
		s.raftStor.Close()
		s.raftStor = nil
	}
	if s.kvStor != nil {
		s.kvStor.Close()
		s.kvStor = nil
	}
}
