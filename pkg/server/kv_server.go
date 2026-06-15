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
)

type Server struct {
	kvServer   *kv.KVServer
	grpcServer *grpc.Server

	// 资源释放
	node     *raftstore.Node
	raftStor raftstore.Storage
	kvStor   storage.Storage

	o *option.Option
}

func NewServer(o *option.Option) (*Server, error) {
	if err := o.Validate(); err != nil {
		return nil, err
	}
	raftStor, err := raftstore.NewRaftStorage(o.RaftDir)
	if err != nil {
		return nil, err
	}
	kvStor, maxRev, err := storage.NewBadgerStorage(o.DataDir)
	if err != nil {
		return nil, err
	}
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
	node, err := raftstore.NewNode(rsConfig)
	if err != nil {
		return nil, err
	}
	kvServer := kv.NewKVServer(node, kvStor, revMgr, o.NodeID)
	return &Server{
		kvServer: kvServer,
		node:     node,
		raftStor: raftStor,
		kvStor:   kvStor,
		o:        o,
	}, nil
}

func (s *Server) Serve(ctx context.Context) error {
	lis, err := net.Listen("tcp", s.o.GRPCAddr)
	if err != nil {
		return err
	}
	s.grpcServer = grpc.NewServer()
	rpcpb.RegisterKVServiceServer(s.grpcServer, s.kvServer)

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.grpcServer.Serve(lis)
	}()

	select {
	case <-ctx.Done():
		s.grpcServer.GracefulStop()
	case err := <-errCh:
		return err
	}

	s.node.Stop()
	s.raftStor.Close()
	s.kvStor.Close()
	return nil
}
