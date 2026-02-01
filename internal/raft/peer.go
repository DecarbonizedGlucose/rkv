package raft

import (
	raftpb "github.com/DecarbonizedGlucose/rkv/api/raftrpc"
	"github.com/DecarbonizedGlucose/rkv/internal/types"
	"google.golang.org/grpc"
)

type Peer struct {
	ID                *string
	Address           *string
	clientConnection  *grpc.ClientConn
	consensusClient   raftpb.RaftConsensusClient
	persistenceClient raftpb.RaftPersistenceClient
}

func MakePeer(id *string, addr *string, opts []grpc.DialOption) (*Peer, error) {
	p := &Peer{ID: id, Address: addr}
	conn, err := grpc.NewClient(*addr, opts...)
	if err != nil {
		return nil, err
	}
	p.clientConnection = conn
	p.consensusClient = raftpb.NewRaftConsensusClient(conn)
	p.persistenceClient = raftpb.NewRaftPersistenceClient(conn)
	return p, nil
}

func MakePeers(cfg *types.ServerConfig, opts []grpc.DialOption) ([]*Peer, int) {
	peers := make([]*Peer, len(cfg.Nodes))
	var me int
	for i, node := range cfg.Nodes {
		if node.ID == cfg.SelfID {
			peers[i] = nil
			me = i
			continue
		}
		peer, err := MakePeer(&node.ID, &node.Address, opts)
		if err != nil {
			panic(err)
		}
		peers[i] = peer
	}
	return peers, me
}
