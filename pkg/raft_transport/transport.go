package raft_transport

import "github.com/DecarbonizedGlucose/rkv/api/proto/pkg/raftpb"

// Raft 集群之间通信中枢
type Transport interface {
	Send(msg *raftpb.RaftMessage) error
	Recv() <-chan *raftpb.RaftMessage
	Start() error
	Stop() error
}
