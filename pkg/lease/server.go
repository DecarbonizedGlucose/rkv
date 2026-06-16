package lease

import (
	"github.com/DecarbonizedGlucose/rkv/api/proto/pkg/rpcpb"
	"github.com/DecarbonizedGlucose/rkv/pkg/raftstore"
)

// LeaseServer 实现 LeaseService gRPC 接口。
type LeaseServer struct {
	rpcpb.UnimplementedLeaseServiceServer
	lm     *LeaseManager
	node   *raftstore.Node
	nodeID uint64
}

func NewLeaseServer(lm *LeaseManager, node *raftstore.Node, nodeID uint64) *LeaseServer {
	return &LeaseServer{lm: lm, node: node, nodeID: nodeID}
}
