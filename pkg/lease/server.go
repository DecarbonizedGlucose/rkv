package lease

import (
	"context"
	stderrors "errors"
	"fmt"
	"log"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/DecarbonizedGlucose/rkv/api/proto/pkg/kvpb"
	"github.com/DecarbonizedGlucose/rkv/api/proto/pkg/rpcpb"
	"github.com/DecarbonizedGlucose/rkv/pkg/kv"
	"github.com/DecarbonizedGlucose/rkv/pkg/raftstore"
)

// LeaseServer 实现 LeaseService gRPC 接口。
type LeaseServer struct {
	rpcpb.UnimplementedLeaseServiceServer
	lm     *LeaseManager
	node   *raftstore.Node
	nodeID uint64
	revMgr kv.RevisionPeeker
}

func NewLeaseServer(lm *LeaseManager, node *raftstore.Node, nodeID uint64, revMgr kv.RevisionPeeker) *LeaseServer {
	return &LeaseServer{lm: lm, node: node, nodeID: nodeID, revMgr: revMgr}
}

func (s *LeaseServer) LeaseGrant(ctx context.Context, req *kvpb.LeaseGrantRequest) (*kvpb.LeaseGrantResponse, error) {
	if !s.isLeader(ctx) {
		log.Println("LeaseServer: LeaseGrant rejected, not leader")
		return nil, status.Error(codes.FailedPrecondition, "not leader")
	}
	id, err := s.lm.LeaseGrant(req.Id, req.Ttl)
	if err != nil {
		log.Printf("LeaseServer: LeaseGrant failed: %v", err)
		return nil, status.Error(codes.Internal, "internal error")
	}
	return &kvpb.LeaseGrantResponse{
		Header: s.makeHeader(),
		Id:     id,
		Ttl:    req.Ttl,
	}, nil
}

func (s *LeaseServer) LeaseRevoke(ctx context.Context, req *kvpb.LeaseRevokeRequest) (*kvpb.LeaseRevokeResponse, error) {
	if !s.isLeader(ctx) {
		log.Println("LeaseServer: LeaseRevoke rejected, not leader")
		return nil, status.Error(codes.FailedPrecondition, "not leader")
	}
	if err := s.lm.LeaseRevoke(req.Id); err != nil {
		log.Printf("LeaseServer: LeaseRevoke failed: %v", err)
		return nil, status.Error(codes.Internal, "internal error")
	}
	return &kvpb.LeaseRevokeResponse{
		Header: s.makeHeader(),
	}, nil
}

func (s *LeaseServer) LeaseKeepAlive(stream rpcpb.LeaseService_LeaseKeepAliveServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return nil // io.EOF：客户端关闭流
		}
		ttl, err := s.lm.KeepAlive(req.Id)
		if err != nil {
			log.Printf("LeaseServer: KeepAlive(%d) failed: %v", req.Id, err)
			// 区分两种失败：
			if stderrors.Is(err, ErrNotLeader) {
				grpc.SetTrailer(stream.Context(), metadata.Pairs("leader-id", fmt.Sprintf("%d", s.node.LeaderID())))
				return status.Error(codes.FailedPrecondition, "not leader")
			}
			return status.Error(codes.NotFound, "lease not found")
		}
		if err := stream.Send(&kvpb.LeaseKeepAliveResponse{
			Header: s.makeHeader(),
			Id:     req.Id,
			Ttl:    ttl,
		}); err != nil {
			return err
		}
	}
}

// ==================== Internal Helper ====================

func (s *LeaseServer) isLeader(ctx context.Context) bool {
	if s.node.IsLeader() {
		return true
	}
	grpc.SetTrailer(ctx, metadata.Pairs("leader-id", fmt.Sprintf("%d", s.node.LeaderID())))
	return false
}

func (s *LeaseServer) makeHeader() *kvpb.ResponseHeader {
	return &kvpb.ResponseHeader{
		MemberId: s.nodeID,
		Revision: s.revMgr.Peek(),
		RaftTerm: s.node.Term(),
	}
}
