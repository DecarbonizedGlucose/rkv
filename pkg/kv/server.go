package kv

import (
	"context"
	"fmt"
	"log"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/DecarbonizedGlucose/rkv/api/proto/pkg/kvpb"
	"github.com/DecarbonizedGlucose/rkv/api/proto/pkg/rpcpb"
	"github.com/DecarbonizedGlucose/rkv/pkg/raftstore"
	"github.com/DecarbonizedGlucose/rkv/pkg/storage"
)

type KVServer struct {
	rpcpb.UnimplementedKVServiceServer
	node   *raftstore.Node
	stor   storage.Storage
	revMgr *RevisionManager
	nodeID uint64
	pidMgr *ProposalIDManager
}

func NewKVServer(node *raftstore.Node, stor storage.Storage, revMgr *RevisionManager, nodeID uint64, pidMgr *ProposalIDManager) *KVServer {
	return &KVServer{
		node:   node,
		stor:   stor,
		revMgr: revMgr,
		nodeID: nodeID,
		pidMgr: pidMgr,
	}
}

// ==================== 读接口 ====================

func (s *KVServer) Get(ctx context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	if !s.isLeader(ctx) {
		log.Println("KVServer: get request rejected, not leader")
		return nil, status.Error(codes.FailedPrecondition, "not leader")
	}
	ikv, err := s.stor.Get(req.Key, s.revMgr.Peek())
	if err == storage.ErrKeyNotFound { // ikv == nil
		return &kvpb.GetResponse{
			Header: s.makeHeader(),
			Count:  0,
		}, nil
	} else if err != nil {
		log.Printf("KVServer: get key: %v", err)
		return nil, status.Error(codes.Internal, "internal error")
	}
	return &kvpb.GetResponse{
		Header: s.makeHeader(),
		Kv:     ikv.ToProto(),
		Count:  1,
	}, nil
}

func (s *KVServer) Range(ctx context.Context, req *kvpb.RangeRequest) (*kvpb.RangeResponse, error) {
	if !s.isLeader(ctx) {
		log.Println("KVServer: range request rejected, not leader")
		return nil, status.Error(codes.FailedPrecondition, "not leader")
	}
	end := req.RangeEnd
	if len(end) == 0 {
		end = nil
	}
	ikvs, more, err := s.stor.Range(req.RangeStart, end, int(req.Limit), nil, s.revMgr.Peek())
	if err != nil {
		log.Printf("KVServer: range scan: %v", err)
		return nil, status.Error(codes.Internal, "internal error")
	}
	kvs := make([]*kvpb.KeyValue, 0, len(ikvs))
	for _, ikv := range ikvs {
		kvs = append(kvs, ikv.ToProto())
	}
	return &kvpb.RangeResponse{
		Header: s.makeHeader(),
		Kvs:    kvs,
		Count:  int64(len(kvs)),
		More:   more,
	}, nil
}

// ==================== 写接口 ====================

func (s *KVServer) Put(ctx context.Context, req *kvpb.PutRequest) (*kvpb.PutResponse, error) {
	if !s.isLeader(ctx) {
		log.Println("KVServer: put request rejected, not leader")
		return nil, status.Error(codes.FailedPrecondition, "not leader")
	}
	res, err := s.proposeWrite(ctx, &kvpb.Command{
		Op: &kvpb.Command_Put{Put: req},
	})
	if err != nil {
		return nil, err
	}
	putResp := res.GetPut()
	putResp.Header = s.makeHeader()
	return putResp, nil
}

func (s *KVServer) Delete(ctx context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	if !s.isLeader(ctx) {
		log.Println("KVServer: delete request rejected, not leader")
		return nil, status.Error(codes.FailedPrecondition, "not leader")
	}
	res, err := s.proposeWrite(ctx, &kvpb.Command{
		Op: &kvpb.Command_Delete{Delete: req},
	})
	if err != nil {
		return nil, err
	}
	delResp := res.GetDelete()
	delResp.Header = s.makeHeader()
	return delResp, nil
}

func (s *KVServer) Txn(ctx context.Context, req *kvpb.TxnRequest) (*kvpb.TxnResponse, error) {
	if !s.isLeader(ctx) {
		log.Println("KVServer: txn request rejected, not leader")
		return nil, status.Error(codes.FailedPrecondition, "not leader")
	}
	res, err := s.proposeWrite(ctx, &kvpb.Command{
		Op: &kvpb.Command_Txn{Txn: req},
	})
	if err != nil {
		return nil, err
	}
	txnResp := res.GetTxn()
	txnResp.Header = s.makeHeader()
	return txnResp, nil
}

// ==================== Internal Helper ====================

func (s *KVServer) isLeader(ctx context.Context) bool {
	if s.node.IsLeader() {
		return true
	}
	grpc.SetTrailer(ctx, metadata.Pairs("leader-id", fmt.Sprintf("%d", s.node.LeaderID())))
	return false
}

func (s *KVServer) makeHeader() *kvpb.ResponseHeader {
	return &kvpb.ResponseHeader{
		MemberId: s.nodeID,
		Revision: s.revMgr.Peek(),
		RaftTerm: s.node.Term(),
	}
}

func (s *KVServer) proposeWrite(ctx context.Context, cmd *kvpb.Command) (*kvpb.Result, error) {
	pid := s.pidMgr.Next()

	cmdBytes, err := proto.Marshal(cmd)
	if err != nil {
		log.Printf("KVServer: marshal command: %v", err)
		return nil, status.Error(codes.Internal, "internal error")
	}
	// revision 在所有可能失败的准备工作（proto.Marshal）之后分配，
	// 避免浪费。此点之后的 gob 编码（marshalProposalOperation）永不失败，
	// n.Propose 失败产生的 revision 缺口对 BadgerDB 无害（版本只需单调）。
	rev := s.revMgr.Next()
	op := &ProposalOperation{
		ProposalID: pid,
		Revision:   rev,
		Command:    cmdBytes,
	}
	data, err := MarshalProposalOperation(op)
	if err != nil {
		log.Printf("KVServer: marshal proposal operation: %v", err)
		return nil, status.Error(codes.Internal, "internal error")
	}
	resultBytes, err := s.node.Propose(data, pid) // 日志真正被应用
	if err != nil {
		if err == raftstore.ErrNotLeader {
			log.Println("KVServer: propose rejected, not leader")
			grpc.SetTrailer(ctx, metadata.Pairs("leader-id", fmt.Sprintf("%d", s.node.LeaderID())))
			return nil, status.Error(codes.FailedPrecondition, "not leader")
		}
		log.Printf("KVServer: propose: %v", err)
		return nil, status.Error(codes.Internal, "internal error")
	}
	pr, err := UnmarshalProposalResult(resultBytes)
	if err != nil {
		log.Printf("KVServer: unmarshal proposal result: %v", err)
		return nil, status.Error(codes.Internal, "internal error")
	}
	var res kvpb.Result
	if err := proto.Unmarshal(pr.Result, &res); err != nil {
		log.Printf("KVServer: unmarshal result: %v", err)
		return nil, status.Error(codes.Internal, "internal error")
	}
	return &res, nil
}
