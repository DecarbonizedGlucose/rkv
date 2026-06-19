package watch

import (
	"github.com/DecarbonizedGlucose/rkv/api/proto/pkg/kvpb"
	"github.com/DecarbonizedGlucose/rkv/api/proto/pkg/rpcpb"
	"github.com/DecarbonizedGlucose/rkv/pkg/kv"
	"github.com/DecarbonizedGlucose/rkv/pkg/raftstore"
)

// WatchServer 实现 WatchService gRPC 接口。
type WatchServer struct {
	rpcpb.UnimplementedWatchServiceServer
	wm     *WatchManager
	node   *raftstore.Node
	nodeID uint64
	revMgr kv.RevisionPeeker
}

func NewWatchServer(wm *WatchManager, node *raftstore.Node, nodeID uint64, revMgr kv.RevisionPeeker) *WatchServer {
	return &WatchServer{wm: wm, node: node, nodeID: nodeID, revMgr: revMgr}
}

func (s *WatchServer) Watch(req *kvpb.WatchRequest, stream rpcpb.WatchService_WatchServer) error {
	w := s.wm.Subscribe(req.Key, req.Prefix, req.StartRevision, req.PrevKv)
	defer s.wm.Cancel(w.ID)

	// 发送 create 响应
	stream.Send(&kvpb.WatchResponse{
		Header:  s.makeHeader(),
		WatchId: w.ID,
		Created: true,
	})

	// 监听循环
	for {
		select {
		case ev, ok := <-w.EventCh:
			if !ok {
				return nil // Watcher 被 Cancel 了，channel 关闭
			}
			stream.Send(&kvpb.WatchResponse{
				Header:  s.makeHeader(),
				WatchId: w.ID,
				Events:  []*kvpb.Event{ev},
			})
		case <-stream.Context().Done():
			return nil
		}
	}
}

// ==================== Internal Helper ====================

func (s *WatchServer) makeHeader() *kvpb.ResponseHeader {
	return &kvpb.ResponseHeader{
		MemberId: s.nodeID,
		Revision: s.revMgr.Peek(),
		RaftTerm: s.node.Term(),
	}
}
