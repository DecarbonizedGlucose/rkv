package watch

import (
	"log"

	"github.com/DecarbonizedGlucose/rkv/api/proto/pkg/kvpb"
	"github.com/DecarbonizedGlucose/rkv/api/proto/pkg/rpcpb"
	"github.com/DecarbonizedGlucose/rkv/pkg/kv"
	"github.com/DecarbonizedGlucose/rkv/pkg/raftstore"
	"github.com/DecarbonizedGlucose/rkv/pkg/storage"
	"github.com/DecarbonizedGlucose/rkv/pkg/util"
)

// WatchServer 实现 WatchService gRPC 接口。
type WatchServer struct {
	rpcpb.UnimplementedWatchServiceServer
	wm     *WatchManager
	node   *raftstore.Node
	nodeID uint64
	revMgr kv.RevisionPeeker
	stor   storage.Storage
}

func NewWatchServer(wm *WatchManager, node *raftstore.Node, nodeID uint64, revMgr kv.RevisionPeeker, stor storage.Storage) *WatchServer {
	return &WatchServer{wm: wm, node: node, nodeID: nodeID, revMgr: revMgr, stor: stor}
}

func (s *WatchServer) Watch(req *kvpb.WatchRequest, stream rpcpb.WatchService_WatchServer) error {
	// 从 start_revision 开始，回放该 key/prefix 下所有已存在的 KV
	// 如果 key 早已被删除，不回放 DELETE 事件，保持与后续实时订阅一致。
	currentRev := s.revMgr.Peek()
	if req.StartRevision > 0 && req.StartRevision < currentRev && s.stor != nil {
		_, _, err := s.stor.Range(nil, nil, 0, func(ikv *util.InternalKV) bool {
			if ikv.MRevision < req.StartRevision {
				return true // 尚未被修改
			}
			if !MatchKey(req.Key, req.Prefix, ikv.Key) {
				return true
			}
			if err := stream.Send(&kvpb.WatchResponse{
				Header: s.makeHeader(),
				Events: []*kvpb.Event{{Type: kvpb.EventType_PUT, Kv: ikv.ToProto()}},
			}); err != nil {
				log.Printf("WatchServer: replay send: %v", err)
				return false
			}
			return true
		}, currentRev)
		if err != nil {
			log.Printf("WatchServer: replay scan: %v", err)
		}
	}

	// 实时订阅
	w := s.wm.Subscribe(req.Key, req.Prefix, req.StartRevision, req.PrevKv)
	defer s.wm.Cancel(w.ID)

	// 发送 created 响应
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
