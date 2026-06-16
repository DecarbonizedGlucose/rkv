package watch

import (
	"github.com/DecarbonizedGlucose/rkv/api/proto/pkg/rpcpb"
)

// WatchServer 实现 WatchService gRPC 接口。
type WatchServer struct {
	rpcpb.UnimplementedWatchServiceServer
	wm *WatchManager
}

func NewWatchServer(wm *WatchManager) *WatchServer {
	return &WatchServer{wm: wm}
}
