package watch

import (
	"sync"
	"sync/atomic"

	"github.com/DecarbonizedGlucose/rkv/api/proto/pkg/kvpb"
	"github.com/DecarbonizedGlucose/rkv/pkg/kv"
)

// Watcher 代表一个客户端 Watch 订阅。
// EventCh 容量 256，防止慢消费者阻塞 Apply 路径。
type Watcher struct {
	ID       int64
	Key      []byte
	Prefix   bool
	StartRev uint64
	PrevKV   bool
	EventCh  chan *kvpb.Event
}

// WatchManager 管理 Watcher 的注册、匹配和事件投递。
// Publish 在 Apply goroutine 内串行调用。
type WatchManager struct {
	mu       sync.RWMutex
	watchers map[int64]*Watcher
	nextID   atomic.Int64
	revMgr   *kv.RevisionManager
}

func NewWatchManager(revMgr *kv.RevisionManager) *WatchManager {
	return &WatchManager{
		watchers: make(map[int64]*Watcher),
		revMgr:   revMgr,
	}
}

func (wm *WatchManager) Subscribe(key []byte, prefix bool, startRev uint64, prevKV bool) *Watcher {
	return nil
}

func (wm *WatchManager) Cancel(id int64) {

}

func (wm *WatchManager) Publish(key []byte, kv, prevKV *kvpb.KeyValue, eventType kvpb.EventType) {

}

func match(w *Watcher, key []byte) bool {
	return false
}
