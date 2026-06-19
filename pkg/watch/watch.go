package watch

import (
	"bytes"
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
	id := wm.nextID.Add(1)
	w := &Watcher{
		ID:       id,
		Key:      key,
		Prefix:   prefix,
		StartRev: startRev,
		PrevKV:   prevKV,
		EventCh:  make(chan *kvpb.Event, 256),
	}
	wm.mu.Lock()
	wm.watchers[id] = w
	wm.mu.Unlock()
	return w
}

func (wm *WatchManager) Cancel(id int64) {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	if w, ok := wm.watchers[id]; ok {
		delete(wm.watchers, id)
		close(w.EventCh)
	}
}

func (wm *WatchManager) Publish(key []byte, kv, prevKV *kvpb.KeyValue, eventType kvpb.EventType) {
	ev := &kvpb.Event{
		Type:   eventType,
		Kv:     kv,
		PrevKv: prevKV,
	}
	wm.mu.RLock()
	defer wm.mu.RUnlock()
	for _, w := range wm.watchers {
		if match(w, key) {
			select {
			case w.EventCh <- ev:
			default:
				// 慢消费者，通道满，丢弃事件以避免阻塞 Apply 路径。
			}
		}
	}
}

func match(w *Watcher, key []byte) bool {
	if w.Prefix {
		return bytes.HasPrefix(key, w.Key)
	}
	return bytes.Equal(key, w.Key)
}
