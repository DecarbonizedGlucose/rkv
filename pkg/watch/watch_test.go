package watch

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/DecarbonizedGlucose/rkv/api/proto/pkg/kvpb"
	"github.com/DecarbonizedGlucose/rkv/pkg/kv"
)

func newTestWM(t *testing.T) *WatchManager {
	t.Helper()
	return NewWatchManager(kv.NewRevisionManager(0))
}

// ========================================
// Subscribe / Cancel
// ========================================

func TestSubscribe(t *testing.T) {
	wm := newTestWM(t)

	w := wm.Subscribe([]byte("k1"), false, 0, false)
	require.NotNil(t, w)
	assert.Equal(t, int64(1), w.ID)
	assert.Equal(t, []byte("k1"), w.Key)
	assert.False(t, w.Prefix)

	// verify registered in watchers map
	wm.mu.RLock()
	_, ok := wm.watchers[w.ID]
	wm.mu.RUnlock()
	assert.True(t, ok)
}

func TestSubscribeIDIncrement(t *testing.T) {
	wm := newTestWM(t)

	w1 := wm.Subscribe([]byte("a"), false, 0, false)
	w2 := wm.Subscribe([]byte("b"), false, 0, false)
	assert.Equal(t, int64(1), w1.ID)
	assert.Equal(t, int64(2), w2.ID)
}

func TestCancel(t *testing.T) {
	wm := newTestWM(t)

	w := wm.Subscribe([]byte("k1"), false, 0, false)
	wm.Cancel(w.ID)

	// watcher removed from map
	wm.mu.RLock()
	_, ok := wm.watchers[w.ID]
	wm.mu.RUnlock()
	assert.False(t, ok)

	// channel closed
	_, open := <-w.EventCh
	assert.False(t, open)
}

func TestCancelTwiceNoPanic(t *testing.T) {
	wm := newTestWM(t)

	w := wm.Subscribe([]byte("k1"), false, 0, false)
	wm.Cancel(w.ID)
	// second cancel must not panic on double-close or missing key
	wm.Cancel(w.ID)
}

// ========================================
// Publish
// ========================================

func TestPublishExactMatch(t *testing.T) {
	wm := newTestWM(t)

	w := wm.Subscribe([]byte("foo"), false, 0, false)

	kv := &kvpb.KeyValue{Key: []byte("foo"), Value: []byte("bar")}
	wm.Publish([]byte("foo"), kv, nil, kvpb.EventType_PUT)

	select {
	case ev := <-w.EventCh:
		assert.Equal(t, kvpb.EventType_PUT, ev.Type)
		assert.Equal(t, []byte("foo"), ev.Kv.Key)
	case <-time.After(time.Second):
		t.Fatal("expected event on matching key")
	}
}

func TestPublishNoMatch(t *testing.T) {
	wm := newTestWM(t)

	w := wm.Subscribe([]byte("foo"), false, 0, false)

	kv := &kvpb.KeyValue{Key: []byte("bar"), Value: []byte("x")}
	wm.Publish([]byte("bar"), kv, nil, kvpb.EventType_PUT)

	select {
	case <-w.EventCh:
		t.Fatal("unexpected event for non-matching key")
	case <-time.After(50 * time.Millisecond):
		// ok
	}
}

func TestPublishPrefixMatch(t *testing.T) {
	wm := newTestWM(t)

	w := wm.Subscribe([]byte("/svc/"), true, 0, false)

	kv := &kvpb.KeyValue{Key: []byte("/svc/node1"), Value: []byte("addr1")}
	wm.Publish([]byte("/svc/node1"), kv, nil, kvpb.EventType_PUT)

	select {
	case ev := <-w.EventCh:
		assert.Equal(t, kvpb.EventType_PUT, ev.Type)
	case <-time.After(time.Second):
		t.Fatal("expected prefix-match event")
	}
}

func TestPublishPrefixNoMatch(t *testing.T) {
	wm := newTestWM(t)

	w := wm.Subscribe([]byte("/svc/"), true, 0, false)

	kv := &kvpb.KeyValue{Key: []byte("/other/key"), Value: []byte("x")}
	wm.Publish([]byte("/other/key"), kv, nil, kvpb.EventType_PUT)

	select {
	case <-w.EventCh:
		t.Fatal("unexpected event outside prefix")
	case <-time.After(50 * time.Millisecond):
		// ok
	}
}

func TestPublishMultipleWatchers(t *testing.T) {
	wm := newTestWM(t)

	w1 := wm.Subscribe([]byte("shared"), false, 0, false)
	w2 := wm.Subscribe([]byte("shared"), false, 0, false)

	kv := &kvpb.KeyValue{Key: []byte("shared"), Value: []byte("v")}
	wm.Publish([]byte("shared"), kv, nil, kvpb.EventType_PUT)

	for _, w := range []*Watcher{w1, w2} {
		select {
		case ev := <-w.EventCh:
			assert.Equal(t, []byte("shared"), ev.Kv.Key)
		case <-time.After(time.Second):
			t.Fatal("both watchers should receive event")
		}
	}
}

func TestPublishChannelFullDropsEvent(t *testing.T) {
	wm := newTestWM(t)

	// create watcher with channel already full (cap 256)
	w := wm.Subscribe([]byte("k1"), false, 0, false)
	// fill channel
	for i := 0; i < 256; i++ {
		w.EventCh <- &kvpb.Event{}
	}

	kv := &kvpb.KeyValue{Key: []byte("k1"), Value: []byte("v")}
	// must not block
	done := make(chan struct{})
	go func() {
		wm.Publish([]byte("k1"), kv, nil, kvpb.EventType_PUT)
		close(done)
	}()

	select {
	case <-done:
		// ok — Publish returned without blocking
	case <-time.After(time.Second):
		t.Fatal("Publish blocked on full channel")
	}
}

// ========================================
// match helper
// ========================================

func TestMatchExact(t *testing.T) {
	w := &Watcher{Key: []byte("foo"), Prefix: false}
	assert.True(t, match(w, []byte("foo")))
	assert.False(t, match(w, []byte("foobar")))
	assert.False(t, match(w, []byte("bar")))
}

func TestMatchPrefix(t *testing.T) {
	w := &Watcher{Key: []byte("/svc/"), Prefix: true}
	assert.True(t, match(w, []byte("/svc/node1")))
	assert.True(t, match(w, []byte("/svc/")))
	assert.False(t, match(w, []byte("/other")))
	assert.False(t, match(w, []byte("/svc"))) // 前缀 "/svc/" 不匹配 "/svc"
}

func TestMatchEmptyKey(t *testing.T) {
	w := &Watcher{Key: []byte(""), Prefix: true}
	assert.True(t, match(w, []byte("anything")))
	assert.True(t, match(w, []byte("")))
}
