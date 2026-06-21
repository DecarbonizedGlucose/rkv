package lease

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ========================================
// Basic: add and fire
// ========================================

func TestTimeWheelBasic(t *testing.T) {
	tw := NewTimeWheel(DefaultTimeWheelConfig())
	tw.Start()
	defer tw.Stop()

	var fired atomic.Bool
	tw.Add(100*time.Millisecond, func() {
		fired.Store(true)
	})

	require.False(t, fired.Load())
	time.Sleep(300 * time.Millisecond)
	assert.True(t, fired.Load(), "task should fire after ~100ms")
}

// ========================================
// Cancel
// ========================================

func TestTimeWheelCancel(t *testing.T) {
	tw := NewTimeWheel(DefaultTimeWheelConfig())
	tw.Start()
	defer tw.Stop()

	var fired atomic.Bool
	id := tw.Add(200*time.Millisecond, func() {
		fired.Store(true)
	})
	tw.Cancel(id)

	time.Sleep(500 * time.Millisecond)
	assert.False(t, fired.Load(), "canceled task must not fire")
}

// ========================================
// Multi-turn: TTL exceeds one full revolution
// ========================================

func TestTimeWheelMultiTurns(t *testing.T) {
	// Small wheel to force multi-turn: 5 slots × 20ms = 100ms per revolution
	cfg := &TimeWheelConfig{TickDuration: 20 * time.Millisecond, NumSlots: 5}
	tw := NewTimeWheel(cfg)
	tw.Start()
	defer tw.Stop()

	var fired atomic.Bool
	// 300ms = 15 ticks = 3 full revolutions
	tw.Add(300*time.Millisecond, func() {
		fired.Store(true)
	})

	time.Sleep(500 * time.Millisecond)
	assert.True(t, fired.Load(), "multi-turn task should fire after ~300ms")
}

// ========================================
// High load: 1000 tasks with varying TTLs
// ========================================

func TestTimeWheelHighLoad(t *testing.T) {
	cfg := &TimeWheelConfig{TickDuration: 10 * time.Millisecond, NumSlots: 128}
	tw := NewTimeWheel(cfg)
	tw.Start()
	defer tw.Stop()

	const N = 1000
	var firedCount atomic.Int64
	var mu sync.Mutex
	maxDelay := time.Duration(0)
	start := time.Now()

	for i := 0; i < N; i++ {
		expected := time.Duration(50+i*2) * time.Millisecond
		if expected > 2500*time.Millisecond {
			expected = 2500 * time.Millisecond // cap for test speed
		}
		expectedAt := start.Add(expected)
		tw.Add(expected, func() {
			firedCount.Add(1)
			delay := time.Since(expectedAt)
			if delay < 0 {
				delay = -delay
			}
			mu.Lock()
			if delay > maxDelay {
				maxDelay = delay
			}
			mu.Unlock()
		})
	}

	// Wait for all tasks — max TTL is ~2500ms + 200ms error margin
	deadline := time.After(3 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

loop:
	for {
		select {
		case <-deadline:
			break loop
		case <-ticker.C:
			if firedCount.Load() >= N {
				break loop
			}
		}
	}

	assert.Equal(t, int64(N), firedCount.Load(), "all %d tasks should fire", N)
	mu.Lock()
	maxErr := maxDelay
	mu.Unlock()
	assert.Less(t, maxErr, 200*time.Millisecond,
		"max delay %v should be under 200ms tolerance", maxErr)
}

// ========================================
// Lifetime: Start / Stop / re-Start
// ========================================

func TestTimeWheelStopTwiceNoPanic(t *testing.T) {
	tw := NewTimeWheel(DefaultTimeWheelConfig())
	tw.Start()
	tw.Stop()
	tw.Stop() // second stop must not panic or hang
}

func TestTimeWheelStartTwiceNoDoubleTick(t *testing.T) {
	cfg := &TimeWheelConfig{TickDuration: 10 * time.Millisecond, NumSlots: 32}
	tw := NewTimeWheel(cfg)
	tw.Start()
	tw.Start() // second start is no-op via sync.Once
	defer tw.Stop()

	var fired atomic.Bool
	tw.Add(50*time.Millisecond, func() {
		fired.Store(true)
	})

	time.Sleep(200 * time.Millisecond)
	assert.True(t, fired.Load())
}

// ========================================
// Cancel id=0 no-op
// ========================================

func TestTimeWheelCancelZeroNoPanic(t *testing.T) {
	tw := NewTimeWheel(DefaultTimeWheelConfig())
	tw.Start()
	defer tw.Stop()

	// Cancel(0) must not panic
	tw.Cancel(0)
}

// ========================================
// Add with d<=0 fires immediately, returns 0
// ========================================

func TestTimeWheelAddZeroOrNegative(t *testing.T) {
	tw := NewTimeWheel(DefaultTimeWheelConfig())
	tw.Start()
	defer tw.Stop()

	var fired atomic.Bool
	id := tw.Add(0, func() {
		fired.Store(true)
	})

	assert.Equal(t, uint64(0), id, "immediate callback should return id=0")
	assert.True(t, fired.Load(), "d<=0 should fire immediately")
}

// ========================================
// Renewal: Cancel + re-Add (KeepAlive pattern)
// ========================================

func TestTimeWheelRenewal(t *testing.T) {
	cfg := &TimeWheelConfig{TickDuration: 10 * time.Millisecond, NumSlots: 64}
	tw := NewTimeWheel(cfg)
	tw.Start()
	defer tw.Stop()

	var fired atomic.Bool
	var mu sync.Mutex
	var fireTime time.Time

	// Schedule task at 200ms
	start := time.Now()
	id := tw.Add(200*time.Millisecond, func() {
		fired.Store(true)
		mu.Lock()
		fireTime = time.Now()
		mu.Unlock()
	})

	// At 100ms: simulate KeepAlive — Cancel old, re-Add new at 300ms from now
	time.Sleep(100 * time.Millisecond)
	tw.Cancel(id)
	id2 := tw.Add(300*time.Millisecond, func() {
		fired.Store(true)
		mu.Lock()
		fireTime = time.Now()
		mu.Unlock()
	})
	require.NotZero(t, id2)

	// At 250ms: original deadline (200ms) has passed — must NOT have fired
	time.Sleep(150 * time.Millisecond) // total: 250ms from start
	assert.False(t, fired.Load(),
		"original timer was cancelled, must not fire at original deadline")

	// Wait for new deadline: 100ms + 300ms = 400ms from start → check at ~500ms
	time.Sleep(300 * time.Millisecond) // total: 550ms from start
	assert.True(t, fired.Load(),
		"renewed timer should fire at extended deadline")
	mu.Lock()
	ft := fireTime
	mu.Unlock()
	assert.True(t, ft.After(start.Add(350*time.Millisecond)),
		"fire time %v should be after extended deadline (~400ms)", ft)
}
