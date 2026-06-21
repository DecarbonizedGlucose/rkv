package lease

import (
	"container/list"
	"sync"
	"sync/atomic"
	"time"
)

// TimerTask 是时间轮中的单个定时任务。
type TimerTask struct {
	ID       uint64
	Callback func()
	rounds   int // 剩余圈数，0 表示本轮到期
	slotIdx  int // 所在槽索引，Cancel 时用于 O(1) 从链表移除
}

// TimeWheelConfig 配置时间轮参数。
type TimeWheelConfig struct {
	TickDuration time.Duration // tick 间隔，默认 100ms
	NumSlots     int           // 槽数量，默认 512
}

func DefaultTimeWheelConfig() *TimeWheelConfig {
	return &TimeWheelConfig{
		TickDuration: 100 * time.Millisecond,
		NumSlots:     512,
	}
}

// TimeWheel 是单层哈希时间轮，用于 Lease 过期管理。
//
// 时间划分为 numSlots 个槽，每槽 tickDuration。最大单圈覆盖 =
// tickDuration × numSlots。
//
// 超限 TTL 通过 rounds 字段多圈调度。
type TimeWheel struct {
	tickDuration time.Duration
	numSlots     int
	slots        []*list.List
	taskMap      map[uint64]*list.Element

	nextID atomic.Uint64 // 待分配的任务 ID

	mu        sync.Mutex
	curSlot   int
	ticker    *time.Ticker
	startOnce sync.Once
	stopOnce  sync.Once
	stopCh    chan struct{}
	doneCh    chan struct{}
}

// NewTimeWheel 创建一个新的时间轮实例。调用方须在 Start 后使用。
func NewTimeWheel(config *TimeWheelConfig) *TimeWheel {
	numSlots := config.NumSlots
	slots := make([]*list.List, numSlots)
	for i := range slots {
		slots[i] = list.New()
	}
	return &TimeWheel{
		tickDuration: config.TickDuration,
		numSlots:     numSlots,
		slots:        slots,
		taskMap:      make(map[uint64]*list.Element),
		stopCh:       make(chan struct{}),
		doneCh:       make(chan struct{}),
	}
}

// Start 启动时间轮的 tick goroutine。仅首次调用生效，重复调用无操作。
func (tw *TimeWheel) Start() {
	tw.startOnce.Do(func() {
		tw.mu.Lock()
		tw.ticker = time.NewTicker(tw.tickDuration)
		tw.mu.Unlock()

		go func() {
			defer close(tw.doneCh)
			for {
				select {
				case <-tw.ticker.C:
					tw.tick()
				case <-tw.stopCh:
					return
				}
			}
		}()
	})
}

// Stop 停止时间轮，等待 tick goroutine 退出后清理定时器。
// 可安全并发调用，也可在 Start 前调用（无操作）。
func (tw *TimeWheel) Stop() {
	tw.stopOnce.Do(func() {
		close(tw.stopCh)
	})
	<-tw.doneCh

	tw.mu.Lock()
	if tw.ticker != nil {
		tw.ticker.Stop()
		tw.ticker = nil
	}
	tw.mu.Unlock()
}

// Add 在 d 时间后执行回调，返回任务 ID 用于 Cancel。
// d <= 0 时立即执行回调并返回 0（不进入时间轮）。
func (tw *TimeWheel) Add(d time.Duration, cb func()) uint64 {
	if d <= 0 {
		cb()
		return 0
	}

	id := tw.nextID.Add(1)
	ticks := max(int(d/tw.tickDuration), 1) // 至少推进 1 个 tick，避免落在当前槽被立刻触发

	tw.mu.Lock()
	slotIdx := (tw.curSlot + ticks) % tw.numSlots
	rounds := ticks / tw.numSlots

	e := tw.slots[slotIdx].PushBack(&TimerTask{
		ID:       id,
		rounds:   rounds,
		Callback: cb,
		slotIdx:  slotIdx,
	})
	tw.taskMap[id] = e
	tw.mu.Unlock()
	return id
}

// Cancel 取消一个定时任务。已执行或不存在则无操作。
func (tw *TimeWheel) Cancel(id uint64) {
	if id == 0 {
		return
	}
	tw.mu.Lock()
	defer tw.mu.Unlock()

	e, ok := tw.taskMap[id]
	if !ok {
		return
	}
	task := e.Value.(*TimerTask)
	tw.slots[task.slotIdx].Remove(e)
	delete(tw.taskMap, id)
}

func (tw *TimeWheel) tick() {
	tw.mu.Lock()
	var cbs []func()
	slot := tw.slots[tw.curSlot]
	var next *list.Element
	for e := slot.Front(); e != nil; e = next {
		next = e.Next()
		task := e.Value.(*TimerTask)
		if task.rounds > 0 {
			task.rounds--
			continue
		}
		cbs = append(cbs, task.Callback)
		delete(tw.taskMap, task.ID)
		slot.Remove(e)
	}
	tw.curSlot = (tw.curSlot + 1) % tw.numSlots
	tw.mu.Unlock()

	for _, cb := range cbs {
		cb()
	}
}
