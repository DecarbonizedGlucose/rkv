package lease

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DecarbonizedGlucose/rkv/api/proto/pkg/kvpb"
	"github.com/DecarbonizedGlucose/rkv/pkg/raftstore"
)

// Lease 表示一个存活中的租约。
type Lease struct {
	ID      int64
	TTL     int64    // 秒
	Keys    [][]byte // 关联的 key 列表
	timerID uint64   // TimeWheel 任务 ID，0 表示未调度
}

// LeaseManager 管理所有 Lease 的生命周期：创建、续约、撤销和过期。
//
// GrantLease/RevokeLease 通过 Raft 提案执行；KeepAlive 是本地操作；
// 过期由 TimeWheel 触发，通过 Raft 提案删除关联 key。
type LeaseManager struct {
	mu        sync.Mutex
	leases    map[int64]*Lease
	tw        *TimeWheel
	node      *raftstore.Node
	nextID    atomic.Int64
	proposeFn func(*kvpb.Command) error
}

func NewLeaseManager(tw *TimeWheel, node *raftstore.Node, proposeFn func(*kvpb.Command) error) *LeaseManager {
	l := &LeaseManager{
		leases:    make(map[int64]*Lease),
		tw:        tw,
		node:      node,
		proposeFn: proposeFn,
	}
	l.tw.Start()
	return l
}

// =================== gRPC handler ====================

// 新建租约，id=0 表示由系统分配。本函数是 gRPC handler。
func (lm *LeaseManager) LeaseGrant(id, ttl int64) (int64, error) {
	if id == 0 {
		id = lm.nextID.Add(1)
	}
	cmd := &kvpb.Command{
		Op: &kvpb.Command_LeaseGrant{
			LeaseGrant: &kvpb.LeaseGrantCommand{
				Id:  id,
				Ttl: ttl,
			},
		},
	}
	return id, lm.proposeFn(cmd)
}

// 撤销租约。关联 key 将被删除。本函数是 gRPC handler。
func (lm *LeaseManager) LeaseRevoke(id int64) error {
	cmd := &kvpb.Command{
		Op: &kvpb.Command_LeaseRevoke{
			LeaseRevoke: &kvpb.LeaseRevokeCommand{
				Id: id,
			},
		},
	}
	return lm.proposeFn(cmd)
}

// KeepAlive 续约租约，重置 TTL 和过期时间。返回新的 TTL。本函数是 gRPC handler。
func (lm *LeaseManager) KeepAlive(id int64) (int64, error) {
	// 这里不经过 Raft，所以要判断是否易主
	if !lm.node.IsLeader() {
		return 0, ErrNotLeader
	}

	lm.mu.Lock()
	defer lm.mu.Unlock()

	lease, ok := lm.leases[id]
	if !ok {
		return 0, ErrLeaseNotFound
	}
	// 重置 TTL 和过期时间，更新 TimeWheel 任务。
	lm.tw.Cancel(lease.timerID)
	lease.timerID = lm.tw.Add(time.Duration(lease.TTL)*time.Second, func() {
		lm.onExpire(id)
	})
	return lease.TTL, nil
}

// =================== 对外方法 ====================

// Grant 创建一个新租约，返回租约 ID。TTL 单位为秒。
func (lm *LeaseManager) Grant(leaseID int64, ttl int64) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	l := &Lease{
		ID:  leaseID,
		TTL: ttl,
	}
	l.timerID = lm.tw.Add(time.Duration(ttl)*time.Second, func() {
		lm.onExpire(leaseID)
	})
	lm.leases[leaseID] = l
}

// Revoke 撤销一个租约，返回要删除的关联 key。
func (lm *LeaseManager) Revoke(leaseID int64) [][]byte {
	lm.mu.Lock()
	l, ok := lm.leases[leaseID]
	if !ok {
		lm.mu.Unlock()
		return nil
	}
	keys := make([][]byte, len(l.Keys))
	copy(keys, l.Keys)
	lm.tw.Cancel(l.timerID)    // 不再调度
	delete(lm.leases, leaseID) // 删本地租约
	lm.mu.Unlock()

	return keys
}

// Attach 将一个 key 关联到租约上，租约过期时该 key 将被删除。
func (lm *LeaseManager) Attach(leaseID int64, key []byte) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	if l, ok := lm.leases[leaseID]; ok {
		l.Keys = append(l.Keys, key)
	}
}

// Detach 将一个 key 从租约上解绑，解绑后该 key 不受租约过期影响。
func (lm *LeaseManager) Detach(leaseID int64, key []byte) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	if l, ok := lm.leases[leaseID]; ok {
		for i, k := range l.Keys {
			if bytes.Equal(k, key) {
				l.Keys = append(l.Keys[:i], l.Keys[i+1:]...)
				return
			}
		}
	}
}

// =================== 内部方法 ====================

// onExpire 是租约过期的回调函数，在 TimeWheel 的 tick goroutine 中被调用。
func (lm *LeaseManager) onExpire(leaseID int64) {
	lm.mu.Lock()
	_, ok := lm.leases[leaseID]
	lm.mu.Unlock()
	if !ok {
		return
	}

	lm.proposeFn(&kvpb.Command{
		Op: &kvpb.Command_LeaseRevoke{
			LeaseRevoke: &kvpb.LeaseRevokeCommand{
				Id: leaseID,
			},
		},
	})
}
