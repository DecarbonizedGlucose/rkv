package lease

import (
	"sync"

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
	mu     sync.Mutex
	leases map[int64]*Lease
	tw     *TimeWheel
	node   *raftstore.Node
}

func NewLeaseManager(tw *TimeWheel, node *raftstore.Node) *LeaseManager {
	return nil
}

// =================== gRPC handler ====================

func (lm *LeaseManager) GrantLease(ttl int64) (id int64, err error) {
	return 0, nil
}

func (lm *LeaseManager) RevokeLease(id int64) error {
	return nil
}

func (lm *LeaseManager) KeepAlive(id int64) (int64, error) {
	return 0, nil
}

// =================== 对外方法 ====================

func (lm *LeaseManager) Grant(leaseID int64, ttl int64) {

}

func (lm *LeaseManager) Revoke(leaseID int64) {

}

func (lm *LeaseManager) Attach(leaseID int64, key []byte) {

}

func (lm *LeaseManager) Detach(leaseID int64, key []byte) {

}

// =================== 内部方法 ====================

func (lm *LeaseManager) onExpire(leaseID int64) {

}
