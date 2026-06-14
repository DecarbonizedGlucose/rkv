package kv

import (
	"sync/atomic"
)

// 全局 Revision 管理器，负责生成全局唯一的 Revision。Revision 是一个
// 单调递增的 uint64 数字，初始 Revision 为 0，每次生成新的 Revision
// 时都会加 1。
//
// 读操作不增加 Revision，写操作会增加 Revision。Revision 用于底层 MVCC
// 版本号。
//
// Revision 应当被持久化到磁盘，重启恢复时能够继续使用上次的 Revision。
type RevisionManager struct {
	revision atomic.Uint64
}

// Next 在內部 Revision 加 1，並返回新的 Revision。
func (m *RevisionManager) Next() uint64 {
	return m.revision.Add(1)
}

// 看一眼当前 Revision，但不增加它。
func (m *RevisionManager) Peek() uint64 {
	return m.revision.Load()
}

// Set 将 Revision 设置为指定值。通常在恢复时使用，
// 从持久化存储中读取上次的 Revision 并设置。
func (m *RevisionManager) Set(rev uint64) {
	m.revision.Store(rev)
}

// newRevisionManager 创建一个新的 RevisionManager，初始 Revision 由参数指定。
// 初始 Revision 应当从持久化存储中读取，确保重启后能够继续使用上次的 Revision。
func NewRevisionManager(initial uint64) *RevisionManager {
	r := &RevisionManager{}
	r.revision.Store(initial)
	return r
}
