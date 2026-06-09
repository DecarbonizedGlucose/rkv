package option

import "time"

// Option 是 rkv 服务节点的顶级配置结构。
type Option struct {
	// NodeID 是本节点在集群中的唯一标识，对应 raft.Config.ID。
	NodeID uint64
	// Peers 是集群所有成员的 NodeID 列表。
	Peers []uint64
	// DataDir 是业务 KV 数据 BadgerDB 的存储目录。
	DataDir string
	// RaftDir 是 Raft 状态 BadgerDB 的存储目录，与 DataDir 分离。
	RaftDir string

	// ElectionTimeout 是选举超时的 tick 数。
	ElectionTimeout int
	// HeartbeatTimeout 是 Leader 发送心跳的 tick 间隔。
	HeartbeatTimeout int
	// TickInterval 是 Raft tick 的时间间隔。默认 100ms。
	TickInterval time.Duration
	// SnapshotCount 是两次快照之间允许的最大已应用日志条数。默认 10000。
	SnapshotCount uint64

	// RaftAddr 是 Raft 节点间通信的 gRPC 地址（本节点）。
	RaftAddr string
	// GRPCAddr 是对外客户端 gRPC 服务的地址（本节点）。
	GRPCAddr string
}

func DefaultConfig() *Option {
	return &Option{
		ElectionTimeout:  10,
		HeartbeatTimeout: 3,
		TickInterval:     100 * time.Millisecond,
		SnapshotCount:    10000,
	}
}
