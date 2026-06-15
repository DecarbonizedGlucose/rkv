package option

import (
	"fmt"
	"slices"
	"time"
)

// Option 是 rkv 服务节点的顶级配置结构。
type Option struct {
	// NodeID 是本节点在集群中的唯一标识，对应 raft.Config.ID。
	NodeID uint64
	// Peers 是集群所有成员的 NodeID 列表。
	Peers []uint64
	// 节点 ID 到 RaftAddr 的映射
	PeersAddr map[uint64]string
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

func (o *Option) SetPeers(peers []uint64, peersAddr map[uint64]string) *Option {
	o.Peers = slices.Clone(peers)
	o.PeersAddr = make(map[uint64]string)
	for _, id := range o.Peers {
		addr, ok := peersAddr[id]
		if !ok {
			panic(fmt.Sprintf("peer ID %d not found in peersAddr", id))
		}
		o.PeersAddr[id] = addr
	}
	return o
}

func (o *Option) SetDataDir(dataDir string) *Option {
	o.DataDir = dataDir
	return o
}

func (o *Option) SetRaftDir(raftDir string) *Option {
	o.RaftDir = raftDir
	return o
}

func (o *Option) Validate() error {
	if o.NodeID == 0 {
		return fmt.Errorf("NodeID must be non-zero")
	}
	if len(o.Peers) == 0 {
		return fmt.Errorf("Peers must be non-empty")
	}
	f := false
	for _, id := range o.Peers {
		if id == 0 {
			return fmt.Errorf("Peer ID must be non-zero")
		}
		if a, ok := o.PeersAddr[id]; !ok {
			return fmt.Errorf("Peer ID %d not found in PeersAddr", id)
		} else if a == o.RaftAddr {
			f = true
		}
	}
	if !f {
		return fmt.Errorf("Peers must contain NodeID")
	}
	if o.DataDir == "" {
		return fmt.Errorf("DataDir must be set")
	}
	if o.RaftDir == "" {
		return fmt.Errorf("RaftDir must be set")
	}
	if o.TickInterval <= 0 {
		return fmt.Errorf("TickInterval must be positive")
	}
	if o.SnapshotCount == 0 {
		return fmt.Errorf("SnapshotCount must be positive")
	}
	if o.RaftAddr == "" {
		return fmt.Errorf("RaftAddr must be set")
	}
	if o.GRPCAddr == "" {
		return fmt.Errorf("GRPCAddr must be set")
	}
	if o.HeartbeatTimeout <= 0 {
		return fmt.Errorf("HeartbeatTimeout must be positive")
	}
	if o.ElectionTimeout <= o.HeartbeatTimeout {
		return fmt.Errorf("ElectionTimeout must be greater than HeartbeatTimeout")
	}
	return nil
}
