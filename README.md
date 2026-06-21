# rkv

基于 Raft 共识算法的分布式 KV 注册中心，Go 实现，参照 etcd 设计。

## 功能

- **KV 存储**：Put / Get / Delete / Range / 事务（CAS）
- **Watch**：键或前缀的实时事件流
- **Lease**：TTL 租约 + KeepAlive 自动续约，租约过期时关联 key 自动删除
- **服务注册 / 发现**：基于 Lease + Watch 实现，开箱即用
- **强一致**：所有写操作经 Raft 多数派确认后返回
- **SDK**：自动 Leader 重定向，集群拓扑对调用方透明

## 架构

```
Client SDK
    │  gRPC (KV / Watch / Lease)
    v
┌──────────────────────────────┐
│  KVServer / WatchServer      │
│  LeaseServer                 │
├──────────────────────────────┤
│  StateMachine (Apply path)   │
│  WatchManager / LeaseManager │
├──────────────────────────────┤
│  Raft  ←→  Raft  ←→  Raft    │  多节点共识
├──────────────────────────────┤
│  BadgerDB (ManagedDB)        │  持久化，版本号对应 revision
└──────────────────────────────┘
```

## 快速开始

```bash
make build
```

在三个终端分别启动：

```bash
PEERS="1=127.0.0.1:12001,2=127.0.0.1:12002,3=127.0.0.1:12003"

./bin/rkv -id=1 -peers=$PEERS -raft-addr=127.0.0.1:12001 -grpc-addr=127.0.0.1:13001 -data-dir=/tmp/rkv1/data -raft-dir=/tmp/rkv1/raft
./bin/rkv -id=2 -peers=$PEERS -raft-addr=127.0.0.1:12002 -grpc-addr=127.0.0.1:13002 -data-dir=/tmp/rkv2/data -raft-dir=/tmp/rkv2/raft
./bin/rkv -id=3 -peers=$PEERS -raft-addr=127.0.0.1:12003 -grpc-addr=127.0.0.1:13003 -data-dir=/tmp/rkv3/data -raft-dir=/tmp/rkv3/raft
```

## SDK 用法

```go
import "github.com/DecarbonizedGlucose/rkv/pkg/client"

cli, _ := client.NewClient(client.DefaultOptions().WithPeers(map[uint64]string{
    1: "127.0.0.1:13001",
    2: "127.0.0.1:13002",
    3: "127.0.0.1:13003",
}))
defer cli.Close()

// KV
cli.Put(ctx, []byte("key"), []byte("value"), 0)
cli.Get(ctx, []byte("key"))

// Watch
ch := cli.Watch(ctx, []byte("/services/"), true, 0)
for resp := range ch {
    for _, ev := range resp.Events { ... }
}

// 服务注册 (Lease + KeepAlive)
reg, _ := cli.Register(ctx, "user-svc", "node-1", "192.168.1.10:8080", 10)
defer reg.Deregister(ctx)

// 服务发现
initial, watchCh, _ := cli.WatchService(ctx, "user-svc")
```

## 包结构

| 包 | 说明 |
|---|---|
| `pkg/raft` | Raft 核心：选举、日志复制、心跳 |
| `pkg/raftstore` | Raft 节点封装：propose、apply、快照 |
| `pkg/kv` | KV 状态机，gRPC KVServer |
| `pkg/watch` | WatchManager + WatchServer |
| `pkg/lease` | LeaseManager（TimeWheel）+ LeaseServer |
| `pkg/storage` | BadgerDB 存储层 |
| `pkg/client` | 客户端 SDK |

## 测试

```bash
make test-unit      # 单元测试
make test-node      # 单节点集成测试（需要 grpcurl）
make test-cluster   # 三节点集群测试
```
