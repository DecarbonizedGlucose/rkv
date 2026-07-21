# rkv

一个用 Go 实现的 Raft KV 项目，目前用于学习和实验，不建议用于生产环境。

## 已实现

- Raft 选举、日志复制、故障切换、快照和日志压缩
- BadgerDB 持久化，业务数据与 Raft 数据分目录保存
- Put、Get、Delete、Range 和事务（CAS）
- Watch、Lease 和 KeepAlive
- 简单的服务注册/发现客户端封装
- Leader Lease 读
- 可选的 Follower ReadIndex 读
- 客户端 Leader 重定向

## 构建

```bash
make build
```

## 启动三节点集群

在每个终端分别设置：

```bash
export PEERS="1=127.0.0.1:12001,2=127.0.0.1:12002,3=127.0.0.1:12003"
```

终端 1：

```bash
./bin/rkv -id=1 -peers="$PEERS" \
  -raft-addr=127.0.0.1:12001 -grpc-addr=127.0.0.1:13001 \
  -data-dir=/tmp/rkv1/data -raft-dir=/tmp/rkv1/raft
```

终端 2：

```bash
./bin/rkv -id=2 -peers="$PEERS" \
  -raft-addr=127.0.0.1:12002 -grpc-addr=127.0.0.1:13002 \
  -data-dir=/tmp/rkv2/data -raft-dir=/tmp/rkv2/raft
```

终端 3：

```bash
./bin/rkv -id=3 -peers="$PEERS" \
  -raft-addr=127.0.0.1:12003 -grpc-addr=127.0.0.1:13003 \
  -data-dir=/tmp/rkv3/data -raft-dir=/tmp/rkv3/raft
```

## 客户端

```go
cli, err := client.NewClient(client.DefaultOptions().WithPeers(map[uint64]string{
    1: "127.0.0.1:13001",
    2: "127.0.0.1:13002",
    3: "127.0.0.1:13003",
}))
if err != nil {
    return err
}
defer cli.Close()

_, err = cli.Put(ctx, []byte("key"), []byte("value"), 0)
resp, err := cli.Get(ctx, []byte("key"))
```

写请求只能由 Leader 处理。Get 和 Range 默认也只读 Leader。

启用 Follower ReadIndex 读时，所有服务节点增加：

```text
-allow-follower-read
```

客户端同时增加：

```go
client.DefaultOptions().WithFollowerRead(true)
```

## TLS

对外 gRPC TLS：

```text
-tls-cert=server.crt -tls-key=server.key
```

校验客户端证书时增加 `-tls-client-ca=ca.crt`。客户端使用 `WithTLS(*tls.Config)`。

Raft 节点间 mTLS：

```text
-raft-tls-cert=node.crt -raft-tls-key=node.key -raft-tls-ca=ca.crt
```

## 测试

```bash
make test-unit
make test-node      # 需要 grpcurl
make test-cluster
make check
```
