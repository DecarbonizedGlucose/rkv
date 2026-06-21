package client

import (
	"crypto/tls"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

const (
	DefaultEndpoint = "127.0.0.1:2379"
)

// Options 收集 NewClient 的所有可选参数。
type Options struct {
	endpoint string
	peers    map[uint64]string
	grpcOpts []grpc.DialOption
	hasCreds bool // WithTLS 已设置 transport credentials，不需要再注入 insecure
}

func DefaultOptions() *Options {
	return &Options{
		endpoint: DefaultEndpoint,
	}
}

// WithEndpoint 指定单节点 gRPC 地址。
// 与 WithPeers 互斥；若两者同时传入，WithPeers 优先。
func (o *Options) WithEndpoint(addr string) *Options {
	o.endpoint = addr
	return o
}

// WithPeers 指定集群节点表 (nodeID -> gRPC addr)。
// 设置后 Client 在收到 ErrNotLeader 时自动跟随 Leader 重试。
func (o *Options) WithPeers(peers map[uint64]string) *Options {
	o.peers = peers
	return o
}

// WithTLS 启用 TLS 传输加密。
func (o *Options) WithTLS(cfg *tls.Config) *Options {
	o.grpcOpts = append(o.grpcOpts, grpc.WithTransportCredentials(credentials.NewTLS(cfg)))
	o.hasCreds = true
	return o
}

// WithGRPCOptions 追加底层 gRPC DialOption，用于细粒度控制。
func (o *Options) WithGRPCOptions(opts ...grpc.DialOption) *Options {
	o.grpcOpts = append(o.grpcOpts, opts...)
	return o
}
