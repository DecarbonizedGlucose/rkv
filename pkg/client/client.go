package client

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/DecarbonizedGlucose/rkv/api/proto/pkg/rpcpb"
)

// Client 封装对 rkv 集群的 gRPC 连接，提供 KV、Watch、Lease 及服务注册发现操作。
// 通过 New 创建；并发安全。
type Client struct {
	mu        sync.RWMutex
	conn      *grpc.ClientConn
	kvStub    rpcpb.KVServiceClient
	watchStub rpcpb.WatchServiceClient
	leaseStub rpcpb.LeaseServiceClient

	peers    map[uint64]string // nodeID -> gRPC addr；nil 表示单节点模式，构造后不可变
	dialOpts []grpc.DialOption // 构造后不可变

	allowFollowerRead bool
}

// NewClient 创建 rkv 客户端。
//
// 仅 WithEndpoint(或默认)：单节点模式，收到 ErrNotLeader 直接返回错误。
//
// 含 WithPeers：集群模式，收到 ErrNotLeader 时自动切换到 Leader 节点重试一次。
func NewClient(o *Options) (*Client, error) {
	dialOpts := o.grpcOpts
	if !o.hasCreds {
		dialOpts = append([]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, dialOpts...)
	}

	if len(o.peers) > 0 {
		var (
			conn    *grpc.ClientConn
			lastErr error
		)
		for _, addr := range o.peers {
			conn, lastErr = grpc.NewClient(addr, dialOpts...)
			if lastErr == nil {
				break
			}
		}
		if conn == nil {
			return nil, fmt.Errorf("rkv: all peers unavailable: %w", lastErr)
		}
		return newClient(conn, o.peers, dialOpts, o.allowFollowerRead), nil
	}

	conn, err := grpc.NewClient(o.endpoint, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("rkv: dial %s: %w", o.endpoint, err)
	}
	return newClient(conn, nil, dialOpts, o.allowFollowerRead), nil
}

// Close 关闭底层 gRPC 连接。
func (c *Client) Close() error {
	c.mu.Lock()
	conn := c.conn
	c.conn = nil
	c.mu.Unlock()
	if conn != nil {
		return conn.Close()
	}
	return nil
}

// ==================== Internal ====================

func (c *Client) getKVStub() rpcpb.KVServiceClient {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.kvStub
}

func (c *Client) getWatchStub() rpcpb.WatchServiceClient {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.watchStub
}

func (c *Client) getLeaseStub() rpcpb.LeaseServiceClient {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.leaseStub
}

// switchTo 切换到新节点：建立连接、替换三个 stub、关闭旧连接。持写锁。
func (c *Client) switchTo(addr string) error {
	conn, err := grpc.NewClient(addr, c.dialOpts...)
	if err != nil {
		return err
	}
	c.mu.Lock()
	old := c.conn
	c.conn = conn
	c.kvStub = rpcpb.NewKVServiceClient(conn)
	c.watchStub = rpcpb.NewWatchServiceClient(conn)
	c.leaseStub = rpcpb.NewLeaseServiceClient(conn)
	c.mu.Unlock()

	if old != nil {
		old.Close()
	}
	return nil
}

// tryFollowLeader 从 trailer 解析 leader-id，切换到 Leader 节点。
// 仅集群模式生效；成功返回 true。
func (c *Client) tryFollowLeader(trailer metadata.MD) bool {
	if c.peers == nil {
		return false
	}
	vals := trailer.Get("leader-id")
	if len(vals) == 0 {
		return false
	}
	leaderID, err := strconv.ParseUint(vals[0], 10, 64)
	if err != nil || leaderID == 0 {
		return false
	}
	addr, ok := c.peers[leaderID]
	if !ok {
		return false
	}
	return c.switchTo(addr) == nil
}

// callUnary 执行一元 gRPC 调用并捕获 trailer。
//
// 集群模式下，如果收到 ErrNotLeader，通过 trailer 中的 leader-id 切换到 Leader 重试一次。
// 如果收到 ErrUnavailable（集群模式），当前节点不可达，遍历所有 peer 直到找到可用节点。
//
// 单点模式下，直接返回错误，不进行重试。
func callUnary[Req, Resp any](
	c *Client,
	ctx context.Context,
	req Req,
	fn func(context.Context, Req, ...grpc.CallOption) (Resp, error),
) (Resp, error) {
	var trailer metadata.MD
	resp, err := fn(ctx, req, grpc.Trailer(&trailer))
	if err == nil {
		return resp, nil
	}

	translated := translateErr(err)

	if errors.Is(translated, ErrNotLeader) && c.tryFollowLeader(trailer) {
		resp, err = fn(ctx, req)
		return resp, translateErr(err)
	}

	// 集群模式, 当前节点不可达时，遍历所有 peer 寻找存活节点
	if errors.Is(translated, ErrUnavailable) && c.peers != nil {
		for _, addr := range c.peers {
			if c.switchTo(addr) != nil {
				continue
			}
			var t2 metadata.MD
			resp, err = fn(ctx, req, grpc.Trailer(&t2))
			if err == nil {
				return resp, nil
			}
			if errors.Is(translateErr(err), ErrNotLeader) && c.tryFollowLeader(t2) {
				resp, err = fn(ctx, req)
				return resp, translateErr(err)
			}
		}
	}

	return resp, translated
}

func newClient(conn *grpc.ClientConn, peers map[uint64]string, opts []grpc.DialOption, allowFollowerRead bool) *Client {
	return &Client{
		conn:              conn,
		kvStub:            rpcpb.NewKVServiceClient(conn),
		watchStub:         rpcpb.NewWatchServiceClient(conn),
		leaseStub:         rpcpb.NewLeaseServiceClient(conn),
		peers:             peers,
		dialOpts:          opts,
		allowFollowerRead: allowFollowerRead,
	}
}
