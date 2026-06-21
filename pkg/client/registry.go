package client

import (
	"context"
	"encoding/json"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Endpoint 表示一个已注册的服务实例。
type Endpoint struct {
	Name    string `json:"name"`
	ID      string `json:"id"`
	Address string `json:"addr"`
}

// Registration 表示一次已完成的服务注册，负责维护续约并支持注销。
type Registration struct {
	c       *Client
	leaseID int64
	key     []byte
	cancel  context.CancelFunc
}

// Register 向 rkv 注册一个服务实例。
//
// name: 服务名；id: 实例唯一标识, 同一 name 下须唯一；
// addr: 服务地址 (host:port)；ttl: 租约 TTL。
//
// 返回 Registration 句柄，调用 Deregister 可立即注销。
func (c *Client) Register(ctx context.Context, name, id, addr string, ttl int64) (*Registration, error) {
	resp, err := c.LeaseGrant(ctx, ttl)
	if err != nil {
		return nil, fmt.Errorf("registry: grant lease: %w", err)
	}
	leaseID := resp.Id

	ep := Endpoint{Name: name, ID: id, Address: addr}
	val, _ := json.Marshal(ep)
	key := serviceKey(name, id)
	if _, err := c.Put(ctx, key, val, leaseID); err != nil {
		_ = c.LeaseRevoke(ctx, leaseID)
		return nil, fmt.Errorf("registry: put key: %w", err)
	}

	kaCtx, cancel := context.WithCancel(context.Background())
	if _, err = c.LeaseKeepAlive(kaCtx, leaseID, ttl); err != nil {
		cancel()
		_ = c.LeaseRevoke(ctx, leaseID)
		return nil, fmt.Errorf("registry: keepalive: %w", err)
	}

	return &Registration{c: c, leaseID: leaseID, key: key, cancel: cancel}, nil
}

// Deregister 停止续约并撤销租约，关联 key 立即从集群删除。
// 若租约已因 TTL 过期被撤销，视为成功。
func (r *Registration) Deregister(ctx context.Context) error {
	r.cancel()
	err := r.c.LeaseRevoke(ctx, r.leaseID)
	if status.Code(err) == codes.NotFound {
		return nil
	}
	return err
}

// LeaseID 返回当前注册对应的租约 ID。
func (r *Registration) LeaseID() int64 {
	return r.leaseID
}

func serviceKey(name, id string) []byte {
	return fmt.Appendf(nil, "/services/%s/%s", name, id)
}
