package client

import (
	"context"
	"time"

	"google.golang.org/grpc"

	"github.com/DecarbonizedGlucose/rkv/api/proto/pkg/kvpb"
)

// LeaseGrant 申请一个 TTL 为 ttl 秒的租约，id=0 由服务端自动分配。
func (c *Client) LeaseGrant(ctx context.Context, ttl int64) (*kvpb.LeaseGrantResponse, error) {
	return callUnary(c, ctx, &kvpb.LeaseGrantRequest{Ttl: ttl},
		func(ctx context.Context, req *kvpb.LeaseGrantRequest, opts ...grpc.CallOption) (*kvpb.LeaseGrantResponse, error) {
			return c.getLeaseStub().LeaseGrant(ctx, req, opts...)
		},
	)
}

// LeaseRevoke 撤销指定租约，关联的 key 将被删除。
func (c *Client) LeaseRevoke(ctx context.Context, id int64) error {
	type voidResp = kvpb.LeaseRevokeResponse
	_, err := callUnary(c, ctx, &kvpb.LeaseRevokeRequest{Id: id},
		func(ctx context.Context, req *kvpb.LeaseRevokeRequest, opts ...grpc.CallOption) (*voidResp, error) {
			return c.getLeaseStub().LeaseRevoke(ctx, req, opts...)
		},
	)
	return err
}

// LeaseKeepAlive 启动后台续约 goroutine，每 ttl/3 秒发送一次 KeepAlive 请求。
// 返回接收续约响应的 channel；ctx 取消或流异常时 channel 关闭。
//
// 调用方若需感知续约失败，应监控 channel 是否意外关闭。
func (c *Client) LeaseKeepAlive(ctx context.Context, id, ttl int64) (<-chan *kvpb.LeaseKeepAliveResponse, error) {
	stream, err := c.getLeaseStub().LeaseKeepAlive(ctx)
	if err != nil {
		return nil, translateErr(err)
	}

	interval := keepAliveInterval(ttl)
	ch := make(chan *kvpb.LeaseKeepAliveResponse, 8)

	go func() {
		defer close(ch)
		defer stream.CloseSend()

		recvCh := make(chan *kvpb.LeaseKeepAliveResponse, 8)
		recvDone := make(chan struct{})
		go func() {
			defer close(recvDone)
			for {
				resp, err := stream.Recv()
				if err != nil {
					return
				}
				select {
				case recvCh <- resp:
				case <-ctx.Done():
					return
				}
			}
		}()

		if err := stream.Send(&kvpb.LeaseKeepAliveRequest{Id: id}); err != nil {
			return
		}

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-recvDone:
				return
			case <-ticker.C:
				if err := stream.Send(&kvpb.LeaseKeepAliveRequest{Id: id}); err != nil {
					return
				}
			case resp := <-recvCh:
				select {
				case ch <- resp:
				default:
				}
				if newIntvl := keepAliveInterval(resp.Ttl); newIntvl != interval {
					interval = newIntvl
					ticker.Reset(interval)
				}
			}
		}
	}()

	return ch, nil
}

func keepAliveInterval(ttl int64) time.Duration {
	if ttl <= 3 {
		return time.Second
	}
	return time.Duration(ttl/3) * time.Second
}
