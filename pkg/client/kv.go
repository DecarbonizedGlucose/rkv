package client

import (
	"context"

	"google.golang.org/grpc"

	"github.com/DecarbonizedGlucose/rkv/api/proto/pkg/kvpb"
)

// Put 写入或更新 key-value，leaseID=0 表示不绑定租约。
func (c *Client) Put(ctx context.Context, key, value []byte, leaseID int64) (*kvpb.PutResponse, error) {
	return callUnary(c, ctx, &kvpb.PutRequest{Key: key, Value: value, Lease: leaseID},
		func(ctx context.Context, req *kvpb.PutRequest, opts ...grpc.CallOption) (*kvpb.PutResponse, error) {
			return c.getKVStub().Put(ctx, req, opts...)
		},
	)
}

// Get 查询单个 key。kv 为 nil 且 count=0 表示 key 不存在。
func (c *Client) Get(ctx context.Context, key []byte) (*kvpb.GetResponse, error) {
	return callUnary(c, ctx, &kvpb.GetRequest{Key: key},
		func(ctx context.Context, req *kvpb.GetRequest, opts ...grpc.CallOption) (*kvpb.GetResponse, error) {
			return c.getKVStub().Get(ctx, req, opts...)
		},
	)
}

// Delete 删除单个 key。
func (c *Client) Delete(ctx context.Context, key []byte) (*kvpb.DeleteResponse, error) {
	return callUnary(c, ctx, &kvpb.DeleteRequest{Key: key},
		func(ctx context.Context, req *kvpb.DeleteRequest, opts ...grpc.CallOption) (*kvpb.DeleteResponse, error) {
			return c.getKVStub().Delete(ctx, req, opts...)
		},
	)
}

// Range 返回 [from, to) 范围内的 KV，limit=0 表示不限制数量。
func (c *Client) Range(ctx context.Context, from, to []byte, limit int64) (*kvpb.RangeResponse, error) {
	return callUnary(c, ctx, &kvpb.RangeRequest{RangeStart: from, RangeEnd: to, Limit: limit},
		func(ctx context.Context, req *kvpb.RangeRequest, opts ...grpc.CallOption) (*kvpb.RangeResponse, error) {
			return c.getKVStub().Range(ctx, req, opts...)
		},
	)
}

// RangePrefix 返回所有以 prefix 开头的 key。
func (c *Client) RangePrefix(ctx context.Context, prefix []byte) (*kvpb.RangeResponse, error) {
	return callUnary(c, ctx, &kvpb.RangeRequest{RangeStart: prefix, RangeEnd: prefixEnd(prefix)},
		func(ctx context.Context, req *kvpb.RangeRequest, opts ...grpc.CallOption) (*kvpb.RangeResponse, error) {
			return c.getKVStub().Range(ctx, req, opts...)
		},
	)
}

// Txn 执行事务。
func (c *Client) Txn(ctx context.Context, req *kvpb.TxnRequest) (*kvpb.TxnResponse, error) {
	return callUnary(c, ctx, req,
		func(ctx context.Context, req *kvpb.TxnRequest, opts ...grpc.CallOption) (*kvpb.TxnResponse, error) {
			return c.getKVStub().Txn(ctx, req, opts...)
		},
	)
}

// prefixEnd 返回字典序上第一个大于所有 prefix 开头 key 的边界值。
// prefix 全为 0xFF 时返回 nil（表示无上界）。
func prefixEnd(prefix []byte) []byte {
	end := make([]byte, len(prefix))
	copy(end, prefix)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}
