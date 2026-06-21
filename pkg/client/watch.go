package client

import (
	"context"
	"io"

	"github.com/DecarbonizedGlucose/rkv/api/proto/pkg/kvpb"
)

// WatchChan 接收 Watch 流返回的响应批次。ctx 取消时 channel 关闭。
type WatchChan <-chan *kvpb.WatchResponse

// Watch 订阅 key（或前缀）的变更事件。
//
// prefix=true 时匹配所有以 key 为前缀的 key。
// startRev=0 从当前 revision 开始，>0 从指定 revision 回放历史。
//
// 调用方 cancel ctx 即可停止 Watch。
func (c *Client) Watch(ctx context.Context, key []byte, prefix bool, startRev uint64) WatchChan {
	ch := make(chan *kvpb.WatchResponse, 64)
	go func() {
		defer close(ch)
		stream, err := c.getWatchStub().Watch(ctx, &kvpb.WatchRequest{
			Key:           key,
			Prefix:        prefix,
			StartRevision: startRev,
		})
		if err != nil {
			return
		}
		for {
			resp, err := stream.Recv()
			if err == io.EOF || ctx.Err() != nil {
				return
			}
			if err != nil {
				return
			}
			select {
			case ch <- resp:
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch
}
