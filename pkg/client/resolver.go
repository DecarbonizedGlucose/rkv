package client

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/DecarbonizedGlucose/rkv/api/proto/pkg/kvpb"
)

// Discover 返回指定服务名当前所有存活的实例列表（快照）。
func (c *Client) Discover(ctx context.Context, name string) ([]Endpoint, error) {
	resp, err := c.RangePrefix(ctx, servicePrefix(name))
	if err != nil {
		return nil, err
	}
	eps := make([]Endpoint, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var ep Endpoint
		if err := json.Unmarshal(kv.Value, &ep); err == nil {
			eps = append(eps, ep)
		}
	}
	return eps, nil
}

// WatchService 返回初始快照及一个实时变更 channel。
// 每当有实例注册或注销时，channel 收到最新的完整实例列表。
// 调用方 cancel ctx 即可停止。
func (c *Client) WatchService(ctx context.Context, name string) ([]Endpoint, <-chan []Endpoint, error) {
	initial, err := c.Discover(ctx, name)
	if err != nil {
		return nil, nil, err
	}

	watchCh := c.Watch(ctx, servicePrefix(name), true, 0)

	ch := make(chan []Endpoint, 8)
	go func() {
		defer close(ch)

		local := make(map[string]Endpoint, len(initial))
		for _, ep := range initial {
			local[ep.ID] = ep
		}

		for resp := range watchCh {
			if resp.Canceled {
				return
			}
			changed := false
			for _, ev := range resp.Events {
				switch ev.Type {
				case kvpb.EventType_PUT:
					if ev.Kv == nil {
						continue
					}
					var ep Endpoint
					if err := json.Unmarshal(ev.Kv.Value, &ep); err == nil {
						local[ep.ID] = ep
						changed = true
					}
				case kvpb.EventType_DELETE:
					// DELETE 事件 Event.Kv 为 nil，key 在 Event.PrevKv 中。
					if ev.PrevKv != nil {
						id := extractServiceID(ev.PrevKv.Key)
						if id != "" {
							delete(local, id)
							changed = true
						}
					}
				}
			}
			if !changed {
				continue
			}
			snap := make([]Endpoint, 0, len(local))
			for _, ep := range local {
				snap = append(snap, ep)
			}
			select {
			case ch <- snap:
			case <-ctx.Done():
				return
			}
		}
	}()

	return initial, ch, nil
}

func servicePrefix(name string) []byte {
	return fmt.Appendf(nil, "/services/%s/", name)
}

func extractServiceID(key []byte) string {
	s := string(key)
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == '/' {
			return s[i+1:]
		}
	}
	return ""
}
