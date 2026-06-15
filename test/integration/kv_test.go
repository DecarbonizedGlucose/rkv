package integration

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DecarbonizedGlucose/rkv/api/proto/pkg/kvpb"
	"github.com/DecarbonizedGlucose/rkv/api/proto/pkg/rpcpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const addr = "127.0.0.1:13001"

var (
	client  rpcpb.KVServiceClient
	seq     atomic.Uint64
)

func TestMain(m *testing.M) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	client = rpcpb.NewKVServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for {
		resp, err := client.Get(ctx, &kvpb.GetRequest{Key: []byte("__probe__")})
		if err == nil && resp.Header != nil {
			break
		}
		select {
		case <-ctx.Done():
			panic("server not ready within 10s")
		case <-time.After(300 * time.Millisecond):
		}
	}

	m.Run()
	conn.Close()
}

func uniq(prefix string) []byte {
	return []byte(fmt.Sprintf("%s-%d", prefix, seq.Add(1)))
}

func TestPutGet(t *testing.T) {
	k := uniq("k")

	putResp, err := client.Put(context.Background(), &kvpb.PutRequest{Key: k, Value: []byte("v1")})
	require.NoError(t, err)
	assert.Greater(t, putResp.Header.Revision, uint64(0))

	getResp, err := client.Get(context.Background(), &kvpb.GetRequest{Key: k})
	require.NoError(t, err)
	require.NotNil(t, getResp.Kv)
	assert.Equal(t, k, getResp.Kv.Key)
	assert.Equal(t, []byte("v1"), getResp.Kv.Value)
	assert.Equal(t, getResp.Kv.CreateRevision, getResp.Kv.ModRevision)
	assert.GreaterOrEqual(t, getResp.Kv.Revision, getResp.Kv.ModRevision)
	assert.Equal(t, int64(1), getResp.Count)
}

func TestPutOverride(t *testing.T) {
	k := uniq("k")

	_, err := client.Put(context.Background(), &kvpb.PutRequest{Key: k, Value: []byte("v1")})
	require.NoError(t, err)

	_, err = client.Put(context.Background(), &kvpb.PutRequest{Key: k, Value: []byte("v2")})
	require.NoError(t, err)

	getResp, err := client.Get(context.Background(), &kvpb.GetRequest{Key: k})
	require.NoError(t, err)
	require.NotNil(t, getResp.Kv)
	assert.Equal(t, []byte("v2"), getResp.Kv.Value)
	assert.Less(t, getResp.Kv.CreateRevision, getResp.Kv.ModRevision)
	assert.Equal(t, int64(1), getResp.Count)
}

func TestPutPrevKV(t *testing.T) {
	k := uniq("k")

	_, err := client.Put(context.Background(), &kvpb.PutRequest{Key: k, Value: []byte("v1")})
	require.NoError(t, err)

	putResp, err := client.Put(context.Background(), &kvpb.PutRequest{
		Key:    k,
		Value:  []byte("v2"),
		PrevKv: 1,
	})
	require.NoError(t, err)
	require.NotNil(t, putResp.PrevKv)
	assert.Equal(t, []byte("v1"), putResp.PrevKv.Value)
}

func TestPutPrevKVOnNewKey(t *testing.T) {
	k := uniq("nk")

	putResp, err := client.Put(context.Background(), &kvpb.PutRequest{
		Key:    k,
		Value:  []byte("nv"),
		PrevKv: 1,
	})
	require.NoError(t, err)
	assert.Nil(t, putResp.PrevKv)
}

func TestDeleteExisting(t *testing.T) {
	k := uniq("d")

	_, err := client.Put(context.Background(), &kvpb.PutRequest{Key: k, Value: []byte("x")})
	require.NoError(t, err)

	delResp, err := client.Delete(context.Background(), &kvpb.DeleteRequest{Key: k})
	require.NoError(t, err)
	assert.Equal(t, int64(1), delResp.Deleted)

	getResp, err := client.Get(context.Background(), &kvpb.GetRequest{Key: k})
	require.NoError(t, err)
	assert.Nil(t, getResp.Kv)
	assert.Equal(t, int64(0), getResp.Count)
}

func TestDeleteNonExisting(t *testing.T) {
	k := uniq("ghost")

	delResp, err := client.Delete(context.Background(), &kvpb.DeleteRequest{Key: k})
	require.NoError(t, err)
	assert.Equal(t, int64(0), delResp.Deleted)
}

func TestDeletePrevKV(t *testing.T) {
	k := uniq("td")

	_, err := client.Put(context.Background(), &kvpb.PutRequest{Key: k, Value: []byte("bye")})
	require.NoError(t, err)

	delResp, err := client.Delete(context.Background(), &kvpb.DeleteRequest{
		Key:    k,
		PrevKv: 1,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1), delResp.Deleted)
	require.NotNil(t, delResp.PrevKv)
	assert.Equal(t, []byte("bye"), delResp.PrevKv.Value)
}

func TestRangeAll(t *testing.T) {
	pfx := uniq("r-")

	_, err := client.Put(context.Background(), &kvpb.PutRequest{Key: []byte(string(pfx) + "a"), Value: []byte("va")})
	require.NoError(t, err)
	_, err = client.Put(context.Background(), &kvpb.PutRequest{Key: []byte(string(pfx) + "b"), Value: []byte("vb")})
	require.NoError(t, err)
	_, err = client.Put(context.Background(), &kvpb.PutRequest{Key: []byte(string(pfx) + "c"), Value: []byte("vc")})
	require.NoError(t, err)

	rangeResp, err := client.Range(context.Background(), &kvpb.RangeRequest{RangeStart: pfx})
	require.NoError(t, err)
	assert.Equal(t, int64(3), rangeResp.Count)
	assert.False(t, rangeResp.More)
}

func TestRangeBounded(t *testing.T) {
	pfx := string(uniq("rb-"))

	_, err := client.Put(context.Background(), &kvpb.PutRequest{Key: []byte(pfx + "a"), Value: []byte("va")})
	require.NoError(t, err)
	_, err = client.Put(context.Background(), &kvpb.PutRequest{Key: []byte(pfx + "b"), Value: []byte("vb")})
	require.NoError(t, err)

	rangeResp, err := client.Range(context.Background(), &kvpb.RangeRequest{
		RangeStart: []byte(pfx + "a"),
		RangeEnd:   []byte(pfx + "c"),
	})
	require.NoError(t, err)
	assert.Equal(t, int64(2), rangeResp.Count)
	assert.Len(t, rangeResp.Kvs, 2)
}

func TestRangeLimit(t *testing.T) {
	pfx := string(uniq("rl-"))

	_, err := client.Put(context.Background(), &kvpb.PutRequest{Key: []byte(pfx + "a"), Value: []byte("va")})
	require.NoError(t, err)
	_, err = client.Put(context.Background(), &kvpb.PutRequest{Key: []byte(pfx + "b"), Value: []byte("vb")})
	require.NoError(t, err)
	_, err = client.Put(context.Background(), &kvpb.PutRequest{Key: []byte(pfx + "c"), Value: []byte("vc")})
	require.NoError(t, err)

	rangeResp, err := client.Range(context.Background(), &kvpb.RangeRequest{
		RangeStart: []byte(pfx),
		Limit:      2,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(2), rangeResp.Count)
	assert.True(t, rangeResp.More)
}

func TestRangeExactLimit(t *testing.T) {
	pfx := string(uniq("rel-"))

	_, err := client.Put(context.Background(), &kvpb.PutRequest{Key: []byte(pfx + "a"), Value: []byte("va")})
	require.NoError(t, err)
	_, err = client.Put(context.Background(), &kvpb.PutRequest{Key: []byte(pfx + "b"), Value: []byte("vb")})
	require.NoError(t, err)
	_, err = client.Put(context.Background(), &kvpb.PutRequest{Key: []byte(pfx + "c"), Value: []byte("vc")})
	require.NoError(t, err)

	rangeResp, err := client.Range(context.Background(), &kvpb.RangeRequest{
		RangeStart: []byte(pfx),
		RangeEnd:   []byte(pfx + "\xff"),
		Limit:      3,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(3), rangeResp.Count)
	assert.False(t, rangeResp.More)
}

func TestRangeEmpty(t *testing.T) {
	rangeResp, err := client.Range(context.Background(), &kvpb.RangeRequest{
		RangeStart: []byte("zzzzzz-no-such-prefix"),
	})
	require.NoError(t, err)
	assert.Equal(t, int64(0), rangeResp.Count)
	assert.Len(t, rangeResp.Kvs, 0)
}

func TestRevisionMonotonic(t *testing.T) {
	k := uniq("mono")
	var last uint64
	for i := 0; i < 5; i++ {
		putResp, err := client.Put(context.Background(), &kvpb.PutRequest{
			Key:   k,
			Value: []byte{byte(i)},
		})
		require.NoError(t, err)
		assert.Greater(t, putResp.Header.Revision, last)
		last = putResp.Header.Revision
	}
}

func TestTxnCASSuccess(t *testing.T) {
	k := uniq("cas")

	_, err := client.Put(context.Background(), &kvpb.PutRequest{Key: k, Value: []byte("v1")})
	require.NoError(t, err)

	getResp, err := client.Get(context.Background(), &kvpb.GetRequest{Key: k})
	require.NoError(t, err)
	require.NotNil(t, getResp.Kv)
	expectedRev := getResp.Kv.ModRevision

	txnResp, err := client.Txn(context.Background(), &kvpb.TxnRequest{
		Compares: []*kvpb.Compare{{
			Key:         k,
			Target:      kvpb.Compare_VERSION,
			Result:      kvpb.Compare_EQUAL,
			TargetValue: &kvpb.Compare_Version{Version: expectedRev},
		}},
		Success: []*kvpb.RequestOp{{
			Request: &kvpb.RequestOp_Put{
				Put: &kvpb.PutRequest{Key: k, Value: []byte("v2")},
			},
		}},
	})
	require.NoError(t, err)
	assert.True(t, txnResp.Succeeded)

	getResp, err = client.Get(context.Background(), &kvpb.GetRequest{Key: k})
	require.NoError(t, err)
	require.NotNil(t, getResp.Kv)
	assert.Equal(t, []byte("v2"), getResp.Kv.Value)
}

func TestTxnCASFailure(t *testing.T) {
	k := uniq("casf")

	_, err := client.Put(context.Background(), &kvpb.PutRequest{Key: k, Value: []byte("v3")})
	require.NoError(t, err)

	getResp, err := client.Get(context.Background(), &kvpb.GetRequest{Key: k})
	require.NoError(t, err)
	require.NotNil(t, getResp.Kv)
	actualRev := getResp.Kv.ModRevision

	marker := uniq("casf-marker")

	txnResp, err := client.Txn(context.Background(), &kvpb.TxnRequest{
		Compares: []*kvpb.Compare{{
			Key:         k,
			Target:      kvpb.Compare_VERSION,
			Result:      kvpb.Compare_EQUAL,
			TargetValue: &kvpb.Compare_Version{Version: actualRev + 99},
		}},
		Failure: []*kvpb.RequestOp{{
			Request: &kvpb.RequestOp_Put{
				Put: &kvpb.PutRequest{Key: marker, Value: []byte("fail")},
			},
		}},
	})
	require.NoError(t, err)
	assert.False(t, txnResp.Succeeded)

	getResp, err = client.Get(context.Background(), &kvpb.GetRequest{Key: k})
	require.NoError(t, err)
	require.NotNil(t, getResp.Kv)
	assert.Equal(t, []byte("v3"), getResp.Kv.Value)

	getResp2, err := client.Get(context.Background(), &kvpb.GetRequest{Key: marker})
	require.NoError(t, err)
	require.NotNil(t, getResp2.Kv)
	assert.Equal(t, []byte("fail"), getResp2.Kv.Value)
}

func TestTxnMultiCompareAND(t *testing.T) {
	ka := uniq("and-a")
	kb := uniq("and-b")

	_, err := client.Put(context.Background(), &kvpb.PutRequest{Key: ka, Value: []byte("va")})
	require.NoError(t, err)
	_, err = client.Put(context.Background(), &kvpb.PutRequest{Key: kb, Value: []byte("vb")})
	require.NoError(t, err)

	ra, err := client.Get(context.Background(), &kvpb.GetRequest{Key: ka})
	require.NoError(t, err)
	require.NotNil(t, ra.Kv)
	rb, err := client.Get(context.Background(), &kvpb.GetRequest{Key: kb})
	require.NoError(t, err)
	require.NotNil(t, rb.Kv)

	ok := uniq("and-ok")

	txnResp, err := client.Txn(context.Background(), &kvpb.TxnRequest{
		Compares: []*kvpb.Compare{
			{Key: ka, Target: kvpb.Compare_VERSION, Result: kvpb.Compare_EQUAL,
				TargetValue: &kvpb.Compare_Version{Version: ra.Kv.ModRevision}},
			{Key: kb, Target: kvpb.Compare_VERSION, Result: kvpb.Compare_EQUAL,
				TargetValue: &kvpb.Compare_Version{Version: rb.Kv.ModRevision}},
		},
		Success: []*kvpb.RequestOp{{
			Request: &kvpb.RequestOp_Put{Put: &kvpb.PutRequest{Key: ok, Value: []byte("ok")}},
		}},
	})
	require.NoError(t, err)
	assert.True(t, txnResp.Succeeded)
}

func TestTxnCompareCREATE(t *testing.T) {
	k := uniq("cc")

	_, err := client.Put(context.Background(), &kvpb.PutRequest{Key: k, Value: []byte("v")})
	require.NoError(t, err)

	getResp, err := client.Get(context.Background(), &kvpb.GetRequest{Key: k})
	require.NoError(t, err)
	require.NotNil(t, getResp.Kv)

	ok := uniq("cc-ok")

	txnResp, err := client.Txn(context.Background(), &kvpb.TxnRequest{
		Compares: []*kvpb.Compare{{
			Key:         k,
			Target:      kvpb.Compare_CREATE,
			Result:      kvpb.Compare_EQUAL,
			TargetValue: &kvpb.Compare_Version{Version: getResp.Kv.CreateRevision},
		}},
		Success: []*kvpb.RequestOp{{
			Request: &kvpb.RequestOp_Put{Put: &kvpb.PutRequest{Key: ok, Value: []byte("ok")}},
		}},
	})
	require.NoError(t, err)
	assert.True(t, txnResp.Succeeded)
}

func TestTxnCompareVALUE(t *testing.T) {
	k := uniq("vc")

	_, err := client.Put(context.Background(), &kvpb.PutRequest{Key: k, Value: []byte("xyz")})
	require.NoError(t, err)

	ok := uniq("vc-ok")

	txnResp, err := client.Txn(context.Background(), &kvpb.TxnRequest{
		Compares: []*kvpb.Compare{{
			Key:         k,
			Target:      kvpb.Compare_VALUE,
			Result:      kvpb.Compare_EQUAL,
			TargetValue: &kvpb.Compare_Value{Value: []byte("xyz")},
		}},
		Success: []*kvpb.RequestOp{{
			Request: &kvpb.RequestOp_Put{Put: &kvpb.PutRequest{Key: ok, Value: []byte("ok")}},
		}},
	})
	require.NoError(t, err)
	assert.True(t, txnResp.Succeeded)
}

func TestResponseHeader(t *testing.T) {
	putResp, err := client.Put(context.Background(), &kvpb.PutRequest{
		Key:   uniq("hdr"),
		Value: []byte("x"),
	})
	require.NoError(t, err)
	assert.NotZero(t, putResp.Header.MemberId)
	assert.Greater(t, putResp.Header.Revision, uint64(0))
	assert.Greater(t, putResp.Header.RaftTerm, uint64(0))
}
