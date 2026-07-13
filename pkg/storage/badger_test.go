package storage_test

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/DecarbonizedGlucose/rkv/pkg/storage"
	"github.com/DecarbonizedGlucose/rkv/pkg/util"
	"github.com/DecarbonizedGlucose/rkv/test/testutil"
)

// 模拟 KV 层的 revision 计数
type revCounter struct {
	current uint64
}

func (r *revCounter) next() uint64 {
	r.current++
	return r.current
}

// key 列表，用于 Range 排序比较
type byKey []*util.InternalKV

func (b byKey) Len() int           { return len(b) }
func (b byKey) Less(i, j int) bool { return string(b[i].Key) < string(b[j].Key) }
func (b byKey) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }

// ========================================
// Put / Get
// ========================================

func TestBadgerPutGet(t *testing.T) {
	s, cleanup := testutil.NewTestStorage(t)
	defer cleanup()

	rev := revCounter{}

	_, err := s.Put([]byte("k1"), []byte("v1"), rev.next(), 0)
	require.NoError(t, err)

	ikv, err := s.Get([]byte("k1"), rev.next())
	require.NoError(t, err)
	assert.Equal(t, []byte("k1"), ikv.Key)
	assert.Equal(t, []byte("v1"), ikv.Value)
	assert.Equal(t, uint64(1), ikv.MRevision)
	assert.Equal(t, uint64(1), ikv.CRevision)
}

func TestBadgerGetKeyNotFound(t *testing.T) {
	s, cleanup := testutil.NewTestStorage(t)
	defer cleanup()

	_, err := s.Get([]byte("no_such_key"), 0)
	assert.ErrorIs(t, err, storage.ErrKeyNotFound)
}

func TestBadgerPutOverride(t *testing.T) {
	s, cleanup := testutil.NewTestStorage(t)
	defer cleanup()

	rev := revCounter{}

	// 首次写
	_, err := s.Put([]byte("k1"), []byte("v1"), rev.next(), 0)
	require.NoError(t, err)

	// 覆盖写
	_, err = s.Put([]byte("k1"), []byte("v2"), rev.next(), 0)
	require.NoError(t, err)

	ikv, err := s.Get([]byte("k1"), rev.next())
	require.NoError(t, err)

	assert.Equal(t, []byte("k1"), ikv.Key)
	assert.Equal(t, []byte("v2"), ikv.Value)
	// CRevision 不变，MRevision 更新为最后一次写入的 revision
	assert.Equal(t, uint64(1), ikv.CRevision)
	assert.Equal(t, uint64(2), ikv.MRevision)
}

func TestBadgerGetAtRevision(t *testing.T) {
	s, cleanup := testutil.NewTestStorage(t)
	defer cleanup()

	_, err := s.Put([]byte("k1"), []byte("v1"), 1, 0)
	require.NoError(t, err)
	_, err = s.Put([]byte("k1"), []byte("v2"), 2, 0)
	require.NoError(t, err)

	ikv, err := s.Get([]byte("k1"), 1)
	require.NoError(t, err)
	assert.Equal(t, []byte("v1"), ikv.Value)
}

func TestBadgerPutGetMultipleKeys(t *testing.T) {
	s, cleanup := testutil.NewTestStorage(t)
	defer cleanup()

	rev := revCounter{}
	keys := [][]byte{[]byte("a"), []byte("b"), []byte("c")}

	for _, k := range keys {
		_, err := s.Put(k, []byte("val_"+string(k)), rev.next(), 0)
		require.NoError(t, err)
	}

	for i, k := range keys {
		ikv, err := s.Get(k, rev.next())
		require.NoError(t, err)
		assert.Equal(t, k, ikv.Key)
		// 每条 key 的 revision 独立递增
		assert.Equal(t, uint64(i+1), ikv.MRevision)
	}
}

// ========================================
// PrevKV
// ========================================

func TestBadgerPutPrevKV(t *testing.T) {
	s, cleanup := testutil.NewTestStorage(t)
	defer cleanup()

	rev := revCounter{}

	// 首次写
	rev1 := rev.next()
	_, err := s.Put([]byte("k1"), []byte("v1"), rev1, 0)
	require.NoError(t, err)

	// 覆盖写，取 prev_kv
	rev2 := rev.next()
	prevKV, err := s.Put([]byte("k1"), []byte("v2"), rev2, 0)
	require.NoError(t, err)
	require.NotNil(t, prevKV)

	assert.Equal(t, []byte("k1"), prevKV.Key)
	assert.Equal(t, []byte("v1"), prevKV.Value)
	assert.Equal(t, uint64(1), prevKV.MRevision)
	assert.Equal(t, uint64(1), prevKV.CRevision)
}

func TestBadgerPutPrevKVNewKey(t *testing.T) {
	s, cleanup := testutil.NewTestStorage(t)
	defer cleanup()

	rev := revCounter{}

	// 新 key，prev_kv=true——ikv 应为 nil
	ikv, err := s.Put([]byte("new_key"), []byte("v"), rev.next(), 0)
	require.NoError(t, err)
	assert.Nil(t, ikv)
}

func TestBadgerDeletePrevKV(t *testing.T) {
	s, cleanup := testutil.NewTestStorage(t)
	defer cleanup()

	rev := revCounter{}

	rev1 := rev.next()
	_, err := s.Put([]byte("k1"), []byte("v1"), rev1, 0)
	require.NoError(t, err)

	rev2 := rev.next()
	prevKV, err := s.Delete([]byte("k1"), rev2)
	require.NoError(t, err)
	require.NotNil(t, prevKV)

	assert.Equal(t, []byte("k1"), prevKV.Key)
	assert.Equal(t, []byte("v1"), prevKV.Value)
	assert.Equal(t, uint64(1), prevKV.MRevision)
	assert.Equal(t, uint64(1), prevKV.CRevision)

	// 删除后 key 应不存在
	_, err = s.Get([]byte("k1"), rev.next())
	assert.ErrorIs(t, err, storage.ErrKeyNotFound)
}

// ========================================
// Delete
// ========================================

func TestBadgerDelete(t *testing.T) {
	s, cleanup := testutil.NewTestStorage(t)
	defer cleanup()

	rev := revCounter{}

	_, err := s.Put([]byte("k1"), []byte("v1"), rev.next(), 0)
	require.NoError(t, err)

	_, err = s.Delete([]byte("k1"), rev.next())
	require.NoError(t, err)

	_, err = s.Get([]byte("k1"), rev.next())
	assert.ErrorIs(t, err, storage.ErrKeyNotFound)
}

func TestBadgerDeleteKeyNotFound(t *testing.T) {
	s, cleanup := testutil.NewTestStorage(t)
	defer cleanup()

	_, err := s.Delete([]byte("no_such_key"), 1)
	assert.ErrorIs(t, err, storage.ErrKeyNotFound)
}

// ========================================
// Lease
// ========================================

func TestBadgerLeaseID(t *testing.T) {
	s, cleanup := testutil.NewTestStorage(t)
	defer cleanup()

	rev := revCounter{}

	_, err := s.Put([]byte("k1"), []byte("v1"), rev.next(), 42)
	require.NoError(t, err)

	ikv, err := s.Get([]byte("k1"), rev.next())
	require.NoError(t, err)

	assert.Equal(t, int64(42), ikv.LeaseID)
}

func TestBadgerLeaseOverride(t *testing.T) {
	s, cleanup := testutil.NewTestStorage(t)
	defer cleanup()

	rev := revCounter{}

	// 首次写，指定 lease=1
	_, err := s.Put([]byte("k1"), []byte("v1"), rev.next(), 1)
	require.NoError(t, err)

	// 覆盖写，不指定 lease——保留旧值
	_, err = s.Put([]byte("k1"), []byte("v2"), rev.next(), 0)
	require.NoError(t, err)

	ikv, err := s.Get([]byte("k1"), rev.next())
	require.NoError(t, err)
	// 覆盖写 lease=0 时沿用旧值
	assert.Equal(t, int64(1), ikv.LeaseID)
}

// ========================================
// Range
// ========================================

func TestBadgerRangeFullScan(t *testing.T) {
	s, cleanup := testutil.NewTestStorage(t)
	defer cleanup()

	rev := revCounter{}
	_, err := s.Put([]byte("a"), []byte("va"), rev.next(), 0)
	require.NoError(t, err)
	_, err = s.Put([]byte("b"), []byte("vb"), rev.next(), 0)
	require.NoError(t, err)
	_, err = s.Put([]byte("c"), []byte("vc"), rev.next(), 0)
	require.NoError(t, err)

	ikvs, more, err := s.Range([]byte("a"), nil, 0, nil, rev.next())
	require.NoError(t, err)
	assert.False(t, more)
	assert.Len(t, ikvs, 3)
}

func TestBadgerRangeWithEnd(t *testing.T) {
	s, cleanup := testutil.NewTestStorage(t)
	defer cleanup()

	rev := revCounter{}
	_, _ = s.Put([]byte("a"), []byte("va"), rev.next(), 0)
	_, _ = s.Put([]byte("b"), []byte("vb"), rev.next(), 0)
	_, _ = s.Put([]byte("c"), []byte("vc"), rev.next(), 0)

	// [a, c) => 只返 a 和 b
	ikvs, more, err := s.Range([]byte("a"), []byte("c"), 0, nil, rev.next())
	require.NoError(t, err)
	assert.False(t, more)
	assert.Len(t, ikvs, 2)

	var keys []string
	for _, kv := range ikvs {
		keys = append(keys, string(kv.Key))
	}
	assert.Equal(t, []string{"a", "b"}, keys)
}

func TestBadgerRangeStartNotExist(t *testing.T) {
	s, cleanup := testutil.NewTestStorage(t)
	defer cleanup()

	rev := revCounter{}
	_, _ = s.Put([]byte("b"), []byte("vb"), rev.next(), 0)
	_, _ = s.Put([]byte("c"), []byte("vc"), rev.next(), 0)

	// start 不存在，Seek 到下一个
	ikvs, more, err := s.Range([]byte("a"), nil, 0, nil, rev.next())
	require.NoError(t, err)
	assert.False(t, more)
	assert.Len(t, ikvs, 2)
}

func TestBadgerRangeLimit(t *testing.T) {
	s, cleanup := testutil.NewTestStorage(t)
	defer cleanup()

	rev := revCounter{}
	_, _ = s.Put([]byte("k1"), []byte("v1"), rev.next(), 0)
	_, _ = s.Put([]byte("k2"), []byte("v2"), rev.next(), 0)
	_, _ = s.Put([]byte("k3"), []byte("v3"), rev.next(), 0)

	ikvs, more, err := s.Range([]byte("k"), nil, 2, nil, rev.next())
	require.NoError(t, err)
	assert.True(t, more)
	assert.Len(t, ikvs, 2)
}

func TestBadgerRangeLimitExact(t *testing.T) {
	s, cleanup := testutil.NewTestStorage(t)
	defer cleanup()

	rev := revCounter{}
	_, _ = s.Put([]byte("k1"), []byte("v1"), rev.next(), 0)
	_, _ = s.Put([]byte("k2"), []byte("v2"), rev.next(), 0)

	// limit 刚好等于总量
	ikvs, more, err := s.Range([]byte("k"), nil, 2, nil, rev.next())
	require.NoError(t, err)
	assert.False(t, more)
	assert.Len(t, ikvs, 2)
}

func TestBadgerRangeEmpty(t *testing.T) {
	s, cleanup := testutil.NewTestStorage(t)
	defer cleanup()

	rev := revCounter{}

	ikvs, more, err := s.Range([]byte("no"), []byte("np"), 0, nil, rev.next())
	require.NoError(t, err)
	assert.False(t, more)
	assert.Empty(t, ikvs)
}

func TestBadgerRangeFilter(t *testing.T) {
	s, cleanup := testutil.NewTestStorage(t)
	defer cleanup()

	rev := revCounter{}
	_, _ = s.Put([]byte("a"), []byte("va"), rev.next(), 0)
	_, _ = s.Put([]byte("ab"), []byte("vab"), rev.next(), 0)
	_, _ = s.Put([]byte("b"), []byte("vb"), rev.next(), 0)

	// 过滤掉 b
	ikvs, more, err := s.Range([]byte("a"), nil, 0,
		func(ikv *util.InternalKV) bool {
			return string(ikv.Key) != "b"
		}, rev.next())
	require.NoError(t, err)
	assert.False(t, more)
	assert.Len(t, ikvs, 2)
	assert.Equal(t, []byte("a"), ikvs[0].Key)
	assert.Equal(t, []byte("ab"), ikvs[1].Key)
}

func TestBadgerRangeEndWithLimit(t *testing.T) {
	s, cleanup := testutil.NewTestStorage(t)
	defer cleanup()

	rev := revCounter{}
	_, _ = s.Put([]byte("k1"), []byte("v1"), rev.next(), 0)
	_, _ = s.Put([]byte("k2"), []byte("v2"), rev.next(), 0)
	_, _ = s.Put([]byte("k3"), []byte("v3"), rev.next(), 0)
	_, _ = s.Put([]byte("k4"), []byte("v4"), rev.next(), 0)

	// [k, k3) 内 limit=2，且区间内实际只有 k1,k2
	ikvs, more, err := s.Range([]byte("k"), []byte("k3"), 2, nil, rev.next())
	require.NoError(t, err)
	assert.False(t, more, "区间内刚好 2 条，无更多")
	assert.Len(t, ikvs, 2)
}

func TestBadgerRangeKeyOrder(t *testing.T) {
	s, cleanup := testutil.NewTestStorage(t)
	defer cleanup()

	rev := revCounter{}
	// 乱序写入
	_, _ = s.Put([]byte("z"), []byte("vz"), rev.next(), 0)
	_, _ = s.Put([]byte("a"), []byte("va"), rev.next(), 0)
	_, _ = s.Put([]byte("m"), []byte("vm"), rev.next(), 0)

	ikvs, more, err := s.Range([]byte("a"), nil, 0, nil, rev.next())
	require.NoError(t, err)
	assert.False(t, more)
	assert.Len(t, ikvs, 3)

	// BadgerDB 迭代器保证字典序
	assert.True(t, sort.IsSorted(byKey(ikvs)))
}

// ========================================
// Close
// ========================================

func TestBadgerClose(t *testing.T) {
	s, cleanup := testutil.NewTestStorage(t)
	defer cleanup()

	assert.NotPanics(t, func() {
		_ = s.Close()
	})

	// double close 不 panic
	assert.NotPanics(t, func() {
		_ = s.Close()
	})
}
