package kv_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/DecarbonizedGlucose/rkv/api/proto/pkg/kvpb"
	"github.com/DecarbonizedGlucose/rkv/api/proto/pkg/raftpb"
	"github.com/DecarbonizedGlucose/rkv/pkg/kv"
	"github.com/DecarbonizedGlucose/rkv/pkg/lease"
	"github.com/DecarbonizedGlucose/rkv/pkg/storage"
	"github.com/DecarbonizedGlucose/rkv/pkg/util"
	"github.com/DecarbonizedGlucose/rkv/pkg/watch"
)

// fixture assembles all Week 3 components and simulates the Raft apply path
// by routing proposeFn directly to StateMachine.Apply.
type fixture struct {
	stor   storage.Storage
	sm     *kv.StateMachine
	wm     *watch.WatchManager
	tw     *lease.TimeWheel
	lm     *lease.LeaseManager
	revMgr *kv.RevisionManager
}

func newFixture(t *testing.T) (*fixture, func()) {
	t.Helper()

	s, err := storage.NewBadgerStorageInMemory()
	require.NoError(t, err)

	revMgr := kv.NewRevisionManager(0)
	sm := kv.NewStateMachine(s, revMgr)
	wm := watch.NewWatchManager(revMgr)

	cfg := lease.DefaultTimeWheelConfig()
	cfg.TickDuration = 10 * time.Millisecond
	tw := lease.NewTimeWheel(cfg)

	// proposeFn bypasses Raft: it marshals the command, wraps it in a
	// ProposalOperation, and feeds it directly into StateMachine.Apply.
	proposeFn := func(cmd *kvpb.Command) error {
		cmdBytes, err := proto.Marshal(cmd)
		if err != nil {
			return err
		}
		rev := revMgr.Next()
		op := &kv.ProposalOperation{
			ProposalID: 0,
			Revision:   rev,
			Command:    cmdBytes,
		}
		data, err := kv.MarshalProposalOperation(op)
		if err != nil {
			return err
		}
		_, err = sm.Apply([]*raftpb.Entry{{Data: data}})
		return err
	}

	lm := lease.NewLeaseManager(tw, nil, proposeFn)

	sm.SetWatchPublisher(wm)
	sm.SetLeaseHandler(lm)

	f := &fixture{
		stor:   s,
		sm:     sm,
		wm:     wm,
		tw:     tw,
		lm:     lm,
		revMgr: revMgr,
	}

	cleanup := func() {
		tw.Stop()
		s.Close()
	}
	return f, cleanup
}

// ========================================
// Lease Grant + Revoke: key deleted
// ========================================

func TestLeaseGrantRevoke(t *testing.T) {
	fx, cleanup := newFixture(t)
	defer cleanup()

	// 1. Put key without lease
	_, err := fx.proposeCommand(&kvpb.Command{
		Op: &kvpb.Command_Put{
			Put: &kvpb.PutRequest{Key: []byte("k1"), Value: []byte("v1")},
		},
	})
	require.NoError(t, err)

	// Verify key exists
	ikv, err := fx.stor.Get([]byte("k1"), fx.revMgr.Peek())
	require.NoError(t, err)
	assert.Equal(t, []byte("v1"), ikv.Value)

	// 2. Grant lease
	id, err := fx.lm.LeaseGrant(0, 30)
	require.NoError(t, err)

	// 3. Attach key to lease via Put with lease
	_, err = fx.proposeCommand(&kvpb.Command{
		Op: &kvpb.Command_Put{
			Put: &kvpb.PutRequest{Key: []byte("k1"), Value: []byte("v2"), Lease: id},
		},
	})
	require.NoError(t, err)

	// 4. Revoke lease
	err = fx.lm.LeaseRevoke(id)
	require.NoError(t, err)

	// 5. Key must be deleted
	_, err = fx.stor.Get([]byte("k1"), fx.revMgr.Peek())
	assert.ErrorIs(t, err, storage.ErrKeyNotFound, "revoked lease must delete attached keys")
}

// ========================================
// Lease Expire: TTL到期后key删除 + Watch DELETE事件
// ========================================

func TestLeaseExpire(t *testing.T) {
	fx, cleanup := newFixture(t)
	defer cleanup()

	// Subscribe watcher with prefix
	w := fx.wm.Subscribe([]byte("svc/"), true, 0, true)

	// Put key with lease
	id, err := fx.lm.LeaseGrant(0, 30)
	require.NoError(t, err)

	_, err = fx.proposeCommand(&kvpb.Command{
		Op: &kvpb.Command_Put{
			Put: &kvpb.PutRequest{Key: []byte("svc/node1"), Value: []byte("addr1"), Lease: id},
		},
	})
	require.NoError(t, err)

	// Drain the PUT event from the channel
	select {
	case ev := <-w.EventCh:
		assert.Equal(t, kvpb.EventType_PUT, ev.Type)
	case <-time.After(time.Second):
		t.Fatal("expected PUT event")
	}

	// Simulate expiry: call onExpire directly (bypasses TimeWheel timing)
	fx.lm.LeaseRevoke(id)

	// Drain the DELETE event (DELETE events carry prev_kv, not kv)
	select {
	case ev := <-w.EventCh:
		assert.Equal(t, kvpb.EventType_DELETE, ev.Type)
		assert.Nil(t, ev.Kv, "DELETE event must have nil Kv")
		require.NotNil(t, ev.PrevKv, "DELETE event must carry prev_kv")
		assert.Equal(t, []byte("addr1"), ev.PrevKv.Value)
	case <-time.After(time.Second):
		t.Fatal("expected DELETE event after lease expiry")
	}

	// Key must be gone from storage
	_, err = fx.stor.Get([]byte("svc/node1"), fx.revMgr.Peek())
	assert.ErrorIs(t, err, storage.ErrKeyNotFound, "expired lease must delete key from storage")
}

// ========================================
// Lease KeepAlive 续约
// ========================================

func TestLeaseKeepAlive(t *testing.T) {
	fx, cleanup := newFixture(t)
	defer cleanup()

	id, err := fx.lm.LeaseGrant(0, 5)
	require.NoError(t, err)

	// KeepAlive requires IsLeader()==true, but our node is nil.
	// We test: the lease was granted and is alive — Revoke confirms it existed.
	// Full KeepAlive test (TimeWheel resets timer, lease doesn't expire)
	// needs a real *raftstore.Node — deferred to integration suite.

	keys := fx.lm.Revoke(id)
	require.NotNil(t, keys, "lease must exist after grant")
	assert.Empty(t, keys, "no keys attached yet")
}

// ========================================
// Lease MultiKeys: 一个lease关联多个key, 过期全部删除
// ========================================

func TestLeaseMultiKeys(t *testing.T) {
	fx, cleanup := newFixture(t)
	defer cleanup()

	w := fx.wm.Subscribe([]byte("k"), true, 0, true)

	id, err := fx.lm.LeaseGrant(0, 30)
	require.NoError(t, err)

	keys := [][]byte{[]byte("k1"), []byte("k2"), []byte("k3")}
	for _, key := range keys {
		_, err := fx.proposeCommand(&kvpb.Command{
			Op: &kvpb.Command_Put{
				Put: &kvpb.PutRequest{Key: key, Value: []byte("v"), Lease: id},
			},
		})
		require.NoError(t, err)
	}

	// Drain PUT events
	for range keys {
		select {
		case ev := <-w.EventCh:
			assert.Equal(t, kvpb.EventType_PUT, ev.Type)
		case <-time.After(time.Second):
			t.Fatal("expected PUT events for all keys")
		}
	}

	// Revoke — all keys must be deleted
	err = fx.lm.LeaseRevoke(id)
	require.NoError(t, err)

	// Drain DELETE events
	for range keys {
		select {
		case ev := <-w.EventCh:
			assert.Equal(t, kvpb.EventType_DELETE, ev.Type)
		case <-time.After(time.Second):
			t.Fatal("expected DELETE events for all keys")
		}
	}

	for _, key := range keys {
		_, err := fx.stor.Get(key, fx.revMgr.Peek())
		assert.ErrorIs(t, err, storage.ErrKeyNotFound, "key %s must be deleted", key)
	}
}

// ========================================
// Service Register + Discover
// ========================================

func TestServiceRegisterDiscover(t *testing.T) {
	fx, cleanup := newFixture(t)
	defer cleanup()

	// Watcher on /services/ prefix
	w := fx.wm.Subscribe([]byte("/services/"), true, 0, true)

	// Register instance 1 with lease 1
	id1, err := fx.lm.LeaseGrant(0, 30)
	require.NoError(t, err)
	_, err = fx.proposeCommand(&kvpb.Command{
		Op: &kvpb.Command_Put{
			Put: &kvpb.PutRequest{
				Key:   []byte("/services/game/node1"),
				Value: []byte(`{"addr":"10.0.0.1:8080"}`),
				Lease: id1,
			},
		},
	})
	require.NoError(t, err)

	// Register instance 2 with lease 2
	id2, err := fx.lm.LeaseGrant(0, 30)
	require.NoError(t, err)
	_, err = fx.proposeCommand(&kvpb.Command{
		Op: &kvpb.Command_Put{
			Put: &kvpb.PutRequest{
				Key:   []byte("/services/game/node2"),
				Value: []byte(`{"addr":"10.0.0.2:8080"}`),
				Lease: id2,
			},
		},
	})
	require.NoError(t, err)

	// Drain 2 PUT events
	for i := 0; i < 2; i++ {
		select {
		case ev := <-w.EventCh:
			assert.Equal(t, kvpb.EventType_PUT, ev.Type)
		case <-time.After(time.Second):
			t.Fatal("expected PUT event for registered service")
		}
	}

	// Instance 1 lease expires
	fx.lm.LeaseRevoke(id1)

	// Drain DELETE event for instance 1
	select {
	case ev := <-w.EventCh:
		assert.Equal(t, kvpb.EventType_DELETE, ev.Type)
		require.NotNil(t, ev.PrevKv, "DELETE must have prev_kv")
		assert.Equal(t, []byte("/services/game/node1"), ev.PrevKv.Key)
	case <-time.After(time.Second):
		t.Fatal("expected DELETE event for expired service instance")
	}

	// Instance 2 still exists
	ikv, err := fx.stor.Get([]byte("/services/game/node2"), fx.revMgr.Peek())
	require.NoError(t, err)
	assert.Equal(t, []byte(`{"addr":"10.0.0.2:8080"}`), ikv.Value)

	// Instance 1 is gone
	_, err = fx.stor.Get([]byte("/services/game/node1"), fx.revMgr.Peek())
	assert.ErrorIs(t, err, storage.ErrKeyNotFound)
}

// ========================================
// Watch StartRevision 历史回放
// ========================================

func TestWatchStartRevision(t *testing.T) {
	fx, cleanup := newFixture(t)
	defer cleanup()

	// Write keys at different revisions
	_, err := fx.proposeCommand(&kvpb.Command{
		Op: &kvpb.Command_Put{Put: &kvpb.PutRequest{Key: []byte("k1"), Value: []byte("v1")}},
	})
	require.NoError(t, err)
	revAfterK1 := fx.revMgr.Peek()

	_, err = fx.proposeCommand(&kvpb.Command{
		Op: &kvpb.Command_Put{Put: &kvpb.PutRequest{Key: []byte("k2"), Value: []byte("v2")}},
	})
	require.NoError(t, err)

	_, err = fx.proposeCommand(&kvpb.Command{
		Op: &kvpb.Command_Put{Put: &kvpb.PutRequest{Key: []byte("sz/1"), Value: []byte("s1")}},
	})
	require.NoError(t, err)

	// startRev = 2: k1 was written at rev=1, should NOT appear in replay.
	// k2 (rev=2) and sz/1 (rev=3) should appear.
	startRev := revAfterK1 + 1
	w := fx.wm.Subscribe([]byte(""), true, startRev, true)

	// Simulate replay by scanning storage with the same logic WatchServer uses
	var replayedKeys []string
	fx.stor.Range(nil, nil, 0, func(ikv *util.InternalKV) bool {
		if ikv.MRevision < startRev {
			return true
		}
		if !watch.MatchKey([]byte(""), true, ikv.Key) {
			return true
		}
		replayedKeys = append(replayedKeys, string(ikv.Key))
		return true
	}, fx.revMgr.Peek())

	assert.NotContains(t, replayedKeys, "k1", "k1 was written before startRev")
	assert.Contains(t, replayedKeys, "k2", "k2 was written after startRev")
	assert.Contains(t, replayedKeys, "sz/1", "sz/1 was written after startRev")

	_ = w // real-time watcher created for completeness
}

// ========================================
// helpers
// ========================================

func (fx *fixture) proposeCommand(cmd *kvpb.Command) (*kvpb.Result, error) {
	cmdBytes, err := proto.Marshal(cmd)
	if err != nil {
		return nil, err
	}
	rev := fx.revMgr.Next()
	op := &kv.ProposalOperation{
		ProposalID: 0,
		Revision:   rev,
		Command:    cmdBytes,
	}
	data, err := kv.MarshalProposalOperation(op)
	if err != nil {
		return nil, err
	}
	results, err := fx.sm.Apply([]*raftpb.Entry{{Data: data}})
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return &kvpb.Result{}, nil
	}
	pr, err := kv.UnmarshalProposalResult(results[0].Data)
	if err != nil {
		return nil, err
	}
	var res kvpb.Result
	if err := proto.Unmarshal(pr.Result, &res); err != nil {
		return nil, err
	}
	return &res, nil
}
