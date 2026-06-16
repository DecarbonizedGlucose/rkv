package kv

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/DecarbonizedGlucose/rkv/api/proto/pkg/kvpb"
	"github.com/DecarbonizedGlucose/rkv/api/proto/pkg/raftpb"
	"github.com/DecarbonizedGlucose/rkv/pkg/raftstore"
	"github.com/DecarbonizedGlucose/rkv/pkg/storage"
)

func makeEntry(pid, rev uint64, cmd *kvpb.Command) *raftpb.Entry {
	cmdBytes, _ := proto.Marshal(cmd)
	op := &proposalOperation{
		ProposalID: pid,
		Revision:   rev,
		Command:    cmdBytes,
	}
	data, _ := marshalProposalOperation(op)
	return &raftpb.Entry{Data: data}
}

func newPutCmd(key, value string) *kvpb.Command {
	return &kvpb.Command{
		Op: &kvpb.Command_Put{
			Put: &kvpb.PutRequest{
				Key:   []byte(key),
				Value: []byte(value),
			},
		},
	}
}

func newDeleteCmd(key string) *kvpb.Command {
	return &kvpb.Command{
		Op: &kvpb.Command_Delete{
			Delete: &kvpb.DeleteRequest{
				Key: []byte(key),
			},
		},
	}
}

func newSM(t *testing.T) *StateMachine {
	t.Helper()
	s, err := storage.NewBadgerStorageInMemory()
	if err != nil {
		t.Fatalf("open in-memory storage: %v", err)
	}
	return NewStateMachine(s, NewRevisionManager(0))
}

func getResult(t *testing.T, sm *StateMachine, entries []*raftpb.Entry) []*kvpb.Result {
	t.Helper()
	results, err := sm.Apply(entries)
	require.NoError(t, err)

	out := make([]*kvpb.Result, 0, len(results))
	for _, r := range results {
		pr, err := unmarshalProposalResult(r.Data)
		require.NoError(t, err)
		var res kvpb.Result
		err = proto.Unmarshal(pr.Result, &res)
		require.NoError(t, err)
		out = append(out, &res)
	}
	return out
}

func TestApplyPut(t *testing.T) {
	sm := newSM(t)

	entries := []*raftpb.Entry{makeEntry(1, 1, newPutCmd("k1", "v1"))}
	results := getResult(t, sm, entries)

	require.Len(t, results, 1)
	putResp := results[0].GetPut()
	require.NotNil(t, putResp)
	assert.Nil(t, putResp.PrevKv)

	ikv, err := sm.stor.Get([]byte("k1"), 0)
	require.NoError(t, err)
	assert.Equal(t, []byte("v1"), ikv.Value)
	assert.Equal(t, uint64(1), ikv.CRevision)
	assert.Equal(t, uint64(1), ikv.MRevision)
}

func TestApplyPutOverride(t *testing.T) {
	sm := newSM(t)

	_, err := sm.Apply([]*raftpb.Entry{makeEntry(1, 1, newPutCmd("k1", "v1"))})
	require.NoError(t, err)

	cmd := &kvpb.Command{
		Op: &kvpb.Command_Put{
			Put: &kvpb.PutRequest{
				Key:    []byte("k1"),
				Value:  []byte("v2"),
				PrevKv: 1,
			},
		},
	}
	entries := []*raftpb.Entry{makeEntry(2, 2, cmd)}
	results := getResult(t, sm, entries)

	putResp := results[0].GetPut()
	require.NotNil(t, putResp)
	require.NotNil(t, putResp.PrevKv)
	assert.Equal(t, []byte("v1"), putResp.PrevKv.Value)

	ikv, err := sm.stor.Get([]byte("k1"), 0)
	require.NoError(t, err)
	assert.Equal(t, []byte("v2"), ikv.Value)
	assert.Equal(t, uint64(1), ikv.CRevision)
	assert.Equal(t, uint64(2), ikv.MRevision)
}

func TestApplyPutWithPrevKV(t *testing.T) {
	sm := newSM(t)

	entries := []*raftpb.Entry{makeEntry(1, 1, newPutCmd("k1", "v1"))}
	getResult(t, sm, entries)

	cmd := &kvpb.Command{
		Op: &kvpb.Command_Put{
			Put: &kvpb.PutRequest{
				Key:    []byte("k2"),
				Value:  []byte("v2"),
				PrevKv: 1,
			},
		},
	}
	results := getResult(t, sm, []*raftpb.Entry{makeEntry(2, 2, cmd)})

	putResp := results[0].GetPut()
	assert.Nil(t, putResp.PrevKv)
}

func TestApplyDelete(t *testing.T) {
	sm := newSM(t)

	_, err := sm.Apply([]*raftpb.Entry{makeEntry(1, 1, newPutCmd("k1", "v1"))})
	require.NoError(t, err)

	results := getResult(t, sm, []*raftpb.Entry{makeEntry(2, 2, newDeleteCmd("k1"))})

	delResp := results[0].GetDelete()
	require.NotNil(t, delResp)
	assert.Equal(t, int64(1), delResp.Deleted)

	_, err = sm.stor.Get([]byte("k1"), 0)
	assert.ErrorIs(t, err, storage.ErrKeyNotFound)
}

func TestApplyDeleteNonExist(t *testing.T) {
	sm := newSM(t)

	results := getResult(t, sm, []*raftpb.Entry{makeEntry(1, 1, newDeleteCmd("k1"))})

	delResp := results[0].GetDelete()
	require.NotNil(t, delResp)
	assert.Equal(t, int64(0), delResp.Deleted)
}

func TestApplyDeletePrevKV(t *testing.T) {
	sm := newSM(t)
	_, err := sm.Apply([]*raftpb.Entry{makeEntry(1, 1, newPutCmd("k1", "v1"))})
	require.NoError(t, err)

	cmd := &kvpb.Command{
		Op: &kvpb.Command_Delete{
			Delete: &kvpb.DeleteRequest{
				Key:    []byte("k1"),
				PrevKv: 1,
			},
		},
	}
	results := getResult(t, sm, []*raftpb.Entry{makeEntry(2, 2, cmd)})

	delResp := results[0].GetDelete()
	require.NotNil(t, delResp.PrevKv)
	assert.Equal(t, []byte("v1"), delResp.PrevKv.Value)
}

func TestApplyTxn_CAS_Success(t *testing.T) {
	sm := newSM(t)
	_, err := sm.Apply([]*raftpb.Entry{makeEntry(1, 1, newPutCmd("k1", "v1"))})
	require.NoError(t, err)

	txn := &kvpb.Command{
		Op: &kvpb.Command_Txn{
			Txn: &kvpb.TxnRequest{
				Compares: []*kvpb.Compare{{
					Key:         []byte("k1"),
					Target:      kvpb.Compare_VERSION,
					Result:      kvpb.Compare_EQUAL,
					TargetValue: &kvpb.Compare_Version{Version: 1},
				}},
				Success: []*kvpb.RequestOp{{
					Request: &kvpb.RequestOp_Put{
						Put: &kvpb.PutRequest{Key: []byte("k1"), Value: []byte("v2")},
					},
				}},
			},
		},
	}
	results := getResult(t, sm, []*raftpb.Entry{makeEntry(2, 2, txn)})

	txnResp := results[0].GetTxn()
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)

	ikv, _ := sm.stor.Get([]byte("k1"), 0)
	assert.Equal(t, []byte("v2"), ikv.Value)
}

func TestApplyTxn_CAS_Failure(t *testing.T) {
	sm := newSM(t)
	_, err := sm.Apply([]*raftpb.Entry{makeEntry(1, 1, newPutCmd("k1", "v1"))})
	require.NoError(t, err)

	txn := &kvpb.Command{
		Op: &kvpb.Command_Txn{
			Txn: &kvpb.TxnRequest{
				Compares: []*kvpb.Compare{{
					Key:         []byte("k1"),
					Target:      kvpb.Compare_VERSION,
					Result:      kvpb.Compare_EQUAL,
					TargetValue: &kvpb.Compare_Version{Version: 2},
				}},
				Success: []*kvpb.RequestOp{{
					Request: &kvpb.RequestOp_Put{
						Put: &kvpb.PutRequest{Key: []byte("k1"), Value: []byte("v2")},
					},
				}},
				Failure: []*kvpb.RequestOp{{
					Request: &kvpb.RequestOp_Put{
						Put: &kvpb.PutRequest{Key: []byte("k1"), Value: []byte("v3")},
					},
				}},
			},
		},
	}
	results := getResult(t, sm, []*raftpb.Entry{makeEntry(2, 2, txn)})

	txnResp := results[0].GetTxn()
	require.False(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)

	ikv, _ := sm.stor.Get([]byte("k1"), 0)
	assert.Equal(t, []byte("v3"), ikv.Value)
}

func TestApplyTxn_CompareVERSION_KeyNotExist(t *testing.T) {
	sm := newSM(t)

	txn := &kvpb.Command{
		Op: &kvpb.Command_Txn{
			Txn: &kvpb.TxnRequest{
				Compares: []*kvpb.Compare{{
					Key:         []byte("k1"),
					Target:      kvpb.Compare_VERSION,
					Result:      kvpb.Compare_EQUAL,
					TargetValue: &kvpb.Compare_Version{Version: 0},
				}},
				Success: []*kvpb.RequestOp{{
					Request: &kvpb.RequestOp_Put{
						Put: &kvpb.PutRequest{Key: []byte("k1"), Value: []byte("v1")},
					},
				}},
			},
		},
	}
	results := getResult(t, sm, []*raftpb.Entry{makeEntry(1, 1, txn)})

	txnResp := results[0].GetTxn()
	assert.True(t, txnResp.Succeeded)
}

func TestApplyTxn_CompareCREATE(t *testing.T) {
	sm := newSM(t)
	_, _ = sm.Apply([]*raftpb.Entry{makeEntry(1, 1, newPutCmd("k1", "v1"))})

	txn := &kvpb.Command{
		Op: &kvpb.Command_Txn{
			Txn: &kvpb.TxnRequest{
				Compares: []*kvpb.Compare{{
					Key:         []byte("k1"),
					Target:      kvpb.Compare_CREATE,
					Result:      kvpb.Compare_EQUAL,
					TargetValue: &kvpb.Compare_Version{Version: 1},
				}},
				Success: []*kvpb.RequestOp{{
					Request: &kvpb.RequestOp_Put{
						Put: &kvpb.PutRequest{Key: []byte("k2"), Value: []byte("v2")},
					},
				}},
			},
		},
	}
	results := getResult(t, sm, []*raftpb.Entry{makeEntry(2, 2, txn)})
	assert.True(t, results[0].GetTxn().Succeeded)
}

func TestApplyTxn_CompareVALUE(t *testing.T) {
	sm := newSM(t)
	_, _ = sm.Apply([]*raftpb.Entry{makeEntry(1, 1, newPutCmd("k1", "v1"))})

	txn := &kvpb.Command{
		Op: &kvpb.Command_Txn{
			Txn: &kvpb.TxnRequest{
				Compares: []*kvpb.Compare{{
					Key:         []byte("k1"),
					Target:      kvpb.Compare_VALUE,
					Result:      kvpb.Compare_EQUAL,
					TargetValue: &kvpb.Compare_Value{Value: []byte("v1")},
				}},
				Success: []*kvpb.RequestOp{{
					Request: &kvpb.RequestOp_Put{
						Put: &kvpb.PutRequest{Key: []byte("k2"), Value: []byte("ok")},
					},
				}},
			},
		},
	}
	results := getResult(t, sm, []*raftpb.Entry{makeEntry(2, 2, txn)})
	assert.True(t, results[0].GetTxn().Succeeded)
}

func TestApplyTxn_CompareLEASE(t *testing.T) {
	sm := newSM(t)
	cmd := &kvpb.Command{
		Op: &kvpb.Command_Put{
			Put: &kvpb.PutRequest{Key: []byte("k1"), Value: []byte("v1"), Lease: 99},
		},
	}
	_, _ = sm.Apply([]*raftpb.Entry{makeEntry(1, 1, cmd)})

	txn := &kvpb.Command{
		Op: &kvpb.Command_Txn{
			Txn: &kvpb.TxnRequest{
				Compares: []*kvpb.Compare{{
					Key:         []byte("k1"),
					Target:      kvpb.Compare_LEASE,
					Result:      kvpb.Compare_EQUAL,
					TargetValue: &kvpb.Compare_Version{Version: 99},
				}},
				Success: []*kvpb.RequestOp{{
					Request: &kvpb.RequestOp_Put{
						Put: &kvpb.PutRequest{Key: []byte("k2"), Value: []byte("ok")},
					},
				}},
			},
		},
	}
	results := getResult(t, sm, []*raftpb.Entry{makeEntry(2, 2, txn)})
	assert.True(t, results[0].GetTxn().Succeeded)
}

func TestApplyTxn_CompareVALUE_NotEqual(t *testing.T) {
	sm := newSM(t)
	_, _ = sm.Apply([]*raftpb.Entry{makeEntry(1, 1, newPutCmd("k1", "v1"))})

	txn := &kvpb.Command{
		Op: &kvpb.Command_Txn{
			Txn: &kvpb.TxnRequest{
				Compares: []*kvpb.Compare{{
					Key:         []byte("k1"),
					Target:      kvpb.Compare_VALUE,
					Result:      kvpb.Compare_NOT_EQUAL,
					TargetValue: &kvpb.Compare_Value{Value: []byte("v2")},
				}},
				Success: []*kvpb.RequestOp{{
					Request: &kvpb.RequestOp_Put{
						Put: &kvpb.PutRequest{Key: []byte("k2"), Value: []byte("ok")},
					},
				}},
			},
		},
	}
	results := getResult(t, sm, []*raftpb.Entry{makeEntry(2, 2, txn)})
	assert.True(t, results[0].GetTxn().Succeeded)
}

func TestApplyTxn_CompareGreaterLess(t *testing.T) {
	sm := newSM(t)
	_, _ = sm.Apply([]*raftpb.Entry{makeEntry(1, 1, newPutCmd("k1", "v1"))})
	_, _ = sm.Apply([]*raftpb.Entry{makeEntry(2, 2, newPutCmd("k1", "v2"))})

	txn := &kvpb.Command{
		Op: &kvpb.Command_Txn{
			Txn: &kvpb.TxnRequest{
				Compares: []*kvpb.Compare{{
					Key:         []byte("k1"),
					Target:      kvpb.Compare_VERSION,
					Result:      kvpb.Compare_GREATER,
					TargetValue: &kvpb.Compare_Version{Version: 1},
				}},
				Success: []*kvpb.RequestOp{{
					Request: &kvpb.RequestOp_Put{
						Put: &kvpb.PutRequest{Key: []byte("k2"), Value: []byte("ok")},
					},
				}},
			},
		},
	}
	results := getResult(t, sm, []*raftpb.Entry{makeEntry(3, 3, txn)})
	assert.True(t, results[0].GetTxn().Succeeded)

	txn = &kvpb.Command{
		Op: &kvpb.Command_Txn{
			Txn: &kvpb.TxnRequest{
				Compares: []*kvpb.Compare{{
					Key:         []byte("k1"),
					Target:      kvpb.Compare_VERSION,
					Result:      kvpb.Compare_LESS,
					TargetValue: &kvpb.Compare_Version{Version: 3},
				}},
				Success: []*kvpb.RequestOp{{
					Request: &kvpb.RequestOp_Put{
						Put: &kvpb.PutRequest{Key: []byte("k3"), Value: []byte("ok")},
					},
				}},
			},
		},
	}
	results = getResult(t, sm, []*raftpb.Entry{makeEntry(4, 4, txn)})
	assert.True(t, results[0].GetTxn().Succeeded)
}

func TestApplyTxn_MultiCompare_AND(t *testing.T) {
	sm := newSM(t)
	_, _ = sm.Apply([]*raftpb.Entry{makeEntry(1, 1, newPutCmd("k1", "v1"))})
	_, _ = sm.Apply([]*raftpb.Entry{makeEntry(2, 2, newPutCmd("k2", "v2"))})

	txn := &kvpb.Command{
		Op: &kvpb.Command_Txn{
			Txn: &kvpb.TxnRequest{
				Compares: []*kvpb.Compare{
					{
						Key:         []byte("k1"),
						Target:      kvpb.Compare_VERSION,
						Result:      kvpb.Compare_EQUAL,
						TargetValue: &kvpb.Compare_Version{Version: 1},
					},
					{
						Key:         []byte("k2"),
						Target:      kvpb.Compare_VERSION,
						Result:      kvpb.Compare_EQUAL,
						TargetValue: &kvpb.Compare_Version{Version: 2},
					},
				},
				Success: []*kvpb.RequestOp{{
					Request: &kvpb.RequestOp_Put{
						Put: &kvpb.PutRequest{Key: []byte("k3"), Value: []byte("ok")},
					},
				}},
			},
		},
	}
	results := getResult(t, sm, []*raftpb.Entry{makeEntry(3, 3, txn)})
	assert.True(t, results[0].GetTxn().Succeeded)

	// 一个不满足 → 都失败
	txn.GetTxn().Compares[1].TargetValue.(*kvpb.Compare_Version).Version = 99
	results = getResult(t, sm, []*raftpb.Entry{makeEntry(4, 4, txn)})
	assert.False(t, results[0].GetTxn().Succeeded)
}

func TestApplyTxn_RangeInTxn(t *testing.T) {
	sm := newSM(t)
	_, _ = sm.Apply([]*raftpb.Entry{makeEntry(1, 1, newPutCmd("k1", "v1"))})

	txn := &kvpb.Command{
		Op: &kvpb.Command_Txn{
			Txn: &kvpb.TxnRequest{
				Compares: []*kvpb.Compare{{
					Key:         []byte("k1"),
					Target:      kvpb.Compare_VERSION,
					Result:      kvpb.Compare_EQUAL,
					TargetValue: &kvpb.Compare_Version{Version: 1},
				}},
				Success: []*kvpb.RequestOp{{
					Request: &kvpb.RequestOp_Range{
						Range: &kvpb.RangeRequest{RangeStart: []byte("k1"), RangeEnd: []byte("k2")},
					},
				}},
			},
		},
	}
	results := getResult(t, sm, []*raftpb.Entry{makeEntry(2, 2, txn)})

	txnResp := results[0].GetTxn()
	require.Len(t, txnResp.Responses, 1)
	rangeResp := txnResp.Responses[0].GetRange()
	require.NotNil(t, rangeResp)
	require.Len(t, rangeResp.Kvs, 1)
	assert.Equal(t, []byte("v1"), rangeResp.Kvs[0].Value)
}

func TestApplyTxn_TxnPutDelete(t *testing.T) {
	sm := newSM(t)
	_, _ = sm.Apply([]*raftpb.Entry{makeEntry(1, 1, newPutCmd("k1", "v1"))})

	txn := &kvpb.Command{
		Op: &kvpb.Command_Txn{
			Txn: &kvpb.TxnRequest{
				Success: []*kvpb.RequestOp{
					{
						Request: &kvpb.RequestOp_Delete{
							Delete: &kvpb.DeleteRequest{Key: []byte("k1"), PrevKv: 1},
						},
					},
					{
						Request: &kvpb.RequestOp_Put{
							Put: &kvpb.PutRequest{Key: []byte("k2"), Value: []byte("v2")},
						},
					},
				},
			},
		},
	}
	results := getResult(t, sm, []*raftpb.Entry{makeEntry(2, 2, txn)})

	txnResp := results[0].GetTxn()
	require.Len(t, txnResp.Responses, 2)

	delResp := txnResp.Responses[0].GetDelete()
	assert.Equal(t, int64(1), delResp.Deleted)
	assert.Equal(t, []byte("v1"), delResp.PrevKv.Value)

	putResp := txnResp.Responses[1].GetPut()
	assert.Nil(t, putResp.PrevKv)

	_, err := sm.stor.Get([]byte("k1"), 0)
	assert.ErrorIs(t, err, storage.ErrKeyNotFound)
	ikv, _ := sm.stor.Get([]byte("k2"), 0)
	assert.Equal(t, []byte("v2"), ikv.Value)
}

func TestApplyCorruptedEntry(t *testing.T) {
	sm := newSM(t)

	entry := &raftpb.Entry{Data: []byte("not-a-valid-gob")}
	_, err := sm.Apply([]*raftpb.Entry{entry})
	assert.ErrorIs(t, err, raftstore.ErrApplyCorrupted)
}

func TestApplyInvalidCommand(t *testing.T) {
	sm := newSM(t)

	cmd := &kvpb.Command{}
	cmdBytes, _ := proto.Marshal(cmd)
	op := &proposalOperation{ProposalID: 1, Revision: 1, Command: cmdBytes}
	data, _ := marshalProposalOperation(op)

	entry := &raftpb.Entry{Data: data}
	_, err := sm.Apply([]*raftpb.Entry{entry})
	assert.ErrorIs(t, err, ErrInvalidCommand)
}

func TestApplyEmptyEntry(t *testing.T) {
	sm := newSM(t)

	entries := []*raftpb.Entry{
		{Data: nil},
		{Data: []byte{}},
		makeEntry(3, 3, newPutCmd("k1", "v1")),
	}
	results := getResult(t, sm, entries)

	assert.Len(t, results, 1)
	// 验证 entry 正确跳过空的
	putResp := results[0].GetPut()
	assert.NotNil(t, putResp)
}

func TestApplyIdempotent(t *testing.T) {
	sm := newSM(t)
	entry := makeEntry(1, 1, newPutCmd("k1", "v1"))

	r1, err := sm.Apply([]*raftpb.Entry{entry})
	require.NoError(t, err)

	r2, err := sm.Apply([]*raftpb.Entry{entry})
	require.NoError(t, err)

	require.Len(t, r1, 1)
	require.Len(t, r2, 1)
	assert.Equal(t, r1[0].ProposalID, r2[0].ProposalID)
	assert.Equal(t, r1[0].Data, r2[0].Data)

	ikv, _ := sm.stor.Get([]byte("k1"), 0)
	assert.Equal(t, []byte("v1"), ikv.Value)
	assert.Equal(t, uint64(1), ikv.MRevision)
}

func TestSnapshotAndRestore(t *testing.T) {
	sm := newSM(t)

	_, err := sm.Apply([]*raftpb.Entry{
		makeEntry(1, 1, newPutCmd("k1", "v1")),
		makeEntry(2, 2, newPutCmd("k2", "v2")),
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(2), sm.stor.MaxRevision())

	snapData, err := sm.SnapshotData()
	require.NoError(t, err)
	require.NotEmpty(t, snapData)

	newStor, err := storage.NewBadgerStorageInMemory()
	require.NoError(t, err)
	newSM := NewStateMachine(newStor, NewRevisionManager(0))
	err = newSM.ApplySnapshot(&raftpb.Snapshot{Data: snapData})
	require.NoError(t, err)

	assert.Equal(t, uint64(2), newStor.MaxRevision())
	assert.Equal(t, uint64(2), newSM.revMgr.Peek())

	ikv1, _ := newStor.Get([]byte("k1"), 0)
	assert.Equal(t, []byte("v1"), ikv1.Value)
	ikv2, _ := newStor.Get([]byte("k2"), 0)
	assert.Equal(t, []byte("v2"), ikv2.Value)
}

func TestApplySnapshotNilOrEmpty(t *testing.T) {
	sm := newSM(t)
	err := sm.ApplySnapshot(nil)
	assert.NoError(t, err)

	err = sm.ApplySnapshot(&raftpb.Snapshot{Data: nil})
	assert.NoError(t, err)

	err = sm.ApplySnapshot(&raftpb.Snapshot{Data: []byte{}})
	assert.NoError(t, err)
}

func TestApplyMultipleEntries(t *testing.T) {
	sm := newSM(t)

	entries := []*raftpb.Entry{
		makeEntry(1, 1, newPutCmd("k1", "v1")),
		makeEntry(2, 2, newPutCmd("k2", "v2")),
		makeEntry(3, 3, newDeleteCmd("k1")),
	}
	results := getResult(t, sm, entries)

	require.Len(t, results, 3)
	assert.NotNil(t, results[0].GetPut())
	assert.NotNil(t, results[1].GetPut())
	assert.NotNil(t, results[2].GetDelete())

	_, err := sm.stor.Get([]byte("k1"), 0)
	assert.ErrorIs(t, err, storage.ErrKeyNotFound)
	ikv, _ := sm.stor.Get([]byte("k2"), 0)
	assert.Equal(t, []byte("v2"), ikv.Value)
}

func TestApplyPutLease(t *testing.T) {
	sm := newSM(t)
	cmd := &kvpb.Command{
		Op: &kvpb.Command_Put{
			Put: &kvpb.PutRequest{Key: []byte("k1"), Value: []byte("v1"), Lease: 99},
		},
	}
	getResult(t, sm, []*raftpb.Entry{makeEntry(1, 1, cmd)})

	ikv, _ := sm.stor.Get([]byte("k1"), 0)
	assert.Equal(t, int64(99), ikv.LeaseID)
}

func TestApplyDeletePrevKVNotFound(t *testing.T) {
	sm := newSM(t)

	cmd := &kvpb.Command{
		Op: &kvpb.Command_Delete{
			Delete: &kvpb.DeleteRequest{Key: []byte("k1"), PrevKv: 1},
		},
	}
	results := getResult(t, sm, []*raftpb.Entry{makeEntry(1, 1, cmd)})

	delResp := results[0].GetDelete()
	assert.Nil(t, delResp.PrevKv)
}

func TestApplyTxn_CompareVALUE_KeyNotExist(t *testing.T) {
	sm := newSM(t)

	txn := &kvpb.Command{
		Op: &kvpb.Command_Txn{
			Txn: &kvpb.TxnRequest{
				Compares: []*kvpb.Compare{{
					Key:         []byte("k1"),
					Target:      kvpb.Compare_VALUE,
					Result:      kvpb.Compare_EQUAL,
					TargetValue: &kvpb.Compare_Value{Value: []byte("v1")},
				}},
				Success: []*kvpb.RequestOp{{
					Request: &kvpb.RequestOp_Put{
						Put: &kvpb.PutRequest{Key: []byte("k2"), Value: []byte("ok")},
					},
				}},
			},
		},
	}
	results := getResult(t, sm, []*raftpb.Entry{makeEntry(1, 1, txn)})
	assert.False(t, results[0].GetTxn().Succeeded)
}
