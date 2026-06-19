package kv

import (
	"errors"

	"google.golang.org/protobuf/proto"

	"github.com/DecarbonizedGlucose/rkv/api/proto/pkg/kvpb"
	"github.com/DecarbonizedGlucose/rkv/api/proto/pkg/raftpb"
	"github.com/DecarbonizedGlucose/rkv/pkg/raftstore"
	"github.com/DecarbonizedGlucose/rkv/pkg/storage"
)

var ErrInvalidCommand = errors.New("invalid command")

// Publisher 是 StateMachine 发布事件的接口，供 WatchManager 实现。
type Publisher interface {
	// Publish 在 applyCommand 成功应用后调用，发布事件通知 WatchManager。
	Publish(key []byte, kv, prevKV *kvpb.KeyValue, eventType kvpb.EventType)
}

// LeaseHandler 是 StateMachine 与 LeaseManager 之间的接口。
type LeaseHandler interface {
	// Attach 在 applyPut 持有 lease 时调用，将 key 关联到租约。
	Attach(leaseID int64, key []byte)

	// Detach 在 applyDelete 或覆盖写入时调用，解除 key 与租约的关联。
	Detach(leaseID int64, key []byte)

	// Grant 在 apply LeaseGrant 时调用，激活租约并启动定时器。
	Grant(leaseID int64, ttl int64)

	// Revoke 在 apply LeaseRevoke 时调用，移除租约并停止定时器。
	Revoke(leaseID int64)
}

// StateMachine 是 Raft 驱动的 KV 状态机，负责应用
// 已提交的日志条目并维护 KV 状态。
//
// 只有写操作会经过 Raft 提交并应用到状态机。
// Apply 不使用 RevisionManager.Next()，revision
// 由 Leader 预分配并编码在 proposalOperation 中。
type StateMachine struct {
	stor   storage.Storage
	revMgr *RevisionManager
	wm     Publisher
	lm     LeaseHandler
}

func NewStateMachine(stor storage.Storage, revMgr *RevisionManager) *StateMachine {
	return &StateMachine{stor: stor, revMgr: revMgr}
}

func (sm *StateMachine) Apply(entries []*raftpb.Entry) (results []raftstore.ApplyResult, err error) {
	results = make([]raftstore.ApplyResult, 0, len(entries))

	for _, entry := range entries {
		if len(entry.Data) == 0 {
			continue
		}
		op, err := unmarshalProposalOperation(entry.Data)
		if err != nil {
			return nil, raftstore.ErrApplyCorrupted
		}
		rev := op.Revision
		cmd, err := deserializeCommand(op.Command)
		if err != nil {
			return nil, raftstore.ErrApplyCorrupted
		}
		res, err := sm.applyCommand(cmd, rev)
		if err != nil {
			return nil, err
		}
		resBytes, err := proto.Marshal(res)
		if err != nil {
			return nil, raftstore.ErrApplyCorrupted
		}
		resultData, err := marshalProposalResult(&proposalResult{
			ProposalID: op.ProposalID,
			Result:     resBytes,
		})
		if err != nil {
			return nil, raftstore.ErrApplyCorrupted
		}
		results = append(results, raftstore.ApplyResult{
			ProposalID: op.ProposalID,
			Data:       resultData,
		})
	}

	return results, nil
}

func (sm *StateMachine) applyCommand(cmd *kvpb.Command, rev uint64) (*kvpb.Result, error) {
	if cmd.GetPut() != nil {
		return sm.applyPut(cmd.GetPut(), rev)
	}
	if cmd.GetDelete() != nil {
		return sm.applyDelete(cmd.GetDelete(), rev)
	}
	if cmd.GetTxn() != nil {
		return sm.applyTxn(cmd.GetTxn(), rev)
	}
	return nil, ErrInvalidCommand
}

func (sm *StateMachine) applyPut(req *kvpb.PutRequest, rev uint64) (*kvpb.Result, error) {
	prevKV, err := sm.stor.Put(req.Key, req.Value, rev, req.Lease)
	res := &kvpb.PutResponse{}
	if err != nil {
		return nil, err
	}
	if req.PrevKv > 0 && prevKV != nil {
		res.PrevKv = prevKV.ToProto()
	}
	return &kvpb.Result{Res: &kvpb.Result_Put{Put: res}}, nil
}

func (sm *StateMachine) applyDelete(req *kvpb.DeleteRequest, rev uint64) (*kvpb.Result, error) {
	prevKV, err := sm.stor.Delete(req.Key, rev)
	res := &kvpb.DeleteResponse{}
	if err == storage.ErrKeyNotFound {
		res.Deleted = 0
	} else if err != nil {
		return nil, err
	} else {
		res.Deleted = 1
	}
	if req.PrevKv > 0 && prevKV != nil {
		res.PrevKv = prevKV.ToProto()
	}
	return &kvpb.Result{Res: &kvpb.Result_Delete{Delete: res}}, nil
}

func (sm *StateMachine) applyTxn(req *kvpb.TxnRequest, rev uint64) (*kvpb.Result, error) {
	txn := sm.stor.Txn(rev)
	defer txn.Discard()

	succeeded, err := sm.evalComparesTxn(txn, req.Compares)
	if err != nil {
		return nil, err
	}

	var ops []*kvpb.RequestOp
	if succeeded {
		ops = req.Success
	} else {
		ops = req.Failure
	}

	responses := make([]*kvpb.ResponseOp, 0, len(ops))
	for _, op := range ops {
		resp, prevKV, err := sm.applyRequestOpTxn(txn, op)
		if err != nil {
			return nil, err
		}
		results = append(results, &txnResult{op: op, resp: resp, prevKV: prevKV})
	}

	if err := txn.Commit(); err != nil {
		return nil, err
	}

	return &kvpb.Result{
		Res: &kvpb.Result_Txn{
			Txn: &kvpb.TxnResponse{
				Succeeded: succeeded,
				Responses: responses,
			},
		},
	}, nil
}

func (sm *StateMachine) evalComparesTxn(txn storage.Transaction, compares []*kvpb.Compare) (bool, error) {
	for _, cmp := range compares {
		ok, err := sm.evalCompareTxn(txn, cmp)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}
	return true, nil
}

func (sm *StateMachine) evalCompareTxn(txn storage.Transaction, cmp *kvpb.Compare) (bool, error) {
	ikv, err := txn.Get(cmp.Key)
	// key 不存在不是错误，按零值参与比较；其余存储错误需上报。
	if err != nil && err != storage.ErrKeyNotFound {
		return false, err
	}
	found := err == nil
	switch cmp.Target {
	case kvpb.Compare_VERSION:
		modRev := uint64(0)
		if found {
			modRev = ikv.MRevision
		}
		return compareInt(modRev, cmp.Result, cmp.GetVersion()), nil
	case kvpb.Compare_CREATE:
		cRev := uint64(0)
		if found {
			cRev = ikv.CRevision
		}
		return compareInt(cRev, cmp.Result, cmp.GetVersion()), nil
	case kvpb.Compare_VALUE:
		if !found {
			return compareBytes(nil, cmp.Result, cmp.GetValue()), nil
		}
		return compareBytes(ikv.Value, cmp.Result, cmp.GetValue()), nil
	case kvpb.Compare_LEASE:
		leaseID := int64(0)
		if found {
			leaseID = ikv.LeaseID
		}
		return compareInt(uint64(leaseID), cmp.Result, cmp.GetVersion()), nil
	default:
		return false, nil
	}
}

func (sm *StateMachine) applyRequestOpTxn(txn storage.Transaction, op *kvpb.RequestOp) (*kvpb.ResponseOp, error) {
	switch {
	case op.GetPut() != nil:
		prevKV, err := txn.Put(op.GetPut().Key, op.GetPut().Value, op.GetPut().Lease)
		if err != nil {
			return nil, err
		}
		res := &kvpb.PutResponse{}
		if op.GetPut().PrevKv > 0 && prevKV != nil {
			res.PrevKv = prevKV.ToProto()
		}
		return &kvpb.ResponseOp{Response: &kvpb.ResponseOp_Put{Put: res}}, nil
	case op.GetDelete() != nil:
		prevKV, err := txn.Delete(op.GetDelete().Key)
		res := &kvpb.DeleteResponse{}
		if err == storage.ErrKeyNotFound {
			res.Deleted = 0
		} else if err != nil {
			return nil, err
		} else {
			res.Deleted = 1
			if op.GetDelete().PrevKv > 0 && prevKV != nil {
				res.PrevKv = prevKV.ToProto()
			}
		}
		return &kvpb.ResponseOp{Response: &kvpb.ResponseOp_Delete{Delete: res}}, nil
	case op.GetRange() != nil:
		req := op.GetRange()
		start := req.RangeStart
		end := req.RangeEnd
		if len(end) == 0 {
			end = nil
		}
		ikvs, more, err := txn.Range(start, end, int(req.Limit), nil)
		if err != nil {
			return nil, err
		}
		kvs := make([]*kvpb.KeyValue, 0, len(ikvs))
		for _, ikv := range ikvs {
			kvs = append(kvs, ikv.ToProto())
		}
		return &kvpb.ResponseOp{Response: &kvpb.ResponseOp_Range{Range: &kvpb.RangeResponse{
			Kvs: kvs, More: more, Count: int64(len(kvs)),
		}}}, nil
	}
	return nil, ErrInvalidCommand
}

func (sm *StateMachine) SnapshotData() ([]byte, error) {
	return sm.stor.Snapshot()
}

func (sm *StateMachine) ApplySnapshot(snap *raftpb.Snapshot) error {
	if snap == nil || len(snap.Data) == 0 {
		return nil

	}
	if err := sm.stor.Restore(snap.Data); err != nil {
		return err
	}
	sm.revMgr.Set(sm.stor.MaxRevision()) // 快照恢复后, revision 可能不为 0，需更新 RevisionManager。
	return nil
}

func (sm *StateMachine) SetWatchPublisher(watchp Publisher) {
	sm.wm = watchp
}

func (sm *StateMachine) SetLeaseHandler(lm LeaseHandler) {
	sm.lm = lm
}
