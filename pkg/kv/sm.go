package kv

import (
	"errors"

	"github.com/DecarbonizedGlucose/rkv/api/proto/pkg/kvpb"
	"github.com/DecarbonizedGlucose/rkv/api/proto/pkg/raftpb"
	"github.com/DecarbonizedGlucose/rkv/pkg/raftstore"
	"github.com/DecarbonizedGlucose/rkv/pkg/storage"
	"google.golang.org/protobuf/proto"
)

var ErrInvalidCommand = errors.New("invalid command")

// StateMachine 是 Raft 驱动的 KV 状态机，负责应用
// 已提交的日志条目并维护 KV 状态。
//
// 只有写操作会经过 Raft 提交并应用到状态机。
// Apply 不使用 RevisionManager.Next()，revision
// 由 Leader 预分配并编码在 proposalOperation 中。
type StateMachine struct {
	stor   storage.Storage
	revMgr *RevisionManager
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
	prevKV, err := sm.stor.Put(req.Key, req.Value, req.PrevKv > 0, rev, req.Lease)
	res := &kvpb.PutResponse{}
	if err != nil {
		return nil, err
	}
	if prevKV != nil {
		res.PrevKv = prevKV.ToProto()
	}
	return &kvpb.Result{Res: &kvpb.Result_Put{Put: res}}, nil
}

func (sm *StateMachine) applyDelete(req *kvpb.DeleteRequest, rev uint64) (*kvpb.Result, error) {
	prevKV, err := sm.stor.Delete(req.Key, req.PrevKv > 0, rev)
	res := &kvpb.DeleteResponse{}
	if err == storage.ErrKeyNotFound {
		res.Deleted = 0
	} else if err != nil {
		return nil, err
	} else {
		res.Deleted = 1
	}
	if prevKV != nil {
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
		resp, err := sm.applyRequestOpTxn(txn, op)
		if err != nil {
			return nil, err
		}
		responses = append(responses, resp)
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
		prevKV, err := txn.Put(op.GetPut().Key, op.GetPut().Value, op.GetPut().PrevKv > 0, op.GetPut().Lease)
		if err != nil {
			return nil, err
		}
		res := &kvpb.PutResponse{}
		if prevKV != nil {
			res.PrevKv = prevKV.ToProto()
		}
		return &kvpb.ResponseOp{Response: &kvpb.ResponseOp_Put{Put: res}}, nil
	case op.GetDelete() != nil:
		prevKV, err := txn.Delete(op.GetDelete().Key, op.GetDelete().PrevKv > 0)
		res := &kvpb.DeleteResponse{}
		if err == storage.ErrKeyNotFound {
			res.Deleted = 0
		} else if err != nil {
			return nil, err
		} else {
			res.Deleted = 1
			if prevKV != nil {
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
