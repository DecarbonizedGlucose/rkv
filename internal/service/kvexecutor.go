package service

import (
	rapb "github.com/DecarbonizedGlucose/rkv/api/raftapplier"
)

type KVExecutor struct {
}

func (exec *KVExecutor) Execute(*rapb.RequestWithMeta) *rapb.Response {
	return &rapb.Response{}
}

func (exec *KVExecutor) Snapshot() []byte {
	return []byte{}
}

func (exec *KVExecutor) Restore([]byte) {

}
