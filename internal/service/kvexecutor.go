package service

import (
	rapb "github.com/DecarbonizedGlucose/rkv/api/raftapplier"
	eg "github.com/DecarbonizedGlucose/rkv/internal/engine"
)

type KVExecutor struct {
	engine *eg.Storage
}

func MakeKVExecutor() *KVExecutor {
	return &KVExecutor{}
}

func (exec *KVExecutor) Execute(*rapb.RequestWithMeta) *rapb.Response {
	return &rapb.Response{}
}

func (exec *KVExecutor) Snapshot() []byte {
	return []byte{}
}

func (exec *KVExecutor) Restore([]byte) {

}
