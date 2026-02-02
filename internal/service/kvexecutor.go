package service

import (
	kvpb "github.com/DecarbonizedGlucose/rkv/api/kvrpc"
	rapb "github.com/DecarbonizedGlucose/rkv/api/raftapplier"
	eg "github.com/DecarbonizedGlucose/rkv/internal/engine"
)

type KVExecutor struct {
	engine eg.Storage
}

func MakeKVExecutor() *KVExecutor {
	return &KVExecutor{engine: eg.MakeStorage()}
}

func (exec *KVExecutor) Execute(reqM *rapb.RequestWithMeta) *rapb.Response {
	response := &rapb.Response{}
	switch req := reqM.KVRequest.(type) {
	case *rapb.RequestWithMeta_GetRequest:
		response.KVResponse = &rapb.Response_GetResponse{
			GetResponse: exec.Get(req.GetRequest),
		}
	case *rapb.RequestWithMeta_PutRequest:
		response.KVResponse = &rapb.Response_PutResponse{
			PutResponse: exec.Put(req.PutRequest),
		}
	case *rapb.RequestWithMeta_DeleteRequest:
		response.KVResponse = &rapb.Response_DeleteResponse{
			DeleteResponse: exec.Delete(req.DeleteRequest),
		}
	case *rapb.RequestWithMeta_AppendRequest:
		response.KVResponse = &rapb.Response_AppendResponse{
			AppendResponse: exec.Append(req.AppendRequest),
		}
	case *rapb.RequestWithMeta_CasRequest:
		response.KVResponse = &rapb.Response_CasResponse{
			CasResponse: exec.CompareAndSwap(req.CasRequest),
		}
	}
	return response
}

func (exec *KVExecutor) Snapshot() []byte {
	return []byte{}
}

func (exec *KVExecutor) Restore([]byte) {

}

// each operation

func (exec *KVExecutor) Get(req *kvpb.GetRequest) *kvpb.GetResponse {
	res := &kvpb.GetResponse{}
	var err error
	res.Value, res.Version, err = exec.engine.Get(req.Key)
	res.Status = eg.ErrorTranslate(err)
	return res
}

func (exec *KVExecutor) Put(req *kvpb.PutRequest) *kvpb.PutResponse {
	res := &kvpb.PutResponse{}
	var err error
	res.Version, err = exec.engine.Put(req.Key, req.Value)
	res.Status = eg.ErrorTranslate(err)
	return res
}

func (exec *KVExecutor) Delete(req *kvpb.DeleteRequest) *kvpb.DeleteResponse {
	res := &kvpb.DeleteResponse{}
	err := exec.engine.Delete(req.Key)
	res.Status = eg.ErrorTranslate(err)
	return res
}

func (exec *KVExecutor) Append(req *kvpb.AppendRequest) *kvpb.AppendResponse {
	res := &kvpb.AppendResponse{}
	var err error
	res.Value, res.Version, err = exec.engine.Append(req.Key, req.Suffix)
	res.Status = eg.ErrorTranslate(err)
	return res
}

func (exec *KVExecutor) CompareAndSwap(req *kvpb.CASRequest) *kvpb.CASResponse {
	res := &kvpb.CASResponse{}
	var err error
	res.Version, err = exec.engine.CompareAndSwap(req.Key, req.ExpectedVersion, req.Value)
	res.Status = eg.ErrorTranslate(err)
	return res
}
