package service

import (
	kvpb "github.com/DecarbonizedGlucose/rkv/api/kvrpc"
	rapb "github.com/DecarbonizedGlucose/rkv/api/raftapplier"
	eg "github.com/DecarbonizedGlucose/rkv/internal/engine"
)

type SessionValue struct {
	lastRequestID int64
	lastResponse  *rapb.Response
}

type KVExecutor struct {
	engine   eg.Storage
	sessions map[string]*SessionValue
}

func MakeKVExecutor() *KVExecutor {
	return &KVExecutor{
		engine:   eg.MakeStorage(),
		sessions: make(map[string]*SessionValue),
	}
}

func (exec *KVExecutor) Execute(reqM *rapb.RequestWithMeta) *rapb.Response {
	response, dup := exec.IsDuplicate(reqM.Client_ID, reqM.Request_ID)
	if dup {
		return response
	}
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
	// update session
	exec.sessions[reqM.Client_ID] = &SessionValue{
		lastRequestID: reqM.Request_ID,
		lastResponse:  response,
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

// Deduplication

func GenOutdatedResponse() *rapb.Response {
	response := &rapb.Response{}
	switch res := response.KVResponse.(type) {
	case *rapb.Response_GetResponse:
		res.GetResponse = &kvpb.GetResponse{
			Status: kvpb.StatusCode_OUTDATED,
		}
	case *rapb.Response_PutResponse:
		res.PutResponse = &kvpb.PutResponse{
			Status: kvpb.StatusCode_OUTDATED,
		}
	case *rapb.Response_DeleteResponse:
		res.DeleteResponse = &kvpb.DeleteResponse{
			Status: kvpb.StatusCode_OUTDATED,
		}
	case *rapb.Response_AppendResponse:
		res.AppendResponse = &kvpb.AppendResponse{
			Status: kvpb.StatusCode_OUTDATED,
		}
	case *rapb.Response_CasResponse:
		res.CasResponse = &kvpb.CASResponse{
			Status: kvpb.StatusCode_OUTDATED,
		}
	}
	return response
}

func (exec *KVExecutor) IsDuplicate(clientID string, requestID int64) (*rapb.Response, bool) {
	session, exists := exec.sessions[clientID]
	if !exists {
		return &rapb.Response{}, false
	}
	if requestID > session.lastRequestID {
		return &rapb.Response{}, false
	}
	if requestID == session.lastRequestID {
		return session.lastResponse, true
	}
	return GenOutdatedResponse(), true
}
