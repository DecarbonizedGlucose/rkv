package kv

import (
	"bytes"
	"encoding/gob"
	"sync/atomic"

	"google.golang.org/protobuf/proto"

	"github.com/DecarbonizedGlucose/rkv/api/proto/pkg/kvpb"
)

type ProposalIDManager struct {
	pid atomic.Uint64
}

func (m *ProposalIDManager) Next() uint64 {
	return m.pid.Add(1)
}

func (m *ProposalIDManager) Peek() uint64 {
	return m.pid.Load()
}

type ProposalOperation struct {
	ProposalID uint64
	Revision   uint64
	Command    []byte // proto.Marshal'ed *kvpb.Command
}

type ProposalResult struct {
	ProposalID uint64
	Result     []byte // proto.Marshal'ed *kvpb.Result
}

func MarshalProposalOperation(op *ProposalOperation) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(op); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func UnmarshalProposalOperation(data []byte) (*ProposalOperation, error) {
	var op ProposalOperation
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&op); err != nil {
		return nil, err
	}
	return &op, nil
}

func MarshalProposalResult(res *ProposalResult) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(res); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func UnmarshalProposalResult(data []byte) (*ProposalResult, error) {
	var res ProposalResult
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&res); err != nil {
		return nil, err
	}
	return &res, nil
}

func DeserializeCommand(data []byte) (*kvpb.Command, error) {
	var cmd kvpb.Command
	if err := proto.Unmarshal(data, &cmd); err != nil {
		return nil, err
	}
	return &cmd, nil
}

func DeserializeResult(data []byte) (*kvpb.Result, error) {
	var res kvpb.Result
	if err := proto.Unmarshal(data, &res); err != nil {
		return nil, err
	}
	return &res, nil
}
