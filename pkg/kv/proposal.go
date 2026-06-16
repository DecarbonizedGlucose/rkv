package kv

import (
	"bytes"
	"encoding/gob"

	"google.golang.org/protobuf/proto"

	"github.com/DecarbonizedGlucose/rkv/api/proto/pkg/kvpb"
)

type proposalOperation struct {
	ProposalID uint64
	Revision   uint64
	Command    []byte // proto.Marshal'ed *kvpb.Command
}

type proposalResult struct {
	ProposalID uint64
	Result     []byte // proto.Marshal'ed *kvpb.Result
}

func marshalProposalOperation(op *proposalOperation) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(op); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func unmarshalProposalOperation(data []byte) (*proposalOperation, error) {
	var op proposalOperation
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&op); err != nil {
		return nil, err
	}
	return &op, nil
}

func marshalProposalResult(res *proposalResult) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(res); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func unmarshalProposalResult(data []byte) (*proposalResult, error) {
	var res proposalResult
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&res); err != nil {
		return nil, err
	}
	return &res, nil
}

func deserializeCommand(data []byte) (*kvpb.Command, error) {
	var cmd kvpb.Command
	if err := proto.Unmarshal(data, &cmd); err != nil {
		return nil, err
	}
	return &cmd, nil
}

func deserializeResult(data []byte) (*kvpb.Result, error) {
	var res kvpb.Result
	if err := proto.Unmarshal(data, &res); err != nil {
		return nil, err
	}
	return &res, nil
}
