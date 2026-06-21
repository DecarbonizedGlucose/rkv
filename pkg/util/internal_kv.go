package util

import (
	"bytes"
	"encoding/gob"
	"errors"

	"github.com/DecarbonizedGlucose/rkv/api/proto/pkg/kvpb"
)

var (
	ErrInternalValueBroken = errors.New("rkv: internal key broken")
)

type InternalValue struct {
	UserValue      []byte
	CreateRevision uint64
	LeaseID        int64
}

func (iv *InternalValue) Copy() *InternalValue {
	newIV := &InternalValue{
		UserValue:      make([]byte, len(iv.UserValue)),
		CreateRevision: iv.CreateRevision,
		LeaseID:        iv.LeaseID,
	}
	copy(newIV.UserValue, iv.UserValue)
	return newIV
}

func MarshalInternalValue(iv *InternalValue) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(iv); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func UnmarshalInternalValue(data []byte) (*InternalValue, error) {
	var ikv InternalValue
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&ikv); err != nil {
		return nil, err
	}
	return &ikv, nil
}

type InternalKV struct {
	Key, Value                     []byte
	Revision, CRevision, MRevision uint64
	LeaseID                        int64
}

func (ikv *InternalKV) Copy() *InternalKV {
	newIKV := &InternalKV{
		Key:       make([]byte, len(ikv.Key)),
		Value:     make([]byte, len(ikv.Value)),
		Revision:  ikv.Revision,
		CRevision: ikv.CRevision,
		MRevision: ikv.MRevision,
		LeaseID:   ikv.LeaseID,
	}
	copy(newIKV.Key, ikv.Key)
	copy(newIKV.Value, ikv.Value)
	return newIKV
}

func MakeInternalKV(iv *InternalValue, data []byte, key []byte, rev, mrev uint64) (ikv *InternalKV, err error) {
	if iv == nil {
		iv, err = UnmarshalInternalValue(data)
		if err != nil {
			return nil, ErrInternalValueBroken
		}
	}

	ikv = &InternalKV{
		Key:       key,
		Value:     iv.UserValue,
		Revision:  rev,
		CRevision: iv.CreateRevision,
		MRevision: mrev,
		LeaseID:   iv.LeaseID,
	}
	return ikv, nil
}

func (ikv *InternalKV) ToProto() *kvpb.KeyValue {
	return &kvpb.KeyValue{
		Key:            ikv.Key,
		Value:          ikv.Value,
		Revision:       ikv.Revision,
		CreateRevision: ikv.CRevision,
		ModRevision:    ikv.MRevision,
		LeaseId:        ikv.LeaseID,
	}
}
