package util

import (
	"bytes"
	"encoding/gob"
	"errors"
)

var (
	ErrInternalValueBroken = errors.New("rkv: internal key broken")
)

type InternalValue struct {
	UserValue      []byte
	CreateRevision uint64
	LeaseID        int64
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
