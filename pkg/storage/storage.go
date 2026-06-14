package storage

import (
	"github.com/DecarbonizedGlucose/rkv/pkg/util"
)

// Storage 为服务层提供存储的抽象
type Storage interface {
	Get(key []byte, rev uint64) (ikv *util.InternalKV, err error)
	Put(key, value []byte, prev_kv bool, rev uint64, lease int64) (ikv *util.InternalKV, err error)
	Delete(key []byte, prev_kv bool, rev uint64) (ikv *util.InternalKV, err error)
	Range(start, end []byte, limit int, fn func(ikv *util.InternalKV) bool, rev uint64) (ikvs []*util.InternalKV, more bool, err error)
	Txn(rev uint64) Transaction

	Close() error
}

type Transaction interface {
	Get(key []byte) (ikv *util.InternalKV, err error)
	Put(key, value []byte, prev_kv bool, lease int64) (ikv *util.InternalKV, err error)
	Delete(key []byte, prev_kv bool) (ikv *util.InternalKV, err error)
	Range(start, end []byte, limit int, fn func(ikv *util.InternalKV) bool) (ikvs []*util.InternalKV, more bool, err error)

	Commit() error
	Discard()
}
