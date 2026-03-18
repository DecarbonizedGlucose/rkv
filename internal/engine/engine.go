package engine

import (
	"bytes"
)

type Storage interface {
	Stop()
	Snapshot() (*bytes.Buffer, error)
	Restore(*bytes.Buffer) error

	Get(key []byte) ([]byte, uint64, error)
	Put(key, value []byte) (uint64, error)
	Delete(key []byte) error
	Append(key, suffix []byte) ([]byte, uint64, error)
	CompareAndSwap(key []byte, version uint64, value []byte) (uint64, error)
}
