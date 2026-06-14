package testutil

import (
	"testing"

	"github.com/DecarbonizedGlucose/rkv/pkg/storage"
)

// NewTestStorage 创建纯内存的 BadgerStorage 并返回销毁函数。
func NewTestStorage(t *testing.T) (storage.Storage, func()) {
	t.Helper()
	s := storage.NewBadgerStorageInMemory()
	return s, func() {
		if err := s.Close(); err != nil {
			t.Errorf("close test storage: %v", err)
		}
	}
}
