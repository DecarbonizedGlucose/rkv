package engine

import (
	"bytes"
	"log"
	"sync"

	kvpb "github.com/DecarbonizedGlucose/rkv/api/kvrpc"
	"github.com/dgraph-io/badger"
)

// Implementation of the "Storage" interface
type BadgerStorage struct {
	// no mutex needed, Badger is thread-safe
	db     *badger.DB
	mu     sync.RWMutex
	termCh chan struct{}
}

func MakeStorage(sp *string) Storage {
	st := &BadgerStorage{}
	// Automatically create it if not exists
	gdb, err := badger.Open(badger.DefaultOptions(*sp).WithSyncWrites(true))
	if err != nil {
		log.Fatal(err)
	}
	st.db = gdb
	st.termCh = make(chan struct{})
	go st.ListenSafeTerm()
	return st
}

func (st *BadgerStorage) Stop() {
	st.termCh <- struct{}{}
}

func (st *BadgerStorage) ListenSafeTerm() {
	<-st.termCh
	st.mu.Lock()
	defer st.mu.Unlock()
	st.db.Close()
}

func (st *BadgerStorage) Snapshot() (*bytes.Buffer, error) {
	st.mu.RLock()
	defer st.mu.Unlock()
	writer := new(bytes.Buffer)
	_, err := st.db.Backup(writer, 0) // returns with timestamp (version) which we do not need
	if err != nil {
		return nil, err
	}
	return writer, nil
}

func (st *BadgerStorage) Restore(buf *bytes.Buffer) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	err := st.db.Load(buf, 16)
	return err
}

func (st *BadgerStorage) Get(key []byte) ([]byte, uint64, error) {
	st.mu.RLock()
	defer st.mu.Unlock()
	txn := st.db.NewTransaction(false)
	defer txn.Discard()
	item, err := txn.Get(key)
	if err != nil {
		return nil, 0, err
	}
	var valCopy []byte
	valCopy, err = item.ValueCopy(nil)
	if err != nil {
		return nil, 0, err
	}
	return valCopy, item.Version(), nil
}

func (st *BadgerStorage) Put(key, value []byte) (uint64, error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	txn := st.db.NewTransaction(true)
	defer txn.Commit()
	err := txn.Set(key, value)
	if err != nil {
		return 0, err
	}
	item, err := txn.Get(key)
	if err != nil {
		return 0, err
	}
	return item.Version(), nil
}

func (st *BadgerStorage) Delete(key []byte) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	txn := st.db.NewTransaction(true)
	defer txn.Commit()
	err := txn.Delete(key)
	return err
}

func (st *BadgerStorage) Append(key, suffix []byte) ([]byte, uint64, error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	txn := st.db.NewTransaction(true)
	defer txn.Commit()
	item, err := txn.Get(key)
	var newValue []byte
	if err != nil {
		if err == badger.ErrKeyNotFound {
			newValue = suffix
		} else {
			return nil, 0, err
		}
	} else {
		var valCopy []byte
		valCopy, err = item.ValueCopy(nil)
		if err != nil {
			return nil, 0, err
		}
		newValue = append(valCopy, suffix...)
	}
	err = txn.Set(key, newValue)
	if err != nil {
		return nil, 0, err
	}
	item, err = txn.Get(key)
	if err != nil {
		return nil, 0, err
	}
	return newValue, item.Version(), nil
}

func (st *BadgerStorage) CompareAndSwap(key []byte, version uint64, value []byte) (uint64, error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	txn := st.db.NewTransaction(true)
	defer txn.Commit()
	item, err := txn.Get(key)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			if version != 0 {
				return 0, badger.ErrConflict
			}
			err = txn.Set(key, value)
			if err != nil {
				return 0, err
			}
			item, err = txn.Get(key)
			if err != nil {
				return 0, err
			}
			return item.Version(), nil
		}
		return 0, err
	}
	if item.Version() != version {
		return 0, badger.ErrConflict
	}
	err = txn.Set(key, value)
	if err != nil {
		return 0, err
	}
	item, err = txn.Get(key)
	if err != nil {
		return 0, err
	}
	return item.Version(), nil
}

func ErrorTranslate(err error) kvpb.KVErrorCode {
	switch err {
	case nil:
		return kvpb.KVErrorCode_OK
	case badger.ErrKeyNotFound:
		return kvpb.KVErrorCode_KEY_NOT_FOUND
	case badger.ErrConflict:
		return kvpb.KVErrorCode_CONFLICT
	default:
		return kvpb.KVErrorCode_INTERNAL
	}
}
