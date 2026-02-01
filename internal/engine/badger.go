package engine

import (
	"log"

	"github.com/dgraph-io/badger"
)

// Implementation of the "Storage" interface
type BadgerStorage struct {
	// no mutex needed, Badger is thread-safe
	db *badger.DB
}

func MakeStorage() Storage {
	st := &BadgerStorage{}
	// Open the database under /tmp/badger
	// Automatically create it if not exists
	gdb, err := badger.Open(badger.DefaultOptions("/tmp/badger"))
	if err != nil {
		log.Fatal(err)
	}
	st.db = gdb
	// Remember to call db.Close()
	return st
}

func (st *BadgerStorage) Get(key []byte) ([]byte, uint64, error) {
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
	txn := st.db.NewTransaction(true)
	defer txn.Commit()
	err := txn.Delete(key)
	return err
}

func (st *BadgerStorage) Append(key, suffix []byte) ([]byte, uint64, error) {
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
