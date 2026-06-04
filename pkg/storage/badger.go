package storage

import (
	"bytes"

	"github.com/DecarbonizedGlucose/rkv/pkg/util"
	"github.com/dgraph-io/badger"
)

type BadgerStorage struct {
	db *badger.DB
}

func (b *BadgerStorage) Get(key []byte, rev uint64) (ikv *util.InternalKV, err error) {
	txn := b.db.NewTransaction(false)
	defer txn.Discard()

	item, err := txn.Get(key)
	if err != nil {
		return nil, err
	}
	cv, err := item.ValueCopy(nil)
	mrev := item.Version()
	if err != nil {
		return nil, err
	}
	ikv, err = util.MakeInternalKV(nil, cv, key, rev, mrev)
	if err != nil {
		return nil, err
	}
	return ikv, nil
}

func (b *BadgerStorage) Put(key, value []byte, prev_kv bool, rev uint64, lease int64) (ikv *util.InternalKV, err error) {
	txn := b.db.NewTransaction(true)
	defer txn.Discard()

	var iv *util.InternalValue
	var cv []byte
	item, err := txn.Get(key)
	if err == badger.ErrKeyNotFound {
		iv = &util.InternalValue{
			UserValue:      value,
			CreateRevision: rev,
			LeaseID:        lease,
		}
	} else if err != nil {
		return nil, err
	} else {
		cv, err = item.ValueCopy(nil)
		if err != nil {
			return nil, err
		}
		iv, err = util.UnmarshalInternalValue(cv)
		if err != nil {
			return nil, err
		}
		iv.UserValue = value
	}
	newv, err := util.MarshalInternalValue(iv)
	if err != nil {
		return nil, err
	}
	err = txn.Set(key, newv)
	if err != nil {
		return nil, err
	}
	if prev_kv {
		ikv, err = util.MakeInternalKV(iv, nil, nil, 0, 0)
		if err != nil {
			return nil, err
		}
	}
	if err = txn.Commit(); err != nil {
		return nil, err
	}
	return ikv, nil
}

func (b *BadgerStorage) Delete(key []byte, prev_kv bool, rev uint64) (ikv *util.InternalKV, err error) {
	txn := b.db.NewTransaction(true)
	defer txn.Discard()

	var cv []byte
	item, err := txn.Get(key)
	if err == badger.ErrKeyNotFound {
		return nil, ErrKeyNotFound
	} else if err != nil {
		return nil, err
	}

	if prev_kv {
		cv, err = item.ValueCopy(nil)
		if err != nil {
			return nil, err
		}
		ikv, err = util.MakeInternalKV(nil, cv, key, rev, item.Version())
		if err != nil {
			return nil, err
		}
	}

	if err = txn.Delete(key); err != nil {
		return nil, err
	}
	if err = txn.Commit(); err != nil {
		return nil, err
	}
	return ikv, nil
}

func (b *BadgerStorage) Range(start, end []byte, limit int, fn func(ikv *util.InternalKV) bool) (ikvs []*util.InternalKV, more bool, err error) {
	txn := b.db.NewTransaction(false)
	defer txn.Discard()

	if len(end) == 0 {
		item, err := txn.Get(start)
		if err == badger.ErrKeyNotFound {
			return nil, false, nil
		}
		if err != nil {
			return nil, false, err
		}
		cv, err := item.ValueCopy(nil)
		if err != nil {
			return nil, false, err
		}
		ikv, err := util.MakeInternalKV(nil, cv, start, 0, item.Version())
		if err != nil {
			return nil, false, err
		}
		if fn != nil && !fn(ikv) {
			return nil, false, nil
		}
		return []*util.InternalKV{ikv}, false, nil
	}

	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = true
	it := txn.NewIterator(opts)
	defer it.Close()

	count := 0
	for it.Seek(start); it.Valid(); it.Next() {
		item := it.Item()
		key := item.Key()
		if bytes.Compare(key, end) >= 0 {
			break
		}

		if limit > 0 && count >= limit {
			it.Next()
			if it.Valid() && bytes.Compare(it.Item().Key(), end) < 0 {
				more = true
			}
			break
		}

		cv, err := item.ValueCopy(nil)
		if err != nil {
			return nil, false, err
		}
		ikv, err := util.MakeInternalKV(nil, cv, key, 0, item.Version())
		if err != nil {
			return nil, false, err
		}
		if fn != nil && !fn(ikv) {
			continue
		}
		ikvs = append(ikvs, ikv)
		count++
	}
	return ikvs, more, nil
}

func (b *BadgerStorage) Close() error {
	return nil
}

func NewBadgerStorage() Storage {
	return &BadgerStorage{}
}
