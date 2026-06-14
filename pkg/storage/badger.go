package storage

import (
	"bytes"
	"encoding/gob"
	"log"
	"math"
	"sort"

	"github.com/DecarbonizedGlucose/rkv/pkg/util"
	"github.com/dgraph-io/badger/v4"
)

// BadgerStorage 是 Storage 接口的 BadgerDB 实现，使用 ManagedDB 手动控制版本号。
// revision 由上层 KV 状态机分配，通过 CommitAt(rev) 写入。
type BadgerStorage struct {
	db *badger.DB
}

// Get 获取指定 key 在当前最新版本下的值。
// rev 参数保留用于后续支持指定 revision 读取，当前固定读取最新版本。
func (b *BadgerStorage) Get(key []byte, rev uint64) (ikv *util.InternalKV, err error) {
	readTs := b.db.MaxVersion()
	txn := b.db.NewTransactionAt(readTs, false)
	defer txn.Discard()

	return bGet(txn, key, rev)
}

// Put 写入或覆盖 key，以 rev 作为 Badger 版本号提交。
// prev_kv 为 true 时返回修改前的完整 IKVs，若 key 不存在则 ikv 为 nil。
func (b *BadgerStorage) Put(key, value []byte, prev_kv bool, rev uint64, lease int64) (ikv *util.InternalKV, err error) {
	readTs := b.db.MaxVersion()
	txn := b.db.NewTransactionAt(readTs, true)
	defer txn.Discard()

	ikv, err = bPut(txn, key, value, prev_kv, rev, lease)
	if err != nil {
		return nil, err
	}

	if err := txn.CommitAt(rev, nil); err != nil {
		return nil, err
	}
	return ikv, nil
}

// Delete 删除 key，以 rev 作为 Badger 版本号提交。
// prev_kv 为 true 时返回删除前的完整 IKVs，若 key 不存在则返回 ErrKeyNotFound。
func (b *BadgerStorage) Delete(key []byte, prev_kv bool, rev uint64) (ikv *util.InternalKV, err error) {
	readTs := b.db.MaxVersion()
	txn := b.db.NewTransactionAt(readTs, true)
	defer txn.Discard()

	ikv, err = bDel(txn, key, prev_kv, rev)
	if err != nil {
		return nil, err
	}

	if err := txn.CommitAt(rev, nil); err != nil {
		return nil, err
	}
	return ikv, nil
}

// Range 在 [start, end) 区间内扫描 kv 对，end 为空表示无上限。
// limit 控制返回条目数（0 = 无限制），fn 用于过滤，返回 false 则跳过该条目。
// more 表示区间内仍有未返回的 key（因 limit 截断或迭代器未耗尽）。
func (b *BadgerStorage) Range(start, end []byte, limit int, fn func(ikv *util.InternalKV) bool, rev uint64) (ikvs []*util.InternalKV, more bool, err error) {
	readTs := b.db.MaxVersion()
	txn := b.db.NewTransactionAt(readTs, false)
	defer txn.Discard()

	return bRange(txn, start, end, limit, fn, rev)
}

func (b *BadgerStorage) Txn(rev uint64) Transaction {
	return NewBadgerTransaction(b.db, rev)
}

// Close 关闭底层 BadgerDB 实例。
func (b *BadgerStorage) Close() error {
	if b.db != nil {
		return b.db.Close()
	}
	return nil
}

func (b *BadgerStorage) Snapshot() ([]byte, error) {
	readTs := b.db.MaxVersion()
	txn := b.db.NewTransactionAt(readTs, false)
	defer txn.Discard()

	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()

	var pairs []*util.InternalKV
	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		cv, err := item.ValueCopy(nil)
		if err != nil {
			return nil, err
		}
		key := item.KeyCopy(nil)
		mrev := item.Version()
		ikv, err := util.MakeInternalKV(nil, cv, key, mrev, mrev)
		if err != nil {
			return nil, err
		}
		pairs = append(pairs, ikv)
	}

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(pairs); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (b *BadgerStorage) Restore(data []byte) error {
	var pairs []*util.InternalKV
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&pairs); err != nil {
		return err
	}
	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].MRevision < pairs[j].MRevision
	})
	for _, ikv := range pairs {
		if _, err := b.Put(ikv.Key, ikv.Value, false, ikv.MRevision, ikv.LeaseID); err != nil {
			return err
		}
	}
	return nil
}

func (b *BadgerStorage) MaxRevision() uint64 {
	return b.db.MaxVersion()
}

// NewBadgerStorage 打开或创建 BadgerDB ManagedDB 实例。
// 使用 Managed 模式以支持上层手动控制 revision 作为版本号。
// 返回 Storage 实例和当前最大 revision，后者可用于
// 初始化全局 RevisionManager。
func NewBadgerStorage(dir string) (Storage, uint64) {
	opts := badger.DefaultOptions(dir).
		WithNumVersionsToKeep(math.MaxInt32) // 保留所有历史版本，由上层控制压缩
	db, err := badger.OpenManaged(opts)
	if err != nil {
		log.Fatal(err)
	}
	return &BadgerStorage{db: db}, db.MaxVersion()
}

// NewBadgerStorageInMemory 创建纯内存的 BadgerStorage 实例，用于测试。
func NewBadgerStorageInMemory() Storage {
	opts := badger.DefaultOptions("").
		WithInMemory(true).
		WithNumVersionsToKeep(math.MaxInt32)
	db, err := badger.OpenManaged(opts)
	if err != nil {
		log.Fatal(err)
	}
	return &BadgerStorage{db: db}
}

type BadgerTransaction struct {
	txn *badger.Txn
	rev uint64
}

func NewBadgerTransaction(db *badger.DB, rev uint64) *BadgerTransaction {
	return &BadgerTransaction{
		txn: db.NewTransactionAt(rev, true),
		rev: rev,
	}
}

func (bt *BadgerTransaction) Get(key []byte) (ikv *util.InternalKV, err error) {
	return bGet(bt.txn, key, bt.rev)
}

func (bt *BadgerTransaction) Put(key, value []byte, prev_kv bool, lease int64) (ikv *util.InternalKV, err error) {
	return bPut(bt.txn, key, value, prev_kv, bt.rev, lease)
}

func (bt *BadgerTransaction) Delete(key []byte, prev_kv bool) (ikv *util.InternalKV, err error) {
	return bDel(bt.txn, key, prev_kv, bt.rev)
}

func (bt *BadgerTransaction) Range(start, end []byte, limit int, fn func(ikv *util.InternalKV) bool) (ikvs []*util.InternalKV, more bool, err error) {
	return bRange(bt.txn, start, end, limit, fn, bt.rev)
}

func (bt *BadgerTransaction) Commit() error {
	return bt.txn.CommitAt(bt.rev, nil)
}

func (bt *BadgerTransaction) Discard() {
	bt.txn.Discard()
}

func bGet(txn *badger.Txn, key []byte, rev uint64) (ikv *util.InternalKV, err error) {
	item, err := txn.Get(key)
	if err == badger.ErrKeyNotFound {
		return nil, ErrKeyNotFound
	}
	if err != nil {
		return nil, err
	}

	cv, err := item.ValueCopy(nil)
	if err != nil {
		return nil, err
	}
	mrev := item.Version()

	ikv, err = util.MakeInternalKV(nil, cv, key, rev, mrev)
	if err != nil {
		return nil, err
	}
	return ikv, nil
}

func bPut(txn *badger.Txn, key, value []byte, prev_kv bool, rev uint64, lease int64) (ikv *util.InternalKV, err error) {
	var iv, newIV *util.InternalValue
	var mrev uint64

	item, err := txn.Get(key)
	if err == badger.ErrKeyNotFound {
		newIV = &util.InternalValue{
			UserValue:      value,
			CreateRevision: rev,
			LeaseID:        lease,
		}
	} else if err != nil {
		return nil, err
	} else {
		mrev = item.Version()
		cv, err := item.ValueCopy(nil)
		if err != nil {
			return nil, err
		}
		iv, err = util.UnmarshalInternalValue(cv)
		if err != nil {
			return nil, err
		}
		newIV = iv.Copy()
		newIV.UserValue = value
		if lease != 0 { // 仅当 lease 非 0 时才更新，0 表示不修改租约
			newIV.LeaseID = lease
		}
	}

	newv, err := util.MarshalInternalValue(newIV)
	if err != nil {
		return nil, err
	}
	if err := txn.Set(key, newv); err != nil {
		return nil, err
	}
	if prev_kv && iv != nil {
		ikv, err = util.MakeInternalKV(iv, nil, key, rev, mrev)
		if err != nil {
			return nil, err
		}
	}
	return ikv, nil
}

func bDel(txn *badger.Txn, key []byte, prev_kv bool, rev uint64) (ikv *util.InternalKV, err error) {
	item, err := txn.Get(key)
	if err == badger.ErrKeyNotFound {
		return nil, ErrKeyNotFound
	}
	if err != nil {
		return nil, err
	}

	mrev := item.Version()

	if prev_kv {
		cv, err := item.ValueCopy(nil)
		if err != nil {
			return nil, err
		}
		ikv, err = util.MakeInternalKV(nil, cv, key, rev, mrev)
		if err != nil {
			return nil, err
		}
	}

	if err := txn.Delete(key); err != nil {
		return nil, err
	}
	return ikv, nil
}

func bRange(txn *badger.Txn, start, end []byte, limit int, fn func(ikv *util.InternalKV) bool, rev uint64) (ikvs []*util.InternalKV, more bool, err error) {
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = true
	it := txn.NewIterator(opts)
	defer it.Close()

	hitLimit := false
	count := 0

	for it.Seek(start); it.Valid() && !hitLimit; it.Next() {
		item := it.Item()
		key := item.KeyCopy(nil)

		// 有 end 则做上界检查
		if len(end) > 0 && bytes.Compare(key, end) >= 0 {
			break
		}

		// 达到 limit，标记截断但不断迭代——需要先取完当前条目才知道 key 位置
		if limit > 0 && count >= limit {
			hitLimit = true
			break
		}

		cv, err := item.ValueCopy(nil)
		if err != nil {
			return nil, false, err
		}
		mrev := item.Version()
		ikv, err := util.MakeInternalKV(nil, cv, key, rev, mrev)
		if err != nil {
			return nil, false, err
		}

		if fn != nil && !fn(ikv) {
			continue
		}

		ikvs = append(ikvs, ikv)
		count++
	}

	if hitLimit {
		// 当前迭代器指向触发 limit 的条目，只要它在 end 范围内，就有 more
		key := it.Item().KeyCopy(nil)
		if len(end) == 0 || bytes.Compare(key, end) < 0 {
			more = true
		}
	}

	return ikvs, more, nil
}
