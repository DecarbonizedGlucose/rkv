package raftstore

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/dgraph-io/badger/v4"
	"google.golang.org/protobuf/proto"

	"github.com/DecarbonizedGlucose/rkv/api/proto/pkg/raftpb"
	"github.com/DecarbonizedGlucose/rkv/pkg/raft"
)

const (
	keyHardState = "raft_hard_state"
	keyConfState = "raft_conf_state"
	keySnapshot  = "raft_snapshot"
	entryPrefix  = "raft_entry_"
)

// raftEntryKey 将日志索引编码为可排序的 key。
// 格式：raft_entry_ + 8 字节大端序 uint64。
func raftEntryKey(idx uint64) []byte {
	buf := make([]byte, len(entryPrefix)+8)
	copy(buf, entryPrefix)
	binary.BigEndian.PutUint64(buf[len(entryPrefix):], idx)
	return buf
}

func raftEntryPrefix() []byte {
	return []byte(entryPrefix)
}

func decodeEntryIndex(key []byte) uint64 {
	return binary.BigEndian.Uint64(key[len(entryPrefix):])
}

// 实现 raft.RaftStorage 以及 raftstore.Storage 中的写方法
type RaftStorage struct {
	db *badger.DB
}

func NewRaftStorage(dir string) (*RaftStorage, error) {
	opts := badger.DefaultOptions(dir)
	//opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("open raft storage: %w", err)
	}
	return &RaftStorage{db: db}, nil
}

func (s *RaftStorage) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// ========================================
// raft.RaftStorage 读接口
// ========================================

// InitialState 返回持久化的 HardState 和 ConfState。
// 初次启动（无持久化数据）时返回零值而非报错。
func (s *RaftStorage) InitialState() (*raftpb.HardState, *raftpb.ConfState, error) {
	hs := &raftpb.HardState{}
	cs := &raftpb.ConfState{}

	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(keyHardState))
		if err == badger.ErrKeyNotFound {
			// 留 hs 为零值
		} else if err != nil {
			return err
		} else {
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			if err := proto.Unmarshal(val, hs); err != nil {
				return err
			}
		}

		item, err = txn.Get([]byte(keyConfState))
		if err == badger.ErrKeyNotFound {
			// 留 cs 为零值
		} else if err != nil {
			return err
		} else {
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			return proto.Unmarshal(val, cs)
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	return hs, cs, nil
}

// 返回 [lo, hi) 区间内的日志条目。lo 必须大于已压缩的索引。
func (s *RaftStorage) Entries(lo, hi uint64) ([]*raftpb.Entry, error) {
	if lo >= hi {
		return nil, nil
	}

	var entries []*raftpb.Entry
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		defer it.Close()

		// 从 lo 开始扫描
		startKey := raftEntryKey(lo)

		for it.Seek(startKey); it.Valid(); it.Next() {
			key := it.Item().KeyCopy(nil)
			if !bytes.HasPrefix(key, raftEntryPrefix()) {
				break
			}
			idx := decodeEntryIndex(key)
			if idx >= hi {
				break
			}
			val, err := it.Item().ValueCopy(nil)
			if err != nil {
				return err
			}
			entry := &raftpb.Entry{}
			if err := proto.Unmarshal(val, entry); err != nil {
				return err
			}
			entries = append(entries, entry)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		// lo 落在快照覆盖范围内，数据已被压缩
		snapIdx, err := s.snapshotIncludedIndex()
		if err != nil {
			return nil, err
		}
		if snapIdx > 0 && lo <= snapIdx {
			return nil, raft.ErrCompacted
		}
		return nil, raft.ErrUnavailable
	}
	return entries, nil
}

// TermOfLog 返回指定索引日志的 term。
func (s *RaftStorage) TermOfLog(idx uint64) (uint64, error) {
	var term uint64
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(raftEntryKey(idx))
		if err == badger.ErrKeyNotFound {
			return raft.ErrUnavailable
		}
		if err != nil {
			return err
		}
		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		entry := &raftpb.Entry{}
		if err := proto.Unmarshal(val, entry); err != nil {
			return err
		}
		term = entry.Term
		return nil
	})
	return term, err
}

// LastIndex 返回已持久化日志的最大索引。
func (s *RaftStorage) LastIndex() (uint64, error) {
	lastIdx, err := s.lastEntryIndex()
	if err != nil {
		return 0, err
	}
	// 快照中的 sentinel 也算一条
	snapIdx, err := s.snapshotIncludedIndex()
	if err != nil {
		return 0, err
	}
	if snapIdx > lastIdx {
		lastIdx = snapIdx
	}
	return lastIdx, nil
}

// FirstIndex 返回已持久化日志的最小索引。
// 无快照也无 entry 时返回 0，此时调用方 newRaftLog 会做 uint64 下溢防护。
// 有快照时返回 snapIdx+1（快照后的第一条日志），
// newRaftLog 取 FirstIndex-1 作为 lastIncludedIndex，即 snapIdx。
func (s *RaftStorage) FirstIndex() (uint64, error) {
	firstIdx, err := s.firstEntryIndex()
	if err != nil {
		return 0, err
	}
	snapIdx, err := s.snapshotIncludedIndex()
	if err != nil {
		return 0, err
	}
	// 如果存在快照，最小索引 = 快照最后索引 + 1
	if snapIdx > 0 && firstIdx < snapIdx+1 {
		firstIdx = snapIdx + 1
	}
	return firstIdx, nil
}

// Snapshot 返回最近一次创建的快照。
func (s *RaftStorage) Snapshot() (*raftpb.Snapshot, error) {
	var snap raftpb.Snapshot
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(keySnapshot))
		if err == badger.ErrKeyNotFound {
			return raft.ErrSnapshotTemporarilyUnavailable
		}
		if err != nil {
			return err
		}
		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		return proto.Unmarshal(val, &snap)
	})
	if err != nil {
		return nil, err
	}
	return &snap, nil
}

// ========================================
// raftstore.Storage 写接口
// ========================================

// SaveHardState 持久化 HardState。
func (s *RaftStorage) SaveHardState(hs *raftpb.HardState) error {
	val, err := proto.Marshal(hs)
	if err != nil {
		return err
	}
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(keyHardState), val)
	})
}

// 将一批日志条目写入持久化存储。按索引升序调用。
func (s *RaftStorage) Append(entries []*raftpb.Entry) error {
	return s.db.Update(func(txn *badger.Txn) error {
		for _, entry := range entries {
			val, err := proto.Marshal(entry)
			if err != nil {
				return err
			}
			if err := txn.Set(raftEntryKey(entry.Index), val); err != nil {
				return err
			}
		}
		return nil
	})
}

// 写入快照数据并清除快照点之前的旧日志。
func (s *RaftStorage) ApplySnapshot(snap *raftpb.Snapshot) error {
	return s.db.Update(func(txn *badger.Txn) error {
		// 持久化快照
		val, err := proto.Marshal(snap)
		if err != nil {
			return err
		}
		if err := txn.Set([]byte(keySnapshot), val); err != nil {
			return err
		}

		// 持久化 ConfState（从快照元数据中提取）
		if snap.Metadata != nil && snap.Metadata.ConfState != nil {
			csVal, err := proto.Marshal(snap.Metadata.ConfState)
			if err != nil {
				return err
			}
			if err := txn.Set([]byte(keyConfState), csVal); err != nil {
				return err
			}
		}

		// 删除快照覆盖范围内的日志
		cutoff := snap.Metadata.LastIncludedIndex
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(raftEntryPrefix()); it.Valid(); it.Next() {
			key := it.Item().Key()
			if !bytes.HasPrefix(key, raftEntryPrefix()) {
				break
			}
			if decodeEntryIndex(key) <= cutoff {
				if err := txn.Delete(key); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// 在指定索引处生成快照，并持久化 ConfState。
func (s *RaftStorage) CreateSnapshot(idx uint64, cs *raftpb.ConfState, data []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		if cs != nil {
			csVal, err := proto.Marshal(cs)
			if err != nil {
				return err
			}
			if err := txn.Set([]byte(keyConfState), csVal); err != nil {
				return err
			}
		}

		// 从存储中读取对应日志的 term，用于快照元数据
		snapTerm := uint64(0)
		item, err := txn.Get(raftEntryKey(idx))
		if err == nil {
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			entry := &raftpb.Entry{}
			if err := proto.Unmarshal(val, entry); err != nil {
				return err
			}
			snapTerm = entry.Term
		} else if err != badger.ErrKeyNotFound {
			return err
		}

		snap := &raftpb.Snapshot{
			Data: data,
			Metadata: &raftpb.SnapshotMetadata{
				LastIncludedIndex: idx,
				LastIncludedTerm:  snapTerm,
				ConfState:         cs,
			},
		}
		snapVal, err := proto.Marshal(snap)
		if err != nil {
			return err
		}
		return txn.Set([]byte(keySnapshot), snapVal)
	})
}

// Compact 删除索引小于 compactIdx 的日志条目。
// 这里 compactIdx 即 lastIncludedIndex。
func (s *RaftStorage) Compact(compactIdx uint64) error {
	return s.db.Update(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		for it.Seek(raftEntryPrefix()); it.Valid(); it.Next() {
			key := it.Item().Key()
			if !bytes.HasPrefix(key, raftEntryPrefix()) {
				break
			}
			if decodeEntryIndex(key) < compactIdx {
				if err := txn.Delete(key); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// ========================================
// Utils
// ========================================

// 返回entry前缀下最大的索引, 无entry时返回0
func (s *RaftStorage) lastEntryIndex() (uint64, error) {
	var last uint64
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Reverse = true // 从大到小扫描
		it := txn.NewIterator(opts)
		defer it.Close()

		// 反向遍历时，需要 seek 到一个严格大于所有 entry key 的位置。
		// entry key 最长 19 字节（11 字节前缀 + 8 字节 uint64），
		// seekKey 取 20 字节（前缀 + 9 字节 0xFF），字典序可覆盖所有情况。
		seekKey := make([]byte, len(entryPrefix)+9)
		copy(seekKey, entryPrefix)
		for i := len(entryPrefix); i < len(seekKey); i++ {
			seekKey[i] = 0xFF
		}
		for it.Seek(seekKey); it.Valid(); it.Next() {
			key := it.Item().Key()
			if !bytes.HasPrefix(key, raftEntryPrefix()) {
				break
			}
			last = decodeEntryIndex(key)
			return nil
		}
		return nil
	})
	return last, err
}

// 返回 entry 前缀下最小的索引, 无 entry 时返回 0。
func (s *RaftStorage) firstEntryIndex() (uint64, error) {
	var first uint64
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(raftEntryPrefix()); it.Valid(); it.Next() {
			key := it.Item().Key()
			if !bytes.HasPrefix(key, raftEntryPrefix()) {
				break
			}
			first = decodeEntryIndex(key)
			return nil
		}
		return nil
	})
	return first, err
}

// 返回快照的 LastIncludedIndex, 无快照时返回 0。
func (s *RaftStorage) snapshotIncludedIndex() (uint64, error) {
	var idx uint64
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(keySnapshot))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		snap := &raftpb.Snapshot{}
		if err := proto.Unmarshal(val, snap); err != nil {
			return err
		}
		idx = snap.Metadata.LastIncludedIndex
		return nil
	})
	return idx, err
}
