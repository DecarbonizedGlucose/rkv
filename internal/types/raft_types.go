package types

import (
	kvpb "github.com/DecarbonizedGlucose/rkv/api/kvrpc"
)

type ApplyMsg struct {
	CommandValid bool
	Command      *kvpb.RequestWithMeta
	CommandIndex int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int64
	SnapshotIndex int
}
