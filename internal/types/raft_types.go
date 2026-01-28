package types

import (
	rapb "github.com/DecarbonizedGlucose/rkv/api/raftapplier"
)

type Raft interface {
	Start(command *rapb.RequestWithMeta) (int, int64, bool)
	GetState() (int64, bool)
	Snapshot(index int, snapshot []byte)
	PersistBytes() int
	Kill()
}

type ApplyMsg struct {
	CommandValid bool
	Command      *rapb.RequestWithMeta
	CommandIndex int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int64
	SnapshotIndex int
}
