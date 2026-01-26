package types

import (
	rsm "github.com/DecarbonizedGlucose/rkv/api/rsm"
)

type Raft interface {
	Start(command interface{}) (int, int, bool)
	GetState() (int, bool)
	Snapshot(index int, snapshot []byte)
	PersistBytes() int
	Kill()
}

type ApplyMsg struct {
	CommandValid bool
	Command      rsm.Command
	CommandIndex int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}
