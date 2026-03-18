package raft

type Persister struct {
	mu               sync.Mutex
	raftSnapshotFile *string // KV DB Snapshot file path
	raftStateFile    *string // Raft state data file path
}

func MakePersister(rsFile, rmFile *string) *Persister {
	return &Persister{}
}
