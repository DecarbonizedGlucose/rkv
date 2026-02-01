package raft

type Persister struct {
}

func MakePersister() *Persister {
	return &Persister{}
}
