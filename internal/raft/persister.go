package raft

import (
	"bytes"
	"log"
	"os"
	"sync"
)

// Persister uses the full data from engine and
// saves them to file with some meta fields.
type Persister struct {
	mu               sync.Mutex
	raftSnapshotFile *string // KV DB Snapshot file path
	raftStateFile    *string // Raft state data file path
}

func MakePersister(rsFile, rmFile *string) *Persister {
	return &Persister{}
}

func readToBuffer(filename *string, buf *bytes.Buffer) error {
	f, err := os.Open(*filename)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = buf.ReadFrom(f)
	return err
}

func (ps *Persister) ReadSnapshot() (*bytes.Buffer, error) {
	buf := new(bytes.Buffer)
	err := readToBuffer(ps.raftSnapshotFile, buf)
	return buf, err
}

// Read raft metadata from file
func (ps *Persister) ReadRaftState() (*bytes.Buffer, error) {
	buf := new(bytes.Buffer)
	err := readToBuffer(ps.raftStateFile, buf)
	return buf, err
}

// Read raft state
func (ps *Persister) RaftStateSize() (int64, error) {
	info, err := os.Stat(*ps.raftSnapshotFile)
	if err != nil {
		return int64(-1), err
	}
	return info.Size(), nil
}

func writeFromBuffer(filename *string, buf *bytes.Buffer) error {
	f, err := os.Create(*filename)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = buf.WriteTo(f)
	return err
}

func (ps *Persister) Save(raftmeta *bytes.Buffer, snapshotBuf *bytes.Buffer) (error, error) {
	// Save raft meta data, aka "raftstate"
	err1 := writeFromBuffer(ps.raftStateFile, raftmeta)
	if err1 != nil {
		log.Fatalln(err1)
	}
	// Save raft logs data block, aka "snapshot"
	if snapshotBuf == nil {
		return err1, nil
	}
	err2 := writeFromBuffer(ps.raftSnapshotFile, snapshotBuf)
	if err2 != nil {
		log.Fatalln(err2)
	}
	return err1, err2
}
