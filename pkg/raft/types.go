package raft

import (
	"errors"
)

type stateType int

const (
	stateFollower = iota
	stateCandidate
	stateLeader
)

func (s stateType) str() string {
	switch s {
	case stateFollower:
		return "Follower"
	case stateCandidate:
		return "Candidate"
	case stateLeader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// 一个peer日志的复制进度
type Progress struct {
	// 已经复制到 follower 的最高日志索引
	MatchIndex uint64

	// 已经发送给 follower 的最高日志索引
	NextIndex uint64
}

type Config struct {
	ID               uint64
	Peers            []uint64
	ElectionTimeout  int
	HeartbeatTimeout int
	Storage          RaftStorage
}

func (c *Config) validate() error {
	if c.ID == 0 {
		return errors.New("Raft ID cannot be zero")
	}
	if c.HeartbeatTimeout <= 0 {
		return errors.New("Heartbeat timeout must be positive")
	}
	if c.ElectionTimeout <= c.HeartbeatTimeout {
		return errors.New("Election timeout must be greater than heartbeat timeout")
	}
	if c.Storage == nil {
		return errors.New("Storage cannot be nil")
	}
	return nil
}
