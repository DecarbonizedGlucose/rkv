package raftstore

import (
	"errors"
)

// Node 服务本身相关 error
var (
	ErrStopped          = errors.New("raftstore: node has been stopped")
	ErrNotLeader        = errors.New("raftstore: not leader")
	ErrQuorumTimeout    = errors.New("raftstore: quorum confirmation timed out")
	ErrReadIndexTimeout = errors.New("raftstore: read index timed out")
)

// 内容相关 error
var (
	ErrApplyCorrupted = errors.New("raftstore: apply entries corrupted, unrecoverable")
)
