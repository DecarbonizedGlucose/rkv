package raft

import (
	"errors"
)

var (
	ErrNotLeader        = errors.New("raft: not leader")
	ErrStepLocalMsg     = errors.New("raft: cannot step local message")
	ErrStepPeerNotFound = errors.New("raft: cannot step as peer not found")
)
