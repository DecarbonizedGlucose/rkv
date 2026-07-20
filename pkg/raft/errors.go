package raft

import (
	"errors"
)

var (
	ErrNotLeader         = errors.New("raft: not leader")
	ErrStepNilMsg        = errors.New("raft: cannot step nil message")
	ErrStepLocalMsg      = errors.New("raft: cannot step local message")
	ErrStepPeerNotFound  = errors.New("raft: cannot step as peer not found")
	ErrLeaderUnknown     = errors.New("raft: leader unknown")
	ErrReadIndexOnLeader = errors.New("raft: follower read index requested on leader")
)
