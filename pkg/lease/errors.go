package lease

import "errors"

var (
	ErrLeaseNotFound = errors.New("lease not found")
	ErrNotLeader     = errors.New("not leader")
)
