package lease

import rkvErrors "github.com/DecarbonizedGlucose/rkv/pkg/errors"

var (
	ErrLeaseNotFound = rkvErrors.ErrLeaseNotFound
	ErrNotLeader     = rkvErrors.ErrNotLeader
)
