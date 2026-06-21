package client

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	rkvErrors "github.com/DecarbonizedGlucose/rkv/pkg/errors"
)

var (
	ErrNotLeader     = rkvErrors.ErrNotLeader
	ErrLeaseNotFound = rkvErrors.ErrLeaseNotFound
	ErrUnavailable   = rkvErrors.ErrUnavailable
	ErrInternal      = rkvErrors.ErrInternal
)

func translateErr(err error) error {
	if err == nil {
		return nil
	}
	st, ok := status.FromError(err)
	if !ok {
		return err
	}
	switch st.Code() {
	case codes.FailedPrecondition:
		return ErrNotLeader
	case codes.NotFound:
		return ErrLeaseNotFound
	case codes.Unavailable:
		return ErrUnavailable
	case codes.Internal:
		return ErrInternal
	}
	return err
}
