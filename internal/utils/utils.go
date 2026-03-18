package utils

import (
	"context"
	"math/rand"
	"time"
)

func RandomTimeout() time.Duration {
	return time.Duration(500+rand.Intn(400)) * time.Millisecond
}

func ConstTimeout() time.Duration {
	return time.Duration(100 * time.Millisecond)
}

func IsCtxFailed(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

func DeadlineFromCtx(ctx context.Context) time.Duration {
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining > 0 {
			return remaining
		}
		return 0
	}
	return 1500 * time.Millisecond
}
