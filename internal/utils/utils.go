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
