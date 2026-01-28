package utils

import (
	"math/rand"
	"time"
)

func RandomTimeout() time.Duration {
	return time.Duration(500+rand.Intn(400)) * time.Millisecond
}

func ConstTimeout() time.Duration {
	return time.Duration(100 * time.Millisecond)
}
