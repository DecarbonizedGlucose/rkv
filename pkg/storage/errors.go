package storage

import (
	"errors"
)

var (
	ErrKeyNotFound   = errors.New("rkv: key not found")
	ErrInternalFault = errors.New("rkv: internal fault")
)
