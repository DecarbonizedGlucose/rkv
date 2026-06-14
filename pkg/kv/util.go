package kv

import "github.com/DecarbonizedGlucose/rkv/api/proto/pkg/kvpb"

func compareInt(a uint64, r kvpb.Compare_CompareResult, b uint64) bool {
	switch r {
	case kvpb.Compare_EQUAL:
		return a == b
	case kvpb.Compare_GREATER:
		return a > b
	case kvpb.Compare_LESS:
		return a < b
	case kvpb.Compare_NOT_EQUAL:
		return a != b
	default:
		return false
	}
}

func compareBytes(a []byte, r kvpb.Compare_CompareResult, b []byte) bool {
	switch r {
	case kvpb.Compare_EQUAL:
		return string(a) == string(b)
	case kvpb.Compare_GREATER:
		return string(a) > string(b)
	case kvpb.Compare_LESS:
		return string(a) < string(b)
	case kvpb.Compare_NOT_EQUAL:
		return string(a) != string(b)
	default:
		return false
	}
}
