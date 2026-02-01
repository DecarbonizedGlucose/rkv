package engine

type Storage interface {
	Get(key []byte) ([]byte, uint64, error)
	Put(key, value []byte) (uint64, error)
	Delete(key []byte) error
	Append(key, suffix []byte) ([]byte, uint64, error)
	CompareAndSwap(key []byte, version uint64, value []byte) (uint64, error)
}
