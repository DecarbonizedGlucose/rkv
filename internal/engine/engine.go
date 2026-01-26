package engine

type Storage interface {
	Get(key string) (string, int64, error)
	Put(key, value string) (int64, error)
	Delete(key string) error
	Append(key, suffix string) (string, int64, error)
	CompareAndSwap(key string, version int64, value string) (int64, error)
}
