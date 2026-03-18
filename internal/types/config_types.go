package types

type NodeConfig struct {
	ID      string `json:"id"`
	Address string `json:"address"`
}

type ServerConfig struct {
	SelfID                 string       `json:"self_id"`
	Nodes                  []NodeConfig `json:"nodes"`
	MaxRaftState           int          `json:"max_raft_state"`
	GRPCAddress            string       `json:"grpc_address"`
	HTTPAddress            string       `json:"http_address"`
	RedisAddress           string       `json:"redis_address"`
	StoragePath            string       `json:"storage_path"`
	RaftStatePersistedPath string       `json:"raft_persist_path"`
}

type ClientConfig struct {
	ServerAddress string `json:"server_address"`
}
