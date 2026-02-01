package types

type NodeConfig struct {
	ID      string `json:"id"`
	Address string `json:"address"`
}

type ServerConfig struct {
	SelfID       string       `json:"self_id"`
	Nodes        []NodeConfig `json:"nodes"`
	MaxRaftState int          `json:"max_raft_state"`
}

type ClientConfig struct {
	ServerAddress string `json:"server_address"`
}
