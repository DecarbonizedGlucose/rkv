package service

import (
	"encoding/json"
	"os"

	"github.com/DecarbonizedGlucose/rkv/internal/types"
)

func LoadServerConfig(path string) (*types.ServerConfig, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var cfg types.ServerConfig
	decoder := json.NewDecoder(f)
	if err := decoder.Decode(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func LoadClientConfig(path string) (*types.ClientConfig, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var cfg types.ClientConfig
	decoder := json.NewDecoder(f)
	if err := decoder.Decode(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}
