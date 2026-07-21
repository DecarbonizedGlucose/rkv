package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/DecarbonizedGlucose/rkv/pkg/option"
	"github.com/DecarbonizedGlucose/rkv/pkg/server"
	"github.com/DecarbonizedGlucose/rkv/pkg/tlsutil"
)

func main() {
	var (
		peers             string
		dataDir           string
		raftDir           string
		raftAddr          string
		grpcAddr          string
		nodeID            uint64
		allowFollowerRead bool
		tlsCert           string
		tlsKey            string
		tlsClientCA       string
		raftTLSCert       string
		raftTLSKey        string
		raftTLSCA         string
	)

	flag.Uint64Var(&nodeID, "id", 0, "node ID")
	flag.StringVar(&peers, "peers", "", "peer list: 1=addr1,2=addr2,...")
	flag.StringVar(&dataDir, "data-dir", "", "KV data directory")
	flag.StringVar(&raftDir, "raft-dir", "", "Raft log directory")
	flag.StringVar(&raftAddr, "raft-addr", "", "Raft transport address")
	flag.StringVar(&grpcAddr, "grpc-addr", "", "gRPC API address")
	flag.BoolVar(&allowFollowerRead, "allow-follower-read", false, "allow linearizable reads through followers")
	flag.StringVar(&tlsCert, "tls-cert", "", "client API TLS certificate")
	flag.StringVar(&tlsKey, "tls-key", "", "client API TLS private key")
	flag.StringVar(&tlsClientCA, "tls-client-ca", "", "optional CA used to verify client certificates")
	flag.StringVar(&raftTLSCert, "raft-tls-cert", "", "Raft mTLS certificate")
	flag.StringVar(&raftTLSKey, "raft-tls-key", "", "Raft mTLS private key")
	flag.StringVar(&raftTLSCA, "raft-tls-ca", "", "CA used to verify Raft peers")
	flag.Parse()

	if nodeID == 0 {
		log.Fatal("-id is required")
	}
	if peers == "" {
		log.Fatal("-peers is required")
	}
	if dataDir == "" {
		log.Fatal("-data-dir is required")
	}
	if raftDir == "" {
		log.Fatal("-raft-dir is required")
	}
	if raftAddr == "" {
		log.Fatal("-raft-addr is required")
	}
	if grpcAddr == "" {
		log.Fatal("-grpc-addr is required")
	}

	peerIDs, peerAddrs, err := parsePeers(peers)
	if err != nil {
		log.Fatal(err)
	}

	opts := option.DefaultConfig()
	opts.NodeID = nodeID
	opts.DataDir = dataDir
	opts.RaftDir = raftDir
	opts.RaftAddr = raftAddr
	opts.GRPCAddr = grpcAddr
	opts.AllowFollowerRead = allowFollowerRead
	opts.TickInterval = 100 * time.Millisecond
	if tlsCert != "" || tlsKey != "" || tlsClientCA != "" {
		if tlsCert == "" || tlsKey == "" {
			log.Fatal("-tls-cert and -tls-key must be configured together")
		}
		cfg, err := tlsutil.LoadServerConfig(tlsCert, tlsKey, tlsClientCA)
		if err != nil {
			log.Fatal(err)
		}
		opts.GRPCTLSConfig = cfg
	}
	if raftTLSCert != "" || raftTLSKey != "" || raftTLSCA != "" {
		if raftTLSCert == "" || raftTLSKey == "" || raftTLSCA == "" {
			log.Fatal("-raft-tls-cert, -raft-tls-key and -raft-tls-ca must be configured together")
		}
		serverCfg, clientCfg, err := tlsutil.LoadMutualConfig(raftTLSCert, raftTLSKey, raftTLSCA)
		if err != nil {
			log.Fatal(err)
		}
		opts.RaftServerTLSConfig = serverCfg
		opts.RaftClientTLSConfig = clientCfg
	}

	if err := opts.SetPeers(peerIDs, peerAddrs); err != nil {
		log.Fatal(err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	srv, err := server.NewServer(ctx, opts)
	if err != nil {
		log.Fatal(err)
	}

	if err := srv.Serve(); err != nil {
		log.Println(err)
	}
}

func parsePeers(s string) ([]uint64, map[uint64]string, error) {
	ids := make([]uint64, 0)
	addrs := make(map[uint64]string)

	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		parts := strings.SplitN(p, "=", 2)
		if len(parts) != 2 {
			return nil, nil, fmt.Errorf("invalid peer format %q, expected id=addr", p)
		}
		id, err := strconv.ParseUint(parts[0], 10, 64)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid peer id %q: %w", parts[0], err)
		}
		ids = append(ids, id)
		addrs[id] = parts[1]
	}
	return ids, addrs, nil
}
