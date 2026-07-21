package tlsutil

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLoadServerConfig(t *testing.T) {
	certFile, keyFile, caFile := writeTestPKI(t)

	cfg, err := LoadServerConfig(certFile, keyFile, "")
	require.NoError(t, err)
	require.Equal(t, tls.NoClientCert, cfg.ClientAuth)
	requireTLSHandshake(t, cfg, &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    mustCertPool(t, caFile),
		ServerName: "localhost",
	})

	cfg, err = LoadServerConfig(certFile, keyFile, caFile)
	require.NoError(t, err)
	require.Equal(t, tls.RequireAndVerifyClientCert, cfg.ClientAuth)
}

func TestLoadMutualConfig(t *testing.T) {
	certFile, keyFile, caFile := writeTestPKI(t)
	serverCfg, clientCfg, err := LoadMutualConfig(certFile, keyFile, caFile)
	require.NoError(t, err)
	require.Equal(t, tls.RequireAndVerifyClientCert, serverCfg.ClientAuth)
	clientCfg.ServerName = "localhost"
	requireTLSHandshake(t, serverCfg, clientCfg)

	// mTLS 服务端拒绝没有客户端证书的连接。
	requireTLSHandshakeFailure(t, serverCfg, &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    mustCertPool(t, caFile),
		ServerName: "localhost",
	})
}

func requireTLSHandshake(t *testing.T, serverCfg, clientCfg *tls.Config) {
	t.Helper()
	serverRaw, clientRaw := net.Pipe()
	server := tls.Server(serverRaw, serverCfg)
	client := tls.Client(clientRaw, clientCfg)
	defer serverRaw.Close()
	defer clientRaw.Close()

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Handshake()
	}()

	require.NoError(t, client.Handshake())
	require.NoError(t, <-errCh)
}

func requireTLSHandshakeFailure(t *testing.T, serverCfg, clientCfg *tls.Config) {
	t.Helper()
	serverRaw, clientRaw := net.Pipe()
	server := tls.Server(serverRaw, serverCfg)
	client := tls.Client(clientRaw, clientCfg)

	errCh := make(chan error, 1)
	go func() { errCh <- server.Handshake() }()
	_ = client.Handshake()
	clientRaw.Close()
	serverErr := <-errCh
	serverRaw.Close()
	require.Error(t, serverErr)
}

func writeTestPKI(t *testing.T) (certFile, keyFile, caFile string) {
	t.Helper()
	now := time.Now()
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "rkv test CA"},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	require.NoError(t, err)
	caCert, err := x509.ParseCertificate(caDER)
	require.NoError(t, err)

	leafKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	leafTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    now.Add(-time.Minute),
		NotAfter:     now.Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTemplate, caCert, &leafKey.PublicKey, caKey)
	require.NoError(t, err)

	dir := t.TempDir()
	caFile = filepath.Join(dir, "ca.crt")
	certFile = filepath.Join(dir, "node.crt")
	keyFile = filepath.Join(dir, "node.key")
	require.NoError(t, os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), 0o600))
	require.NoError(t, os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER}), 0o600))
	require.NoError(t, os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(leafKey)}), 0o600))
	return certFile, keyFile, caFile
}

func mustCertPool(t *testing.T, path string) *x509.CertPool {
	t.Helper()
	pool, err := loadCertPool(path)
	require.NoError(t, err)
	return pool
}
