package certs

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
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

// writeCertificate writes a key pair signed by parent (self-signed when nil) and returns it.
func writeCertificate(t *testing.T, directory, name string, template *x509.Certificate, parent *x509.Certificate, parentKey *ecdsa.PrivateKey) (*x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	if parent == nil {
		parent, parentKey = template, key
	}
	der, err := x509.CreateCertificate(rand.Reader, template, parent, &key.PublicKey, parentKey)
	require.NoError(t, err)
	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(directory, name+".crt"), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(directory, name+".key"), pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600))
	certificate, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return certificate, key
}

func writeCA(t *testing.T, directory, name string) (*x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()
	return writeCertificate(t, directory, name, &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}, nil, nil)
}

// handshake dials a server using the directory's server key pair as serverName.
func handshake(t *testing.T, opts Opts, serverName string) error {
	t.Helper()
	serverConfig, err := opts.ServerTLSConfig()
	require.NoError(t, err)
	listener, err := tls.Listen("tcp", "127.0.0.1:0", serverConfig)
	require.NoError(t, err)
	defer listener.Close()
	go func() {
		connection, err := listener.Accept()
		if err == nil {
			connection.(*tls.Conn).Handshake()
			connection.Close()
		}
	}()

	clientConfig, err := opts.ClientTLSConfig()
	require.NoError(t, err)
	clientConfig.ServerName = serverName
	connection, err := tls.DialWithDialer(&net.Dialer{Timeout: time.Second}, "tcp", listener.Addr().String(), clientConfig)
	if err != nil {
		return err
	}
	return connection.Close()
}

func TestClientVerifiesServer(t *testing.T) {
	directory := t.TempDir()
	ca, caKey := writeCA(t, directory, "ca")
	writeCA(t, directory, "other-ca")
	leaf := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "server"},
		DNSNames:     []string{"localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	writeCertificate(t, directory, "server", leaf, ca, caKey)
	writeCertificate(t, directory, "client", leaf, ca, caKey)
	opts := Opts{
		CAFile:         filepath.Join(directory, "ca.crt"),
		ClientCertFile: filepath.Join(directory, "client.crt"),
		ClientKeyFile:  filepath.Join(directory, "client.key"),
		ServerCertFile: filepath.Join(directory, "server.crt"),
		ServerKeyFile:  filepath.Join(directory, "server.key"),
	}

	require.NoError(t, handshake(t, opts, "localhost"))

	// The certificate does not name the host.
	require.ErrorContains(t, handshake(t, opts, "user-service.default.svc"), "certificate is valid for localhost, not user-service.default.svc")

	// The certificate is not signed by the trusted CA.
	opts.CAFile = filepath.Join(directory, "other-ca.crt")
	require.ErrorContains(t, handshake(t, opts, "localhost"), "certificate signed by unknown authority")
}
