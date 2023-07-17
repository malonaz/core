package certs

import (
	"crypto/tls"
	"crypto/x509"
	"os"

	"github.com/pkg/errors"

	"go/logging"
)

var logger = logging.NewLogger()

// Opts holds options for certificates.
type Opts struct {
	CAFile         string `long:"ca_file"          env:"CA_FILE"          default:"ca.crt"      description:"Path to the CA cert file to load."`
	ClientCertFile string `long:"client_cert_file" env:"CLIENT_CERT_FILE" default:"client.crt"  description:"Path to the client certificate .pem file."`
	ClientKeyFile  string `long:"client key file"  env:"CLIENT_KEY_FILE"  default:"client.key"  description:"Path to the client key .pem file."`
	ServerCertFile string `long:"server_cert_file" env:"SERVER_CERT_FILE" default:"server.crt"  description:"Path to the server certificate .pem file."`
	ServerKeyFile  string `long:"server_key_file"  env:"SERVER_KEY_FILE"  default:"server.key"  description:"Path to the server key .pem file."`
}

// ClientTLSConfig returns a client TLS config.
func (c Opts) ClientTLSConfig() (*tls.Config, error) {
	return tlsConfig(c.ClientKeyFile, c.ClientCertFile, c.CAFile, false)
}

// ServerTLSConfig returns a server TLS config.
func (c Opts) ServerTLSConfig() (*tls.Config, error) {
	return tlsConfig(c.ServerKeyFile, c.ServerCertFile, c.CAFile, true)
}

func tlsConfig(keyFile, certFile, caFile string, server bool) (*tls.Config, error) {
	if caFile == "" {
		return nil, errors.New("certificate Authority filename is empty")
	}
	if keyFile == "" {
		return nil, errors.New("Certificate Key filename is empty")
	}
	if certFile == "" {
		return nil, errors.New("Certificate File filename is empty")
	}

	certificatePool, err := certificatePool(caFile)
	if err != nil {
		return nil, errors.Wrap(err, "Could not create a certificate pool")
	}
	certificate, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, errors.Wrap(err, "Could not load Key/Pair")
	}
	config := &tls.Config{
		Certificates:       []tls.Certificate{certificate},
		InsecureSkipVerify: true,
	}
	if server {
		config.ClientCAs = certificatePool
		config.ClientAuth = tls.RequireAndVerifyClientCert
	} else {
		config.RootCAs = certificatePool
	}
	config.BuildNameToCertificate()
	return config, nil
}

func certificatePool(filename string) (*x509.CertPool, error) {
	bytes, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	certificatePool := x509.NewCertPool()
	ok := certificatePool.AppendCertsFromPEM(bytes)
	if !ok {
		return nil, errors.New("Failed to append CA certs to certificate pool. Is the .pem file valid?")
	}
	return certificatePool, nil
}
